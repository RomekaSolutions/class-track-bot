"""
Telegram Bot for ESL Class Tracking ("ClassTrackBot").

This script implements a Telegram bot that helps a private ESL tutor
track class schedules, payments, cancellations and communication with
students.  It follows the specification laid out in the project
description: https://chat.openai.com.  Students interact with the bot
via commands and inline keyboards, while the tutor (admin) can manage
students and view summaries via a private dashboard.

The bot is designed around a flat‑file JSON database stored in
``students.json`` and ``logs.json``.  Each student entry contains
their name, Telegram identifier, payment plan, schedule, renewal date
and various flags (paused, free class credits etc.).
The logs file records every class interaction (completed, missed,
cancelled, rescheduled) along with optional notes.

To run this bot you need a Telegram Bot token.  Set it via the
``TELEGRAM_BOT_TOKEN`` environment variable or replace the
``TOKEN`` constant below.  You should also populate the ``ADMIN_IDS``
list with the Telegram user IDs of admins allowed to use the
management commands.

The bot uses the ``python-telegram-bot`` library for message
handling.  If you don't have it installed yet, run

  pip install python-telegram-bot

This script has been written against version 20+ of the library.

Because this environment does not allow outbound network connections
and we cannot install external packages during development, the code
has not been executed here.  Nonetheless it adheres to the published
API and serves as a solid starting point for deploying the bot.

Author: ChatGPT
"""

import json
import logging
import os
from datetime import datetime, timedelta, time, date
from typing import Dict, Any, List, Optional, Tuple

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    JobQueue,
    filters,
)

# Additional imports for timezone handling
import pytz
from pytz import AmbiguousTimeError, NonExistentTimeError


# -----------------------------------------------------------------------------
# Configuration
#
# Set your bot token here or via the TELEGRAM_BOT_TOKEN environment variable.
# The admin IDs should contain the Telegram user IDs of the tutor(s) who
# are allowed to run management commands.  You can find your user ID by
# talking to @userinfobot on Telegram.
# -----------------------------------------------------------------------------

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")

# Replace with your own Telegram numerical user IDs
#
# This value can also be overridden via the ``ADMIN_IDS`` environment variable. If
# present, it should be a comma‑separated list of integers. For example:
#   export ADMIN_IDS="123456789,987654321"
# will result in ``ADMIN_IDS`` being set to ``{123456789, 987654321}``.
admin_env = os.environ.get("ADMIN_IDS")
if admin_env:
    try:
        ADMIN_IDS = {int(item.strip()) for item in admin_env.split(",") if item.strip()}
    except ValueError:
        logging.warning(
            "Invalid ADMIN_IDS environment variable; falling back to default admin list."
        )
        ADMIN_IDS = {123456789}
else:
    ADMIN_IDS = {123456789}


# Paths to the JSON database files.  Adjust if you wish to store them elsewhere.
STUDENTS_FILE = "students.json"
LOGS_FILE = "logs.json"

# Default values for new student fields
DEFAULT_CUTOFF_HOURS = 24
DEFAULT_CYCLE_WEEKS = 4
DEFAULT_DURATION_HOURS = 1.0
# Offset before class time to send reminder
DEFAULT_REMINDER_OFFSET = timedelta(hours=1)

# Base timezone for all operations (Bangkok time)
BASE_TZ = pytz.timezone("Asia/Bangkok")


def student_timezone(student: Dict[str, Any]) -> pytz.timezone:
    """Return the base timezone for all students (no per-student TZ)."""
    return BASE_TZ


def safe_localize(tz: pytz.timezone, naive_dt: datetime) -> datetime:
    """Localize ``naive_dt`` handling DST edge cases."""
    try:
        return tz.localize(naive_dt, is_dst=None)
    except AmbiguousTimeError as e:
        logging.warning("Ambiguous time %s in %s: %s", naive_dt, tz, e)
        return tz.localize(naive_dt, is_dst=True)
    except NonExistentTimeError as e:
        logging.warning("Non-existent time %s in %s: %s", naive_dt, tz, e)
        # shift by +1h
        return tz.localize(naive_dt + timedelta(hours=1), is_dst=True)


def parse_student_datetime(dt_str: str, student: Dict[str, Any]) -> datetime:
    """Parse ``dt_str`` and return an aware datetime in the base timezone."""
    dt_str = dt_str.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(dt_str)
    except ValueError as e:
        raise ValueError(f"Invalid datetime: {dt_str}") from e
    if dt.tzinfo is None:
        dt = safe_localize(student_timezone(student), dt)
    return dt


def normalize_handle(handle: Optional[str]) -> str:
    """Return handle lowercased without leading @."""
    return (handle or "").lstrip("@").lower()


def dedupe_student_keys(students: Dict[str, Any]) -> bool:
    """Normalize handles and merge duplicate records.

    Prefers numeric telegram_id as the dictionary key when available.
    Returns True if any modifications were made.
    """
    changed = False
    new_students: Dict[str, Any] = {}
    for key, student in list(students.items()):
        telegram_id = student.get("telegram_id")
        handle = normalize_handle(student.get("telegram_handle"))
        if handle:
            student["telegram_handle"] = handle
        canonical_key = str(telegram_id) if telegram_id else handle or normalize_handle(key)
        existing = new_students.get(canonical_key)
        if existing is not None:
            for k, v in student.items():
                if k not in existing:
                    existing[k] = v
            changed = True
        else:
            new_students[canonical_key] = student
        if canonical_key != key:
            changed = True
    if changed:
        students.clear()
        students.update(new_students)
    return changed


def resolve_student(students: Dict[str, Any], key: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """Return (canonical_key, student) for given ID or handle input."""
    skey = normalize_handle(key)
    if skey.isdigit() and str(int(skey)) in students:
        canon = str(int(skey))
        return canon, students[canon]
    if skey in students:
        return skey, students[skey]
    for k, s in students.items():
        if normalize_handle(s.get("telegram_handle")) == skey:
            return k, s
    return None, None


def normalize_students(students: Dict[str, Any]) -> bool:
    """Ensure all student records contain required fields.

    Injects defaults for legacy students. Returns True if any student was
    modified so callers may persist the upgraded data.
    """
    changed_any = False
    for key, student in students.items():
        changed = False
        if "cutoff_hours" not in student:
            student["cutoff_hours"] = DEFAULT_CUTOFF_HOURS
            changed = True
        if "cycle_weeks" not in student:
            student["cycle_weeks"] = DEFAULT_CYCLE_WEEKS
            changed = True
        if "class_duration_hours" not in student:
            student["class_duration_hours"] = DEFAULT_DURATION_HOURS
            changed = True
        # Drop legacy per-student timezone field
        if "student_timezone" in student:
            student.pop("student_timezone", None)
            changed = True
        # Remove legacy pending reschedule field if present
        if "pending_reschedule" in student:
            student.pop("pending_reschedule", None)
            changed = True
        if changed:
            logging.info(
                "Applied default fields for legacy student %s",
                student.get("name", key),
            )
            changed_any = True
    return changed_any


def migrate_student_dates(students: Dict[str, Any]) -> bool:
    """Migrate naive date strings to ISO 8601 with offsets."""
    changed = False
    for student in students.values():
        tz = BASE_TZ
        # class dates
        new_dates = []
        converted = False
        for item in student.get("class_dates", []):
            if isinstance(item, str) and ("T" in item or "+" in item or item.endswith("Z")):
                new_dates.append(item)
            else:
                try:
                    dt = safe_localize(tz, datetime.strptime(item, "%Y-%m-%d %H:%M"))
                    new_dates.append(dt.isoformat())
                    converted = True
                except Exception:
                    continue
        if converted:
            student["class_dates"] = sorted(new_dates)
            changed = True
        # cancelled dates
        new_cancel = []
        converted_cancel = False
        for item in student.get("cancelled_dates", []):
            if isinstance(item, str) and ("T" in item or "+" in item or item.endswith("Z")):
                new_cancel.append(item)
            else:
                try:
                    dt = safe_localize(tz, datetime.strptime(item, "%Y-%m-%d %H:%M"))
                    new_cancel.append(dt.isoformat())
                    converted_cancel = True
                except Exception:
                    continue
        if converted_cancel:
            student["cancelled_dates"] = new_cancel
            changed = True
        # pending cancel
        pending = student.get("pending_cancel")
        if pending:
            for field in ["class_time", "requested_at"]:
                item = pending.get(field)
                if item and "T" not in item:
                    try:
                        dt = safe_localize(tz, datetime.strptime(item, "%Y-%m-%d %H:%M"))
                        pending[field] = dt.isoformat()
                        changed = True
                    except Exception:
                        continue
    return changed


# Conversation states for adding a student
(
    ADD_NAME,
    ADD_HANDLE,
    ADD_PRICE,
    ADD_CLASSES,
    ADD_SCHEDULE,
    ADD_CUTOFF,
    ADD_WEEKS,
    ADD_DURATION,
    ADD_RENEWAL,
) = range(9)

def load_students() -> Dict[str, Any]:
    """Load students from the JSON file and normalize legacy records."""
    if not os.path.exists(STUDENTS_FILE):
        return {}
    with open(STUDENTS_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            if isinstance(data, dict):
                students = data
            elif isinstance(data, list):
                # If stored as list in earlier versions, convert to dict keyed by telegram_id
                students = {str(item["telegram_id"]): item for item in data}
            else:
                students = {}
        except json.JSONDecodeError:
            logging.error("Failed to parse students.json; starting with empty database.")
            return {}
    changed = normalize_students(students)
    if migrate_student_dates(students):
        changed = True
    if dedupe_student_keys(students):
        changed = True
    if changed:
        # One-time migration: persist upgraded student records
        save_students(students)
    return students


def save_students(students: Dict[str, Any]) -> None:
    """Persist students dict to JSON."""
    with open(STUDENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(students, f, indent=2, ensure_ascii=False, sort_keys=True)


def load_logs() -> List[Dict[str, Any]]:
    """Load class logs from JSON file."""
    if not os.path.exists(LOGS_FILE):
        return []
    with open(LOGS_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            logging.error("Failed to parse logs.json; starting with empty log.")
            return []


def save_logs(logs: List[Dict[str, Any]]) -> None:
    """Write logs list back to JSON."""
    with open(LOGS_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, ensure_ascii=False, sort_keys=True)


def parse_schedule(
    schedule_str: str,
    *,
    start_date: Optional[date] = None,
    cycle_weeks: int = DEFAULT_CYCLE_WEEKS,
) -> List[str]:
    """Generate concrete class dates for a repeating schedule.

    ``schedule_str`` is a comma separated string such as
    "Monday 17:00, Thursday 17:00". ``start_date`` marks the beginning of
    the cycle.  For each entry we compute all occurrences within
    ``cycle_weeks`` weeks and return a list of ISO8601 strings with
    timezone offsets in chronological order. All dates are interpreted in
    the base timezone (ICT).
    """
    if start_date is None:
        start_date = date.today()
    tz = BASE_TZ
    entries = [item.strip() for item in schedule_str.split(",") if item.strip()]
    if not entries:
        return []
    start_dt = safe_localize(tz, datetime.combine(start_date, time.min))
    end_dt = tz.normalize(start_dt + timedelta(weeks=cycle_weeks))
    results: List[str] = []
    for entry in entries:
        next_dt = next_occurrence(entry, now=start_dt)
        while next_dt < end_dt:
            results.append(next_dt.isoformat())
            next_dt = tz.normalize(next_dt + timedelta(weeks=1))
    results.sort()
    return results


def parse_renewal_date(date_str: str) -> Optional[str]:
    """Validate a date string in YYYY‑MM‑DD format.  Returns the same
    string if valid, otherwise None."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        return None


def next_occurrence(day_time_str: str, now: datetime) -> datetime:
    """Given a schedule entry like "Monday 17:00", return the next datetime
    after `now` that matches that weekday and time.

    ``now`` must be timezone-aware.  Example: if today is Monday at 16:00
    and schedule is "Monday 17:00", the next occurrence will be today at
    17:00.  If it's already past 17:00, the result will be next week.
    """
    if now.tzinfo is None or now.tzinfo.utcoffset(now) is None:
        raise ValueError("now must be timezone-aware")
    try:
        day_name, time_str = day_time_str.split()
        hour, minute = map(int, time_str.split(":"))
        # Map weekday names to numbers (Monday=0)
        weekdays = {
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
            "sunday": 6,
        }
        weekday = weekdays.get(day_name.lower())
        if weekday is None:
            raise ValueError
        # Build candidate datetime for this week
        days_ahead = (weekday - now.weekday()) % 7
        candidate = now + timedelta(days=days_ahead)
        candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
        candidate = now.tzinfo.normalize(candidate)
        # If candidate is before now, push to next week
        if candidate <= now:
            candidate = now.tzinfo.normalize(candidate + timedelta(days=7))
        return candidate
    except Exception:
        # If parsing fails just return now + 1 hour as fallback
        return now + timedelta(hours=1)


def get_upcoming_classes(student: Dict[str, Any], count: int = 5) -> List[datetime]:
    """Return upcoming class datetimes in the base timezone.

    ``student['class_dates']`` stores concrete class datetimes as
    ISO 8601 strings with timezone offsets in the base timezone.
    This function converts them to aware ``datetime`` objects, filters out
    past or cancelled classes and returns the next ``count`` items.
    """
    tz = student_timezone(student)
    now = datetime.now(tz)
    cancelled = set(student.get("cancelled_dates", []))
    renewal_date = None
    renewal_str = student.get("renewal_date")
    if renewal_str:
        try:
            renewal_date = datetime.strptime(renewal_str, "%Y-%m-%d").date()
        except ValueError:
            renewal_date = None
    results: List[datetime] = []
    for item in student.get("class_dates", []):
        try:
            dt = datetime.fromisoformat(item)
        except Exception:
            try:
                dt = safe_localize(tz, datetime.strptime(item, "%Y-%m-%d %H:%M"))
            except Exception:
                continue
        if dt <= now:
            continue
        if item in cancelled or dt.isoformat() in cancelled:
            continue
        if renewal_date and dt.date() > renewal_date:
            continue
        results.append(dt)
    results.sort()
    return results[:count]


async def send_class_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job to send a reminder before a class."""
    data = context.job.data or {}
    student_key = data.get("student_key")
    class_dt_str = data.get("class_dt")
    if not student_key or not class_dt_str:
        return
    students = load_students()
    student = students.get(student_key)
    if not student or student.get("paused"):
        return
    chat_id = student.get("telegram_id")
    if not chat_id:
        return
    try:
        class_dt = datetime.fromisoformat(class_dt_str)
    except Exception:
        return
    tz = student_timezone(student)
    local_dt = class_dt.astimezone(tz)
    msg = f"Reminder: you have a class at {local_dt.strftime('%Y-%m-%d %H:%M %Z')}"
    try:
        await context.bot.send_message(chat_id=chat_id, text=msg)
    except Exception:
        logging.warning("Failed to send class reminder to %s", student.get("name"))


def schedule_class_reminder(
    application: Application,
    student_key: str,
    student: Dict[str, Any],
    class_dt_str: str,
    reminder_offset: timedelta = DEFAULT_REMINDER_OFFSET,
) -> None:
    tz = student_timezone(student)
    now = datetime.now(tz)
    try:
        class_dt = datetime.fromisoformat(class_dt_str)
    except Exception:
        return
    run_time = class_dt - reminder_offset
    if run_time <= now:
        return
    application.job_queue.run_once(
        send_class_reminder,
        when=run_time,
        name=f"class_reminder:{student_key}:{class_dt_str}",
        data={"student_key": student_key, "class_dt": class_dt_str},
    )


def schedule_student_reminders(
    application: Application,
    student_key: str,
    student: Dict[str, Any],
    reminder_offset: timedelta = DEFAULT_REMINDER_OFFSET,
) -> None:
    """Schedule reminder jobs for all future classes of a student."""
    prefix = f"class_reminder:{student_key}:"
    # remove existing reminder jobs for this student
    for job in application.job_queue.jobs():
        if job.name and job.name.startswith(prefix):
            job.schedule_removal()
    for item in student.get("class_dates", []):
        if item in student.get("cancelled_dates", []):
            continue
        schedule_class_reminder(
            application, student_key, student, item, reminder_offset
        )


def ensure_future_class_dates(student: Dict[str, Any], horizon_weeks: Optional[int] = None) -> bool:
    """Ensure class_dates extend at least ``horizon_weeks`` into the future."""
    if horizon_weeks is None:
        horizon_weeks = student.get("cycle_weeks", DEFAULT_CYCLE_WEEKS)
    tz = student_timezone(student)
    now = datetime.now(tz)
    class_dates = student.get("class_dates", [])
    original_len = len(class_dates)

    renewal_date = None
    renewal_str = student.get("renewal_date")
    if renewal_str:
        try:
            renewal_date = date.fromisoformat(renewal_str)
        except ValueError:
            renewal_date = None

    parsed: List[datetime] = []
    for item in class_dates:
        try:
            dt = datetime.fromisoformat(item)
        except Exception:
            continue
        if renewal_date and dt.date() > renewal_date:
            continue
        parsed.append(dt)
    parsed.sort()
    latest = parsed[-1] if parsed else None
    horizon = now + timedelta(weeks=horizon_weeks)
    added = False
    if (not latest) or latest < horizon:
        schedule_pattern = student.get("schedule_pattern", "")
        if schedule_pattern:
            start_date = (latest if latest else now).date()
            new_dates = parse_schedule(
                schedule_pattern,
                start_date=start_date,
                cycle_weeks=horizon_weeks,
            )
            for dt_str in new_dates:
                dt = datetime.fromisoformat(dt_str)
                if latest and dt <= latest:
                    continue
                if renewal_date and dt.date() > renewal_date:
                    continue
                parsed.append(dt)
                added = True
    parsed.sort()
    student["class_dates"] = [dt.isoformat() for dt in parsed]
    return added or len(student["class_dates"]) != original_len


def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id if user else None
        logging.info("Command %s called by user_id=%s", func.__name__, user_id)

        if user_id not in ADMIN_IDS:
            try:
                if update.message:
                    await update.message.reply_text("Sorry, you are not authorized to perform this command.")
                elif update.callback_query:
                    await update.callback_query.answer("Not authorized.", show_alert=True)
            except Exception:
                logging.warning("Unauthorized call with no message/callback context.")
            return

        try:
            return await func(update, context)
        except Exception:
            logging.exception("Error in admin command %s", func.__name__)
            try:
                if update.message:
                    await update.message.reply_text("Oops, something went wrong running that command.")
                elif update.callback_query:
                    await update.callback_query.edit_message_text("Oops, something went wrong running that command.")
            except Exception:
                pass
    return wrapper


# -----------------------------------------------------------------------------
# Command handlers for admin (tutor) side
# -----------------------------------------------------------------------------

@admin_only
async def add_student_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Initiate the conversation to add a new student."""
    await update.message.reply_text(
        "Adding a new student. Please enter the student's name:",
        reply_markup=ReplyKeyboardRemove(),
    )
    context.user_data.clear()
    return ADD_NAME


async def add_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["name"] = update.message.text.strip()
    await update.message.reply_text("Enter the student's Telegram @handle or numeric ID:")
    return ADD_HANDLE


async def add_handle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    handle = update.message.text.strip()
    if handle.startswith("@"):  # strip leading @
        handle = handle[1:]
    if handle.isdigit():
        context.user_data["telegram_id"] = int(handle)
    else:
        context.user_data["telegram_handle"] = normalize_handle(handle)
    await update.message.reply_text("Enter the plan price (numerical value, e.g., 3500):")
    return ADD_PRICE


async def add_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        price = float(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("Invalid price. Please enter a numeric value:")
        return ADD_PRICE
    context.user_data["plan_price"] = price
    await update.message.reply_text("Enter number of classes in the plan (e.g., 8):")
    return ADD_CLASSES


async def add_classes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        num_classes = int(update.message.text.strip())
        if num_classes <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Invalid number. Please enter a positive integer:")
        return ADD_CLASSES
    context.user_data["classes_remaining"] = num_classes
    await update.message.reply_text(
        "Enter the weekly schedule (e.g., 'Monday 17:00, Thursday 17:00'). "
        "Separate multiple entries with commas or leave blank for open schedule:",
    )
    return ADD_SCHEDULE


async def add_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    schedule_input = update.message.text.strip()
    context.user_data["schedule_pattern"] = schedule_input
    await update.message.reply_text(
        "Hours before class when cancellations are 'no deduction' (e.g., 24):",
    )
    return ADD_CUTOFF

async def add_cutoff(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        cutoff = int(update.message.text.strip())
        if cutoff < 0 or cutoff > 168:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Invalid number. Hours before class when cancellations are 'no deduction' (0-168, e.g., 24):"
        )
        return ADD_CUTOFF
    context.user_data["cutoff_hours"] = cutoff
    await update.message.reply_text(
        "Length of the repeating cycle in weeks (e.g., 4 for a monthly cycle):"
    )
    return ADD_WEEKS


async def add_weeks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        weeks = int(update.message.text.strip())
        if weeks <= 0 or weeks > 26:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Invalid number. Length of the repeating cycle in weeks (1-26, e.g., 4):"
        )
        return ADD_WEEKS
    context.user_data["cycle_weeks"] = weeks
    await update.message.reply_text(
        "Class length in hours (e.g., 1.5 for 90 minutes):"
    )
    return ADD_DURATION


async def add_duration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        duration = float(update.message.text.strip())
        if duration <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Invalid duration. Class length in hours (0.5-4.0, e.g., 1.5):"
        )
        return ADD_DURATION
    duration = max(0.5, min(4.0, duration))
    duration = round(duration * 4) / 4
    context.user_data["class_duration_hours"] = duration
    await update.message.reply_text(
        "Enter the renewal date (YYYY-MM-DD). This is when the student is expected to renew payment:",
    )
    return ADD_RENEWAL


async def add_renewal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    date_str = update.message.text.strip()
    renewal = parse_renewal_date(date_str)
    if renewal is None:
        await update.message.reply_text("Invalid date format. Please use YYYY-MM-DD:")
        return ADD_RENEWAL
    context.user_data["renewal_date"] = renewal
    # Build student record
    students = load_students()
    telegram_id = context.user_data.get("telegram_id")
    handle = normalize_handle(context.user_data.get("telegram_handle"))
    key = str(telegram_id) if telegram_id else handle
    if key in students:
        await update.message.reply_text("A student with this identifier already exists. Aborting.")
        return ConversationHandler.END
    schedule_pattern = context.user_data.get("schedule_pattern", "")
    cycle_weeks = context.user_data.get("cycle_weeks", DEFAULT_CYCLE_WEEKS)
    start_date = datetime.now(BASE_TZ).date()
    class_dates = parse_schedule(
        schedule_pattern,
        start_date=start_date,
        cycle_weeks=cycle_weeks,
    )

    student = {
        "name": context.user_data.get("name"),
        "telegram_id": telegram_id,
        "telegram_handle": handle,
        "classes_remaining": context.user_data.get("classes_remaining"),
        "plan_price": context.user_data.get("plan_price"),
        "renewal_date": context.user_data.get("renewal_date"),
        "class_dates": class_dates,
        "schedule_pattern": schedule_pattern,
        "cutoff_hours": context.user_data.get("cutoff_hours"),
        "cycle_weeks": cycle_weeks,
        "class_duration_hours": context.user_data.get("class_duration_hours"),
        "paused": False,
        "free_class_credit": 0,
        "reschedule_credit": 0,
        "notes": [],
    }
    ensure_future_class_dates(student)
    students[key] = student
    save_students(students)
    schedule_student_reminders(context.application, key, student)
    await update.message.reply_text(f"Added student {context.user_data.get('name')} successfully!")
    return ConversationHandler.END


@admin_only
async def log_class_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log a completed class for a student.

    Usage: /logclass <student_key> [YYYY-MM-DDTHH:MM[:SS]+07:00] [note]
    If no datetime is provided, the bot chooses the nearest class within ±12 hours.
    If none is found, it logs the most recent past class.
    student_key can be telegram_id or handle as stored in students.json.
    """
    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: /logclass <student_key> [YYYY-MM-DDTHH:MM[:SS]+07:00] [note]\n"
            "If no datetime is provided, the bot chooses the nearest class within ±12 hours.\n"
            "If none is found, the most recent past class is logged." 
        )
        return
    student_key_input = args[0]
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return

    # Parse optional datetime argument
    dt_input = None
    note_start = 1
    if len(args) >= 2:
        try:
            dt_input = parse_student_datetime(args[1], student)
            note_start = 2
        except ValueError:
            dt_input = None
            note_start = 1
    note = " ".join(args[note_start:]) if len(args) > note_start else ""

    class_dates = student.get("class_dates", [])
    tz = student_timezone(student)
    selected_dt_str: Optional[str] = None
    selected_dt: Optional[datetime] = None

    if dt_input:
        dt_str = dt_input.isoformat()
        if dt_str in class_dates:
            selected_dt_str = dt_str
            selected_dt = dt_input
        else:
            tolerance = timedelta(minutes=5)
            for existing in class_dates:
                try:
                    existing_dt = datetime.fromisoformat(existing)
                except Exception:
                    continue
                if abs(existing_dt - dt_input) <= tolerance:
                    selected_dt_str = existing
                    selected_dt = existing_dt
                    break
            if not selected_dt_str:
                await update.message.reply_text(
                    "That datetime isn’t in the schedule. Pick a scheduled time or omit the datetime to auto-select one near now."
                )
                return
    else:
        now = datetime.now(tz)
        past_candidates: List[datetime] = []
        future_candidates: List[datetime] = []
        past_all: List[datetime] = []
        for existing in class_dates:
            try:
                existing_dt = datetime.fromisoformat(existing)
            except Exception:
                continue
            diff = existing_dt - now
            if diff <= timedelta(0):
                past_all.append(existing_dt)
                if diff >= timedelta(hours=-12):
                    past_candidates.append(existing_dt)
            elif diff <= timedelta(hours=12):
                future_candidates.append(existing_dt)
        if past_candidates:
            selected_dt = max(past_candidates)
            selected_dt_str = selected_dt.isoformat()
        elif future_candidates:
            selected_dt = min(future_candidates)
            selected_dt_str = selected_dt.isoformat()
        elif past_all:
            selected_dt = max(past_all)
            selected_dt_str = selected_dt.isoformat()
        else:
            await update.message.reply_text(
                f"No past classes found for {student_key}."
            )
            return

    # Remove the class occurrence
    try:
        idx = class_dates.index(selected_dt_str)
        class_dates.pop(idx)
    except ValueError:
        await update.message.reply_text("Selected class occurrence not found.")
        return

    # Defensive cleanup from cancelled_dates
    cancelled = student.get("cancelled_dates", [])
    if selected_dt_str in cancelled:
        cancelled.remove(selected_dt_str)

    # Cancel reminder job for this occurrence
    for job in context.application.job_queue.jobs():
        if job.name == f"class_reminder:{student_key}:{selected_dt_str}":
            job.schedule_removal()

    # Deduct balance
    if student.get("free_class_credit", 0) > 0:
        student["free_class_credit"] -= 1
    else:
        if student.get("classes_remaining", 0) > 0:
            student["classes_remaining"] -= 1
        else:
            await update.message.reply_text(
                f"Warning: {student['name']} has no classes remaining. Logging anyway."
            )

    # After deducting, possibly notify the student about low or zero balance
    await maybe_send_balance_warning(context.bot, student)
    save_students(students)

    # Record log
    logs = load_logs()
    note_text = f"completed: {selected_dt_str}"
    if note:
        note_text += f"; {note}"
    logs.append(
        {
            "student": student_key,
            "date": datetime.now(tz).strftime("%Y-%m-%d"),
            "status": "completed",
            "note": note_text,
        }
    )
    save_logs(logs)

    local_dt = selected_dt.astimezone(tz) if selected_dt else datetime.now(tz)
    await update.message.reply_text(
        f"Logged class on {local_dt.strftime('%Y-%m-%d %H:%M')} for {student_key}.",
        reply_markup=ReplyKeyboardRemove(),
    )


@admin_only
async def cancel_class_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel a class from the admin side.

    Usage: /cancelclass <student_key>
    Cancelling a class awards a reschedule credit to the student.
    """
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /cancelclass <student_key>")
        return
    student_key_input = args[0]
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return
    # Award a reschedule credit
    student["reschedule_credit"] = student.get("reschedule_credit", 0) + 1
    save_students(students)
    # Log
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now(student_timezone(student)).strftime("%Y-%m-%d"),
            "status": "cancelled_by_admin",
            "note": "",
        }
    )
    save_logs(logs)
    await update.message.reply_text(
        f"Cancelled a class for {student['name']}. They now have {student['reschedule_credit']} reschedule credit(s)."
    )


@admin_only
async def award_free_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Award a free class credit to a student.

    Usage: /awardfree <student_key>
    """
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /awardfree <student_key>")
        return
    student_key_input = args[0]
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return
    student["free_class_credit"] = student.get("free_class_credit", 0) + 1
    save_students(students)
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now(student_timezone(student)).strftime("%Y-%m-%d"),
            "status": "free_credit_awarded",
            "note": "",
        }
    )
    save_logs(logs)
    await update.message.reply_text(
        f"Awarded a free class credit to {student['name']}. They now have {student['free_class_credit']} free credit(s)."
    )


@admin_only
async def renew_student_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Renew a student's plan by adding classes and setting a new renewal date.

    Usage: /renewstudent <student_key> <num_classes> <YYYY-MM-DD>
    """
    args = context.args
    if len(args) != 3:
        await update.message.reply_text(
            "Usage: /renewstudent <student_key> <num_classes> <YYYY-MM-DD>"
        )
        return
    student_key_input, classes_str, date_str = args
    try:
        num_classes = int(classes_str)
        if num_classes <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Number of classes must be a positive integer.")
        return
    renewal = parse_renewal_date(date_str)
    if renewal is None:
        await update.message.reply_text("Invalid date format. Use YYYY-MM-DD.")
        return
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return
    student["classes_remaining"] = student.get("classes_remaining", 0) + num_classes
    student["renewal_date"] = renewal
    # Reset last balance warning upon renewal
    student.pop("last_balance_warning", None)
    ensure_future_class_dates(student)
    save_students(students)
    schedule_student_reminders(context.application, student_key, student)
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now(student_timezone(student)).strftime("%Y-%m-%d"),
            "status": "renewed",
            "note": f"{num_classes} classes, new renewal {renewal}",
        }
    )
    save_logs(logs)
    await update.message.reply_text(
        f"Renewed {student['name']}: added {num_classes} classes, renewal date set to {renewal}. "
        f"Balance: {student['classes_remaining']}"
    )


@admin_only
async def pause_student_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Pause or unpause a student.

    Usage: /pause <student_key>
    Toggles the paused state.  When paused, reminders and tracking are suppressed.
    """
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /pause <student_key>")
        return
    student_key_input = args[0]
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return
    student["paused"] = not student.get("paused", False)
    save_students(students)
    state = "paused" if student["paused"] else "resumed"
    await update.message.reply_text(f"{student['name']}'s tracking has been {state}.")


def generate_dashboard_summary() -> str:
    """Generate a textual summary for the dashboard command."""

    students = load_students()
    logs = load_logs()
    now = datetime.now()
    today = now.date()
    month_start = date(now.year, now.month, 1)

    active_students = [s for s in students.values() if not s.get("paused")]
    total_hours = 0.0
    for s in active_students:
        entries = [e for e in s.get("schedule_pattern", "").split(",") if e.strip()]
        duration = s.get("class_duration_hours", DEFAULT_DURATION_HOURS)
        total_hours += len(entries) * duration

    today_classes: List[str] = []
    low_balance: List[str] = []
    upcoming_renewals: List[str] = []
    overdue_renewals: List[str] = []
    paused_students: List[str] = []
    free_credits: List[str] = []
    completed = missed = cancelled = rescheduled = 0

    for entry in logs:
        entry_date = datetime.strptime(entry["date"], "%Y-%m-%d").date()
        if entry_date < month_start:
            continue
        status = entry.get("status", "")
        if status == "completed":
            completed += 1
        elif status.startswith("missed"):
            missed += 1
        elif "cancelled" in status:
            cancelled += 1
        elif "rescheduled" in status:
            rescheduled += 1

    for student in students.values():
        tz = student_timezone(student)
        today_student = datetime.now(tz).date()

        if student.get("paused"):
            paused_students.append(student["name"])
        else:
            for dt in get_upcoming_classes(student, count=3):
                if dt.astimezone(tz).date() == today_student:
                    today_classes.append(
                        f"{student['name']} at {dt.astimezone(tz).strftime('%H:%M')}"
                    )
                    break

        remaining = student.get("classes_remaining", 0)
        if remaining <= 2:
            low_balance.append(student["name"])

        renewal_str = student.get("renewal_date")
        if renewal_str:
            try:
                renewal_date = datetime.strptime(renewal_str, "%Y-%m-%d").date()
            except ValueError:
                renewal_date = None
            if renewal_date:
                if renewal_date < today:
                    overdue_renewals.append(
                        f"{student['name']} ({renewal_date.isoformat()})"
                    )
                elif today <= renewal_date <= today + timedelta(days=7):
                    upcoming_renewals.append(
                        f"{student['name']} ({renewal_date.isoformat()})"
                    )

        if student.get("free_class_credit", 0) > 0:
            free_credits.append(
                f"{student['name']} ({student['free_class_credit']})"
            )

    lines: List[str] = ["📊 Dashboard Summary", ""]
    lines.append(f"Active students: {len(active_students)}")
    lines.append(f"Total scheduled hours/week: {total_hours:.1f}")
    lines.append("")
    lines.append(f"Today's classes ({today.isoformat()}):")
    if today_classes:
        lines.extend(f"- {item}" for item in today_classes)
    else:
        lines.append("- None")
    lines.append("")

    lines.append("Students with low class balance:")
    if low_balance:
        lines.extend(f"- {item}" for item in low_balance)
    else:
        lines.append("- None")
    lines.append("")

    lines.append("Upcoming payment renewals (next 7 days):")
    if upcoming_renewals:
        lines.extend(f"- {item}" for item in upcoming_renewals)
    else:
        lines.append("- None")
    lines.append("")

    lines.append("Overdue payment renewals:")
    if overdue_renewals:
        lines.extend(f"- {item}" for item in overdue_renewals)
    else:
        lines.append("- None")
    lines.append("")

    lines.append("Paused students:")
    if paused_students:
        lines.extend(f"- {item}" for item in paused_students)
    else:
        lines.append("- None")
    lines.append("")

    lines.append("Free class credits:")
    if free_credits:
        lines.extend(f"- {item}" for item in free_credits)
    else:
        lines.append("- None")
    lines.append("")

    lines.append("Class statistics (this month):")
    lines.append(f"- Completed: {completed}")
    lines.append(f"- Missed/late cancels: {missed}")
    lines.append(f"- Cancelled: {cancelled}")
    lines.append(f"- Rescheduled: {rescheduled}")

    return "\n".join(lines)


@admin_only
async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display a summary dashboard to the admin."""
    summary = generate_dashboard_summary()
    await update.message.reply_text(summary)


@admin_only
async def download_month_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Generate and send class logs for the specified month."""
    args = context.args
    if args:
        try:
            target = datetime.strptime(args[0], "%Y-%m")
        except ValueError:
            await update.message.reply_text("Invalid format. Use YYYY-MM.")
            return
    else:
        target = datetime.now()
    month_start = date(target.year, target.month, 1)
    next_month = month_start.replace(day=28) + timedelta(days=4)
    month_end = next_month - timedelta(days=next_month.day)
    logs = load_logs()
    month_logs = [
        entry
        for entry in logs
        if month_start
        <= datetime.strptime(entry["date"], "%Y-%m-%d").date()
        <= month_end
    ]
    filename = f"class_logs_{month_start.strftime('%Y_%m')}.json"
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(month_logs, f, indent=2, ensure_ascii=False)
        with open(filename, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=filename,
                caption=f"Logs for {month_start.strftime('%Y-%m')}",
            )
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@admin_only
async def confirm_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm a student's cancellation request.

    Usage: /confirmcancel <student_key>

    The pending cancellation contains a "type" field ("early" or "late").
    Early cancels award a reschedule credit; late cancels deduct a class.
    The cancelled class time is recorded in the student's ``cancelled_dates``.
    """
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /confirmcancel <student_key>")
        return
    student_key_input = args[0]
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return
    pending_cancel = student.get("pending_cancel")
    if not pending_cancel:
        await update.message.reply_text("There is no pending cancellation to confirm.")
        return
    class_time_str = pending_cancel.get("class_time")
    try:
        datetime.fromisoformat(class_time_str)
    except Exception:
        await update.message.reply_text("Invalid class time format; cancellation not confirmed.")
        return
    cancelled_dates = student.setdefault("cancelled_dates", [])
    if class_time_str not in cancelled_dates:
        cancelled_dates.append(class_time_str)
    cancel_type = pending_cancel.get("type", "late")
    if cancel_type == "early":
        student["reschedule_credit"] = student.get("reschedule_credit", 0) + 1
        response = (
            f"Cancellation confirmed for {student['name']}. Reschedule credit added."
        )
        log_status = "cancelled (early)"
    else:
        if student.get("classes_remaining", 0) > 0:
            student["classes_remaining"] -= 1
        await maybe_send_balance_warning(context.bot, student)
        response = (
            f"Cancellation confirmed for {student['name']}. One class deducted."
        )
        log_status = "missed (late cancel)"
    student.pop("pending_cancel", None)
    ensure_future_class_dates(student)
    save_students(students)
    # remove any scheduled reminder for this class and reschedule remaining
    for job in context.application.job_queue.jobs():
        if job.name == f"class_reminder:{student_key}:{class_time_str}":
            job.schedule_removal()
    schedule_student_reminders(context.application, student_key, student)
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now(student_timezone(student)).strftime("%Y-%m-%d"),
            "status": log_status,
            "note": "",
        }
    )
    save_logs(logs)
    await update.message.reply_text(response)


@admin_only
async def reschedule_student_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Reschedule a specific class for a student.

    Usage: /reschedulestudent <student_key> <old_datetime> <new_datetime>

    Datetimes should be in ISO 8601 format and match entries in the
    student's ``class_dates`` list.
    """

    args = context.args
    if len(args) != 3:
        await update.message.reply_text(
            "Usage: /reschedulestudent <student_key> <old_datetime> <new_datetime>"
        )
        return

    student_key_input, old_str, new_str = args
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return
    try:
        old_dt = parse_student_datetime(old_str, student)
        new_dt = parse_student_datetime(new_str, student)
    except ValueError:
        await update.message.reply_text("Datetimes must be in ISO 8601 format.")
        return

    tz = student_timezone(student)
    if new_dt <= datetime.now(tz):
        await update.message.reply_text("Cannot reschedule into the past.")
        return

    class_dates = student.get("class_dates", [])
    original_dates = set(class_dates)
    old_item = None
    for item in class_dates:
        try:
            if parse_student_datetime(item, student) == old_dt:
                old_item = item
                break
        except ValueError:
            continue
    if not old_item:
        await update.message.reply_text("Old datetime not found in student's schedule.")
        return

    class_dates.remove(old_item)
    exists = any(
        parse_student_datetime(item, student) == new_dt for item in class_dates
    )
    new_dt_str = new_dt.isoformat()
    if not exists:
        class_dates.append(new_dt_str)

    cancelled_dates = student.get("cancelled_dates", [])
    warn_msg = ""
    for idx, c_item in enumerate(list(cancelled_dates)):
        try:
            if parse_student_datetime(c_item, student) == new_dt:
                cancelled_dates.pop(idx)
                warn_msg = "New datetime was in cancelled dates; removing."
                break
        except ValueError:
            continue
    student["cancelled_dates"] = cancelled_dates

    class_dates.sort(key=lambda x: parse_student_datetime(x, student))
    student["class_dates"] = class_dates
    ensure_future_class_dates(student)
    save_students(students)

    # Update reminder jobs: remove old, add new and any newly generated future dates
    prefix = f"class_reminder:{student_key}:"
    for job in context.application.job_queue.jobs():
        if job.name == f"{prefix}{old_item}":
            job.schedule_removal()
            break
    added_dates = set(student.get("class_dates", [])) - original_dates
    for dt_str in added_dates:
        schedule_class_reminder(context.application, student_key, student, dt_str)

    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now(tz).strftime("%Y-%m-%d"),
            "status": f"rescheduled:{old_item}->{new_dt_str}",
            "note": "admin_reschedule",
            "admin": update.effective_user.id,
        }
    )
    save_logs(logs)

    msg = f"Rescheduled {student.get('name', student_key)} from {old_item} to {new_dt_str}."
    if warn_msg:
        msg += f" {warn_msg}"
    await update.message.reply_text(msg)


@admin_only
async def remove_student_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a student record and cancel scheduled reminders.

    Usage: /removestudent <student_key> confirm [reason]
    Run once to prompt confirmation, then repeat with ``confirm`` to finalize.
    """
    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: /removestudent <student_key> confirm [reason]"
        )
        return

    student_key_input = args[0]
    confirm = len(args) > 1 and args[1].lower() == "confirm"
    reason = " ".join(args[2:]) if confirm and len(args) > 2 else ""

    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return

    if not confirm:
        await update.message.reply_text(
            "Are you sure you want to remove"
            f" {student.get('name', student_key)}? "
            f"Run /removestudent {student_key} confirm [reason] to confirm."
        )
        return

    telegram_id = str(student.get("telegram_id", ""))
    handle = normalize_handle(student.get("telegram_handle"))

    keys_to_delete = {student_key}
    for k, v in students.items():
        if k == student_key:
            continue
        if telegram_id and str(v.get("telegram_id", "")) == telegram_id:
            keys_to_delete.add(k)
        v_handle = normalize_handle(v.get("telegram_handle"))
        if handle and v_handle and v_handle == handle:
            keys_to_delete.add(k)

    for key in keys_to_delete:
        students.pop(key, None)
    save_students(students)

    for job in context.application.job_queue.jobs():
        name = job.name or ""
        if any(name.startswith(f"class_reminder:{k}:") for k in keys_to_delete):
            job.schedule_removal()

    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now(student_timezone(student)).strftime("%Y-%m-%d"),
            "status": "removed",
            "note": reason,
            "admin": update.effective_user.id,
        }
    )
    save_logs(logs)

    await update.message.reply_text(
        f"Removed {student.get('name', student_key)} from records."
    )


async def view_student(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.info("viewstudent command triggered")

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("Not authorized.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /viewstudent <student_key>")
        return

    student_key_input = context.args[0]
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text("Student not found.")
        return

    # Retrieve upcoming classes and display
    schedule = get_upcoming_classes(
        student, count=len(student.get("class_dates", []))
    )

    renewal_str = student.get("renewal_date")
    renewal_date: Optional[date] = None
    if renewal_str:
        try:
            renewal_date = datetime.strptime(renewal_str, "%Y-%m-%d").date()
        except ValueError:
            renewal_date = None

    lines = [f"Student: {student.get('name', student_key)}"]
    lines.append(f"Classes remaining: {student.get('classes_remaining', 0)}")
    if schedule:
        lines.append("Schedule:")
        for dt in schedule:
            if renewal_date and dt.date() > renewal_date:
                continue
            lines.append(f"  - {dt.strftime('%A %d %b %Y at %H:%M')}")
    else:
        lines.append("Schedule: None")

    await update.message.reply_text("\n".join(lines))


# -----------------------------------------------------------------------------
# Student interface handlers
# -----------------------------------------------------------------------------

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start for students.  Show upcoming class, remaining credits and renewal date."""
    students = load_students()
    user = update.effective_user
    user_id = str(user.id)
    key, student = resolve_student(students, user_id)
    if not student and user.username:
        key, student = resolve_student(students, user.username)
    if not student:
        await update.message.reply_text(
            "Hello! You are not registered with this tutoring bot. Please contact your teacher to be added."
        )
        return
    canonical_key = user_id
    new_handle = normalize_handle(user.username)
    if key != canonical_key:
        students.pop(key, None)
        students[canonical_key] = student
        student["telegram_id"] = int(user_id)
        if new_handle:
            student["telegram_handle"] = new_handle
        save_students(students)
    else:
        updated = False
        if student.get("telegram_id") != int(user_id):
            student["telegram_id"] = int(user_id)
            updated = True
        if new_handle and student.get("telegram_handle") != new_handle:
            student["telegram_handle"] = new_handle
            updated = True
        if updated:
            save_students(students)
    # Build summary message
    upcoming = get_upcoming_classes(student, count=1)
    next_class_str = upcoming[0].strftime("%A %d %b %Y at %H:%M") if upcoming else "No upcoming classes set"
    classes_remaining = student.get("classes_remaining", 0)
    renewal = student.get("renewal_date", "N/A")
    message_lines = [
        f"Hello, {student['name']}!",
        f"Your next class: {next_class_str}",
        f"Classes remaining: {classes_remaining}",
        f"Plan renews on: {renewal}",
    ]
    if student.get("paused"):
        message_lines.append("Your plan is currently paused. Contact your teacher to resume.")
    # Build keyboard
    buttons = []
    buttons.append([InlineKeyboardButton("📅 My Classes", callback_data="my_classes")])
    buttons.append([InlineKeyboardButton("❌ Cancel Class", callback_data="cancel_class")])
    # Show free class credit info if available
    if student.get("free_class_credit", 0) > 0:
        buttons.append([InlineKeyboardButton("🎁 Free Class Credit", callback_data="free_credit")])
    reply_markup = InlineKeyboardMarkup(buttons)
    await update.message.reply_text("\n".join(message_lines), reply_markup=reply_markup)


async def student_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button presses from students (inline keyboard)."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    students = load_students()
    _, student = resolve_student(students, str(user.id))
    if not student and user.username:
        _, student = resolve_student(students, user.username)
    if not student:
        await query.edit_message_text("You are not recognised. Please contact your teacher.")
        return
    data = query.data
    if data == "my_classes":
        await show_my_classes(query, student)
    elif data == "cancel_class":
        await initiate_cancel_class(query, student)
    elif data == "free_credit":
        await show_free_credit(query, student)
    else:
        await query.edit_message_text("Unknown action.")


def build_student_classes_text(student: Dict[str, Any], *, limit: int = 5) -> str:
    """Return the text shown in a student's "My Classes" view."""
    limit = max(1, min(20, limit))
    upcoming_list = get_upcoming_classes(student, count=limit)
    if upcoming_list:
        lines = [f"Upcoming classes for {student['name']}:\n"]
        for dt in upcoming_list:
            lines.append(f"  - {dt.strftime('%A %d %b %Y at %H:%M')}")
    else:
        lines = ["You have no classes scheduled."]
    lines.append(f"Classes remaining: {student.get('classes_remaining', 0)}")
    lines.append(f"Renewal date: {student.get('renewal_date', 'N/A')}")
    if student.get("paused"):
        lines.append("Your plan is currently paused.")
    return "\n".join(lines)


async def show_my_classes(query, student: Dict[str, Any]) -> None:
    """Display upcoming scheduled classes and remaining credits."""
    text = build_student_classes_text(student, limit=5)
    await query.edit_message_text(text, reply_markup=None)


async def initiate_cancel_class(query, student: Dict[str, Any]) -> None:
    """Begin the cancellation process.  Show a list of upcoming classes."""
    upcoming_list = get_upcoming_classes(student, count=5)
    if not upcoming_list:
        await query.edit_message_text("You have no classes to cancel.")
        return
    buttons = []
    for idx, dt in enumerate(upcoming_list):
        label = dt.strftime("%a %d %b %H:%M")
        callback = f"cancel_selected:{idx}"
        buttons.append([InlineKeyboardButton(label, callback_data=callback)])
    keyboard = InlineKeyboardMarkup(buttons)
    await query.edit_message_text("Select a class to cancel:", reply_markup=keyboard)


async def handle_cancel_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the student's selection of a class to cancel."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    students = load_students()
    student_key, student = resolve_student(students, str(user.id))
    if not student and user.username:
        student_key, student = resolve_student(students, user.username)
    if not student:
        await query.edit_message_text("You are not recognised. Please contact your teacher.")
        return
    _, index_str = query.data.split(":")
    try:
        idx = int(index_str)
    except ValueError:
        await query.edit_message_text("Invalid selection.")
        return
    upcoming = get_upcoming_classes(student, count=5)
    if idx >= len(upcoming):
        await query.edit_message_text("Invalid class selected.")
        return
    selected_dt = upcoming[idx]
    tz = student_timezone(student)
    now_tz = datetime.now(tz)
    cutoff_hours = student.get("cutoff_hours", DEFAULT_CUTOFF_HOURS)
    cutoff_dt = selected_dt - timedelta(hours=cutoff_hours)
    cancel_type = "early" if now_tz <= cutoff_dt else "late"
    student["pending_cancel"] = {
        "class_time": selected_dt.isoformat(),
        "requested_at": now_tz.isoformat(),
        "type": cancel_type,
    }
    save_students(students)
    cutoff_str = cutoff_dt.astimezone(tz).strftime("%a %d %b %H:%M")
    if cancel_type == "early":
        message = (
            "Cancellation request sent to your teacher. "
            f"Cancel before {cutoff_str} ({cutoff_hours} hours in your timezone) = no deduction."
        )
    else:
        message = (
            "Cancellation request sent to your teacher. "
            f"You are within {cutoff_hours} hours (cutoff: {cutoff_str} your time) = one class deducted."
        )
    await query.edit_message_text(message)

    # Notify all admins about the cancellation request
    student_name = student.get("name", student_key)
    class_time_str = selected_dt.strftime("%a %d %b %H:%M")
    cancel_type_readable = "Early" if cancel_type == "early" else "Late"
    admin_message = (
        f"🚨 Cancellation Request: {student_name} wants to cancel {class_time_str}. "
        f"Type: {cancel_type_readable}. Use /confirmcancel {student_key}"
    )
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=admin_message)
        except Exception as e:
            logging.warning(
                "Failed to notify admin %s about cancellation from %s: %s",
                admin_id,
                student_name,
                e,
            )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Catch all handler for plain text messages from students."""
    await update.message.reply_text(
        "I'm sorry, I didn't understand that. Please use the menu buttons or commands."
    )


async def show_free_credit(query, student: Dict[str, Any]) -> None:
    """Inform the student about their free class credit(s)."""
    credits = student.get("free_class_credit", 0)
    if credits > 0:
        msg = f"You have {credits} free class credit{'s' if credits > 1 else ''}. You can use it at any time!"
    else:
        msg = "You currently have no free class credits."
    await query.edit_message_text(msg)


# -----------------------------------------------------------------------------
# Automatic jobs (balance warnings, monthly export)
# -----------------------------------------------------------------------------
async def maybe_send_balance_warning(bot, student) -> bool:
    """Send a warning to the student if their balance is low.

    Returns True if the student's record was modified (i.e., a warning was sent).
    """
    if student.get("paused"):
        return False
    remaining = student.get("classes_remaining", 0)
    last_sent = student.get("last_balance_warning")
    if remaining in {2, 1, 0} and last_sent != remaining:
        telegram_id = student.get("telegram_id")
        if not telegram_id:
            return False
        if remaining == 0:
            text = (
                f"Hi {student['name']}, your plan has finished, please renew."
            )
        else:
            text = (
                f"Hi {student['name']}, you have {remaining} class"
                f"{'es' if remaining != 1 else ''} remaining in your plan."
            )
        try:
            await bot.send_message(chat_id=telegram_id, text=text)
            student["last_balance_warning"] = remaining
            return True
        except Exception:
            logging.warning(f"Failed to send low class warning to {student['name']}")
    return False


async def low_class_warning_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send warnings to students when their remaining classes are low."""
    bot = context.bot
    students = load_students()
    changed = False
    for student in students.values():
        if await maybe_send_balance_warning(bot, student):
            changed = True
    if changed:
        save_students(students)


async def monthly_export_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """At the end of the month, send the logs JSON to the admin(s)."""
    bot = context.bot
    # Only run on the last day of the month (checked by looking at tomorrow's day)
    today = datetime.now(BASE_TZ).date()
    if (today + timedelta(days=1)).day != 1:
        return
    logs = load_logs()
    # Determine current month range
    month_start = date(today.year, today.month, 1)
    next_month = month_start.replace(day=28) + timedelta(days=4)
    month_end = next_month - timedelta(days=next_month.day)
    # Filter logs for the month
    month_logs = [entry for entry in logs if month_start <= datetime.strptime(entry["date"], "%Y-%m-%d").date() <= month_end]
    # Dump to JSON string
    month_data = json.dumps(month_logs, indent=2, ensure_ascii=False)
    # Send as file to each admin
    for admin_id in ADMIN_IDS:
        try:
            # Save to temp file
            filename = f"class_logs_{month_start.strftime('%Y_%m')}.json"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(month_data)
            with open(filename, "rb") as f:
                await bot.send_document(chat_id=admin_id, document=f, filename=filename, caption="Monthly class logs")
            os.remove(filename)
        except Exception:
            logging.warning("Failed to send monthly export to admin %s", admin_id)


# -----------------------------------------------------------------------------
# Setup and main entry point
# -----------------------------------------------------------------------------
def main() -> None:
    """Create the bot application and register handlers."""
    # Configure logging
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    logging.info("ADMIN_IDS loaded: %s", ADMIN_IDS)

    if TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
        logging.warning("Please set the TELEGRAM_BOT_TOKEN environment variable or edit the TOKEN constant.")

    # ----------------------------------------------------------------------
    # Configure JobQueue with a timezone-aware scheduler
    #
    # APScheduler in python-telegram-bot requires a pytz timezone object. We
    # specify our desired timezone (Bangkok by default), create a JobQueue, and
    # pass it to the application.
    #
    # If you deploy this bot in a different locale, replace "Asia/Bangkok" with
    # your own timezone, e.g. "UTC" or "America/New_York".  See pytz
    # documentation for valid identifiers.
    tz = BASE_TZ
    job_queue = JobQueue()
    application: Application = (
        ApplicationBuilder()
        .token(TOKEN)
        .job_queue(job_queue)
        .build()
    )

    # Conversation handler for adding student
    async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Fallback handler to gracefully cancel the add‑student conversation."""
        await update.message.reply_text("Operation cancelled.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("addstudent", add_student_command)],
        states={
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name)],
            ADD_HANDLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_handle)],
            ADD_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_price)],
            ADD_CLASSES: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_classes)],
            ADD_SCHEDULE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_schedule)],
            ADD_CUTOFF: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_cutoff)],
            ADD_WEEKS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_weeks)],
            ADD_DURATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_duration)],
            ADD_RENEWAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_renewal)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
        allow_reentry=True,
    )
    application.add_handler(conv_handler)
    # Admin commands
    application.add_handler(CommandHandler("logclass", log_class_command))
    application.add_handler(CommandHandler("cancelclass", cancel_class_command))
    application.add_handler(CommandHandler("awardfree", award_free_command))
    application.add_handler(CommandHandler("renewstudent", renew_student_command))
    application.add_handler(CommandHandler("pause", pause_student_command))
    application.add_handler(CommandHandler("dashboard", dashboard_command))
    application.add_handler(CommandHandler("downloadmonth", download_month_command))
    application.add_handler(CommandHandler("confirmcancel", confirm_cancel_command))
    application.add_handler(CommandHandler("reschedulestudent", reschedule_student_command))
    application.add_handler(CommandHandler("removestudent", remove_student_command))
    application.add_handler(CommandHandler("viewstudent", view_student))

    # Student handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(handle_cancel_selection, pattern=r"^cancel_selected:", block=False))
    application.add_handler(CallbackQueryHandler(student_button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # Ensure schedules extend into the future and reminders are set
    students = load_students()
    changed = False
    for key, student in students.items():
        changed |= ensure_future_class_dates(student)
        schedule_student_reminders(application, key, student)
    if changed:
        save_students(students)
    # Job queue for balance warnings and monthly export
    # Low class warnings at 10:00 every day (timezone-aware)
    application.job_queue.run_daily(low_class_warning_job, time=time(hour=10, minute=0, tzinfo=tz))
    # Monthly export job runs daily at 23:00; it exits early unless it's the last day
    application.job_queue.run_daily(
        monthly_export_job,
        time=time(hour=23, minute=0, tzinfo=tz),
    )
    # Start the bot
    application.run_polling()


if __name__ == "__main__":
    main()
