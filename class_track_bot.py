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
import inspect
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
try:
    from telegram.error import BadRequest
except Exception:  # pragma: no cover - fallback for environments without telegram.error
    class BadRequest(Exception):
        pass

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


# Debug flag controlling extra diagnostics
DEBUG_MODE = os.environ.get("CTRACK_DEBUG", "0") == "1"


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

# Weekday helpers used throughout scheduling utilities
WEEKDAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
WEEKDAY_MAP = {name.lower(): idx for idx, name in enumerate(WEEKDAY_NAMES)}


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
    """Return (canonical_key, student) for a given ID or handle input.

    Preference is given to numeric ``telegram_id`` keys when available.  Handles
    are normalised by stripping any leading ``@`` and lowercasing before
    comparison.  The returned key is exactly as stored in ``students.json``.
    """

    skey = normalize_handle(key)

    # First try a direct numeric lookup
    if skey.isdigit():
        canon = str(int(skey))
        student = students.get(canon)
        if student is not None:
            return canon, student

    # Next search by handle or legacy keys, preferring numeric ids if present
    for k, s in students.items():
        telegram_id = s.get("telegram_id")
        handle_match = skey == k or normalize_handle(s.get("telegram_handle")) == skey
        id_match = telegram_id is not None and str(telegram_id) == skey
        if handle_match or id_match:
            if telegram_id is not None and str(telegram_id) in students:
                canon = str(telegram_id)
                return canon, students[canon]
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


def normalize_log_students(
    logs: List[Dict[str, Any]], students: Dict[str, Any]
) -> bool:
    """Normalise ``student`` fields in log entries to canonical keys.

    Returns True if any log entries were modified.
    """

    changed = False
    for entry in logs:
        student_field = entry.get("student")
        if student_field is None:
            continue
        normalized = normalize_handle(str(student_field))
        canonical, _ = resolve_student(students, normalized)
        new_key = canonical or normalized
        if entry.get("student") != new_key:
            entry["student"] = new_key
            changed = True
    return changed


def load_logs() -> List[Dict[str, Any]]:
    """Load class logs from JSON file and normalise student keys."""
    if not os.path.exists(LOGS_FILE):
        return []
    with open(LOGS_FILE, "r", encoding="utf-8") as f:
        try:
            logs = json.load(f)
        except json.JSONDecodeError:
            logging.error("Failed to parse logs.json; starting with empty log.")
            return []

    students = load_students()
    if normalize_log_students(logs, students):
        save_logs(logs)

    return logs


def save_logs(logs: List[Dict[str, Any]]) -> None:
    """Write logs list back to JSON."""
    with open(LOGS_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, ensure_ascii=False, sort_keys=True)


def parse_log_date(date_str: str) -> date:
    """Return a date object from an ISO date or datetime string."""
    try:
        return datetime.fromisoformat(date_str).date()
    except ValueError:
        return datetime.strptime(date_str, "%Y-%m-%d").date()


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


def parse_day_time(text: str) -> Optional[str]:
    """Validate and normalize a string like ``"Monday 17:00"``.

    Returns the normalized ``"Day HH:MM"`` form or ``None`` if the
    input is invalid. Day names are case-insensitive and times must be
    24-hour ``HH:MM``.
    """
    if not isinstance(text, str):
        return None
    parts = text.strip().split()
    if len(parts) != 2:
        return None
    day_raw, time_raw = parts
    weekday_idx = WEEKDAY_MAP.get(day_raw.lower())
    if weekday_idx is None:
        return None
    try:
        hour, minute = map(int, time_raw.split(":"))
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return f"{WEEKDAY_NAMES[weekday_idx]} {hour:02d}:{minute:02d}"


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
        await getattr(context, "bot", None).send_message(chat_id=chat_id, text=msg)
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


def regenerate_future_class_dates(student: Dict[str, Any], *, now: Optional[datetime] = None) -> None:
    """Regenerate future ``class_dates`` based on ``schedule_pattern``.

    Past class dates are preserved. Future dates are generated from the
    current ``schedule_pattern`` from ``now`` up to the student's
    ``renewal_date`` (if any).  ``class_dates`` remain sorted and
    de-duplicated and cancelled dates are skipped.
    """
    tz = student_timezone(student)
    if now is None:
        now = datetime.now(tz)
    pattern = student.get("schedule_pattern", "")
    entries = [e.strip() for e in pattern.split(",") if e.strip()]
    past: List[datetime] = []
    for item in student.get("class_dates", []):
        try:
            dt = datetime.fromisoformat(item)
        except Exception:
            continue
        if dt <= now:
            past.append(dt)
    renewal_str = student.get("renewal_date")
    renewal_date = date.fromisoformat(renewal_str) if renewal_str else None
    horizon_weeks = student.get("cycle_weeks", DEFAULT_CYCLE_WEEKS)
    if renewal_date:
        diff_weeks = max(0, (renewal_date - now.date()).days // 7 + 1)
        horizon_weeks = max(horizon_weeks, diff_weeks)
    future: List[datetime] = []
    if entries:
        gen = parse_schedule(
            ", ".join(entries), start_date=now.date(), cycle_weeks=horizon_weeks
        )
        cancelled = set(student.get("cancelled_dates", []))
        for dt_str in gen:
            try:
                dt = datetime.fromisoformat(dt_str)
            except Exception:
                continue
            if dt <= now:
                continue
            if renewal_date and dt.date() > renewal_date:
                continue
            if dt.isoformat() in cancelled:
                continue
            future.append(dt)
    all_dates = sorted({dt.isoformat() for dt in past + future})
    student["class_dates"] = all_dates


def edit_weekly_slot(
    student_key: str,
    student: Dict[str, Any],
    index: int,
    new_entry: str,
    *,
    now: Optional[datetime] = None,
    application: Optional[Application] = None,
) -> None:
    """Replace one entry in ``schedule_pattern`` and regenerate future dates."""
    normalized = parse_day_time(new_entry)
    if normalized is None:
        raise ValueError("Invalid day/time format")
    pattern = student.get("schedule_pattern", "")
    entries = [e.strip() for e in pattern.split(",") if e.strip()]
    if index < 0 or index >= len(entries):
        raise IndexError("slot index out of range")
    old_entry = entries[index]
    entries[index] = normalized
    student["schedule_pattern"] = ", ".join(entries)
    # maintain slot-specific durations
    durations = student.get("slot_durations")
    if durations and old_entry in durations:
        durations[normalized] = durations.pop(old_entry)
        student["slot_durations"] = durations
    regenerate_future_class_dates(student, now=now)
    if application:
        schedule_student_reminders(application, student_key, student)


def add_weekly_slot(
    student_key: str,
    student: Dict[str, Any],
    entry: str,
    *,
    now: Optional[datetime] = None,
    application: Optional[Application] = None,
) -> None:
    """Append a new repeating slot to ``schedule_pattern``."""
    normalized = parse_day_time(entry)
    if normalized is None:
        raise ValueError("Invalid day/time format")
    pattern = student.get("schedule_pattern", "")
    entries = [e.strip() for e in pattern.split(",") if e.strip()]
    if normalized in entries:
        raise ValueError("Slot already exists")
    entries.append(normalized)
    student["schedule_pattern"] = ", ".join(entries)
    regenerate_future_class_dates(student, now=now)
    if application:
        schedule_student_reminders(application, student_key, student)


def delete_weekly_slot(
    student_key: str,
    student: Dict[str, Any],
    index: int,
    *,
    now: Optional[datetime] = None,
    application: Optional[Application] = None,
) -> None:
    """Remove a slot from ``schedule_pattern`` and drop its future classes."""
    pattern = student.get("schedule_pattern", "")
    entries = [e.strip() for e in pattern.split(",") if e.strip()]
    if index < 0 or index >= len(entries):
        raise IndexError("slot index out of range")
    removed = entries.pop(index)
    student["schedule_pattern"] = ", ".join(entries)
    durations = student.get("slot_durations")
    if durations and removed in durations:
        durations.pop(removed, None)
        student["slot_durations"] = durations
    regenerate_future_class_dates(student, now=now)
    if application:
        schedule_student_reminders(application, student_key, student)


def reschedule_single_class(
    student_key: str,
    student: Dict[str, Any],
    old_dt_str: str,
    new_dt_input: str,
    *,
    now: Optional[datetime] = None,
    application: Optional[Application] = None,
    log: bool = True,
) -> None:
    """Move one upcoming class to a new datetime."""
    tz = student_timezone(student)
    if now is None:
        now = datetime.now(tz)
    try:
        old_dt = datetime.fromisoformat(old_dt_str)
    except Exception as e:
        raise ValueError("Invalid old datetime") from e
    if "T" in new_dt_input:
        new_dt = parse_student_datetime(new_dt_input, student)
    else:
        norm = parse_day_time(new_dt_input)
        if norm is None:
            try:
                hour, minute = map(int, new_dt_input.strip().split(":"))
                target_weekday = old_dt.weekday()
            except Exception as e:
                raise ValueError("Invalid datetime") from e
        else:
            day_name, time_part = norm.split()
            target_weekday = WEEKDAY_MAP[day_name.lower()]
            hour, minute = map(int, time_part.split(":"))
        delta_days = (target_weekday - old_dt.weekday()) % 7
        new_dt = tz.normalize(old_dt + timedelta(days=delta_days))
        new_dt = new_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if new_dt <= now:
        raise ValueError("Cannot reschedule into the past")
    renewal_str = student.get("renewal_date")
    if renewal_str:
        renewal_date = date.fromisoformat(renewal_str)
        if new_dt.date() > renewal_date:
            raise ValueError("Beyond renewal date")
    class_dates = [datetime.fromisoformat(x) for x in student.get("class_dates", [])]
    try:
        class_dates.remove(old_dt)
    except ValueError:
        raise ValueError("Old datetime not found")
    if new_dt not in class_dates:
        class_dates.append(new_dt)
    class_dates.sort()
    student["class_dates"] = [dt.isoformat() for dt in class_dates]
    cancelled = student.get("cancelled_dates", [])
    if new_dt.isoformat() in cancelled:
        cancelled.remove(new_dt.isoformat())
    student["cancelled_dates"] = cancelled
    if application:
        schedule_student_reminders(application, student_key, student)
    if log:
        logs = load_logs()
        logs.append(
            {
                "student": student_key,
                "date": now.isoformat(),
                "status": "rescheduled",
                "note": f"to {new_dt.isoformat()}",
            }
        )
        save_logs(logs)


def cancel_single_class(
    student_key: str,
    student: Dict[str, Any],
    dt_str: str,
    *,
    grant_credit: bool = False,
    application: Optional[Application] = None,
    log: bool = True,
) -> None:
    """Mark one upcoming class as cancelled."""
    cancelled = student.setdefault("cancelled_dates", [])
    if dt_str not in cancelled:
        cancelled.append(dt_str)
    if grant_credit:
        student["reschedule_credit"] = student.get("reschedule_credit", 0) + 1
    if application:
        schedule_student_reminders(application, student_key, student)
    if log:
        logs = load_logs()
        logs.append(
            {
                "student": student_key,
                "date": datetime.now(BASE_TZ).isoformat(),
                "status": "cancelled (admin)",
                "note": dt_str,
            }
        )
        save_logs(logs)


def set_class_length(student: Dict[str, Any], duration_hours: float, slot: Optional[str] = None) -> None:
    """Update class duration globally or for a specific weekly slot."""
    duration_hours = round(duration_hours * 4) / 4
    if slot:
        durations = student.setdefault("slot_durations", {})
        durations[slot] = duration_hours
        student["slot_durations"] = durations
    else:
        student["class_duration_hours"] = duration_hours

def bulk_shift_slot(
    student_key: str,
    student: Dict[str, Any],
    index: int,
    *,
    new_entry: Optional[str] = None,
    offset_minutes: int = 0,
    now: Optional[datetime] = None,
    application: Optional[Application] = None,
) -> None:
    """Shift all future occurrences of one slot."""
    tz = student_timezone(student)
    if now is None:
        now = datetime.now(tz)
    pattern = student.get("schedule_pattern", "")
    entries = [e.strip() for e in pattern.split(",") if e.strip()]
    if index < 0 or index >= len(entries):
        raise IndexError("slot index out of range")
    old_entry = entries[index]
    if new_entry is not None:
        normalized = parse_day_time(new_entry)
        if normalized is None:
            raise ValueError("Invalid day/time format")
        entries[index] = normalized
        student["schedule_pattern"] = ", ".join(entries)
        durations = student.get("slot_durations")
        if durations and old_entry in durations:
            durations[normalized] = durations.pop(old_entry)
            student["slot_durations"] = durations
        regenerate_future_class_dates(student, now=now)
    else:
        delta = timedelta(minutes=offset_minutes)
        class_dates: List[datetime] = []
        for item in student.get("class_dates", []):
            try:
                dt = datetime.fromisoformat(item)
            except Exception:
                continue
            if dt > now and dt.strftime("%A %H:%M") == old_entry:
                new_dt = tz.normalize(dt + delta)
                if new_dt > now:
                    class_dates.append(new_dt)
                # else drop past ones
            else:
                class_dates.append(dt)
        class_dates = sorted({dt.isoformat() for dt in class_dates})
        student["class_dates"] = class_dates
    if application:
        schedule_student_reminders(application, student_key, student)


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
    await maybe_send_balance_warning(getattr(context, "bot", None), student)
    save_students(students)

    # Record log
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": selected_dt_str or datetime.now(tz).isoformat(),
            "status": "completed",
            "note": note or "",
        }
    )
    save_logs(logs)

    local_dt = selected_dt.astimezone(tz) if selected_dt else datetime.now(tz)
    await update.message.reply_text(
        f"Logged class on {local_dt.strftime('%Y-%m-%d %H:%M')} for {student_key}.",
        reply_markup=ReplyKeyboardRemove(),
    )


@admin_only
async def _cancel_class_command_legacy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
            "date": datetime.now(student_timezone(student)).isoformat(),
            "status": "cancelled",
            "note": "admin cancel",
        }
    )
    save_logs(logs)
    await update.message.reply_text(
        f"Cancelled a class for {student['name']}. They now have {student['reschedule_credit']} reschedule credit(s)."
    )
    await refresh_student_menu(student_key, student, getattr(context, "bot", None))


@admin_only
async def cancel_class_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start admin-driven cancellation for a student's upcoming class.

    Usage: /cancelclass <student_key> [--late] [--note "..."]
    Default behaviour grants a reschedule credit. Use --late to deduct instead.
    """
    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: /cancelclass <student_key> [--late] [--note \"...\"]"
        )
        return
    late = False
    note = ""
    student_key_input = None
    i = 0
    while i < len(args):
        if args[i] == "--late":
            late = True
            i += 1
        elif args[i] == "--note":
            note = " ".join(args[i + 1 :])
            break
        elif student_key_input is None:
            student_key_input = args[i]
            i += 1
        else:
            i += 1
    if not student_key_input:
        await update.message.reply_text("Student key required.")
        return
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return
    upcoming_list = get_upcoming_classes(student, count=8)
    if not upcoming_list:
        await update.message.reply_text("No upcoming classes to cancel.")
        return
    context.user_data["admin_cancel"] = {
        "student_key": student_key,
        "late": late,
        "note": note,
    }
    buttons = []
    for idx, dt in enumerate(upcoming_list):
        label = dt.strftime("%a %d %b %H:%M")
        buttons.append(
            [InlineKeyboardButton(label, callback_data=f"admin_cancel_sel:{idx}")]
        )
    await update.message.reply_text(
        "Select class to cancel:", reply_markup=InlineKeyboardMarkup(buttons)
    )


@admin_only
async def admin_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle selection of class to cancel from admin command."""
    query = update.callback_query
    await query.answer()
    info = context.user_data.get("admin_cancel")
    if not info:
        await query.edit_message_text("No pending cancellation.")
        return
    students = load_students()
    student_key = info.get("student_key")
    student = students.get(student_key)
    if not student:
        await query.edit_message_text("Student not found.")
        return
    try:
        index = int(query.data.split(":")[1])
    except Exception:
        await query.edit_message_text("Invalid selection.")
        return
    upcoming_list = get_upcoming_classes(student, count=8)
    if index < 0 or index >= len(upcoming_list):
        await query.edit_message_text("Invalid selection.")
        return
    dt = upcoming_list[index]
    grant_credit = not info.get("late")
    cancel_single_class(
        student_key,
        student,
        dt.isoformat(),
        grant_credit=grant_credit,
        application=context.application,
        log=False,
    )
    if not grant_credit and student.get("classes_remaining", 0) > 0:
        student["classes_remaining"] -= 1
        await maybe_send_balance_warning(getattr(context, "bot", None), student)
    save_students(students)
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now(student_timezone(student)).isoformat(),
            "status": "cancelled_late" if info.get("late") else "cancelled",
            "note": info.get("note", ""),
        }
    )
    save_logs(logs)
    await refresh_student_menu(student_key, student, getattr(context, "bot", None))
    msg = (
        f"Cancelled {dt.strftime('%d %b %H:%M')}, reschedule credit granted."
        if grant_credit
        else f"Cancelled {dt.strftime('%d %b %H:%M')}, one class deducted."
    )
    await query.edit_message_text(msg)
    context.user_data.pop("admin_cancel", None)


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
            "date": datetime.now(student_timezone(student)).isoformat(),
            "status": "free_credit_awarded",
            "note": "admin award free credit",
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
        entry_date = parse_log_date(entry["date"])
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
async def dayview_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show inline keyboard with today and the next six days."""
    now = datetime.now(BASE_TZ)
    today = now.date()
    buttons: List[List[InlineKeyboardButton]] = []
    for i in range(7):
        target = today + timedelta(days=i)
        label = f"{target.strftime('%a')} {target.day} {target.strftime('%b')}"
        buttons.append(
            [
                InlineKeyboardButton(
                    label, callback_data=f"dayview:{target.isoformat()}"
                )
            ]
        )
    await update.message.reply_text(
        "Select a day:", reply_markup=InlineKeyboardMarkup(buttons)
    )


@admin_only
async def dayview_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle callback for a specific day's class view."""
    query = update.callback_query
    await query.answer()
    data = query.data.split(":", 1)
    if len(data) != 2:
        await query.edit_message_text("Invalid day selection.")
        return
    try:
        target_date = datetime.fromisoformat(data[1]).date()
    except ValueError:
        await query.edit_message_text("Invalid day selection.")
        return

    students = load_students()
    entries: List[Tuple[datetime, str]] = []
    now = datetime.now(BASE_TZ)
    for student in students.values():
        if student.get("paused"):
            continue
        cancelled = set(student.get("cancelled_dates", []))
        renewal_date = None
        renewal_str = student.get("renewal_date")
        if renewal_str:
            try:
                renewal_date = datetime.strptime(renewal_str, "%Y-%m-%d").date()
            except ValueError:
                renewal_date = None
        for item in student.get("class_dates", []):
            try:
                dt = parse_student_datetime(item, student).astimezone(BASE_TZ)
            except Exception:
                continue
            if dt.date() != target_date:
                continue
            if item in cancelled:
                continue
            if renewal_date and dt.date() > renewal_date:
                continue
            if target_date == now.date() and dt < now:
                continue
            entries.append((dt, student.get("name", "Unknown")))
    entries.sort(key=lambda x: x[0])
    if entries:
        text = "\n".join(f"{dt.strftime('%H:%M')} – {name}" for dt, name in entries)
    else:
        label = target_date.strftime("%a %d %b %Y")
        text = f"No classes for {label}."
    await query.edit_message_text(text)


@admin_only
async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display a summary dashboard to the admin."""
    summary = generate_dashboard_summary()
    await update.message.reply_text(summary)


@admin_only
async def list_students_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List all active students with their handles."""
    students = load_students()
    active = [s for s in students.values() if not s.get("paused")]
    if not active:
        await update.message.reply_text("No active students found.")
        return

    lines = ["Active students:"]
    for s in sorted(active, key=lambda x: x.get("name", "").lower()):
        handle = s.get("telegram_handle")
        if handle:
            ident = f"@{handle}"
        elif s.get("telegram_id"):
            ident = f"id {s['telegram_id']}"
        else:
            ident = "no handle"
        lines.append(f"- {s['name']} ({ident})")

    await update.message.reply_text("\n".join(lines))


@admin_only
async def edit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Entry point for editing a student's schedule or metadata."""
    students = load_students()
    buttons: List[List[InlineKeyboardButton]] = []
    for key, student in students.items():
        if student.get("paused"):
            continue
        name = student.get("name", key)
        handle = student.get("telegram_handle")
        label = name
        if handle:
            label += f" @{handle}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"edit:pick:{key}")])
    if not buttons:
        await update.message.reply_text("No active students found.")
        return
    await update.message.reply_text(
        "Select a student to edit:", reply_markup=InlineKeyboardMarkup(buttons)
    )


@admin_only
async def edit_pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle selection of a student to edit."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    if len(parts) < 3:
        await query.edit_message_text("Student not found")
        return
    student_key_input = parts[2]
    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await query.edit_message_text("Student not found")
        return
    context.user_data["edit_student_key"] = student_key
    menu_buttons = [
        [InlineKeyboardButton("Change class time", callback_data="edit:option:changetime")],
        [InlineKeyboardButton("Change class length", callback_data="edit:option:length")],
        [InlineKeyboardButton("Add weekly class", callback_data="edit:option:addweekly")],
        [InlineKeyboardButton("Delete weekly class", callback_data="edit:option:delweekly")],
        [InlineKeyboardButton("Cancel one class", callback_data="edit:option:cancel")],
        [InlineKeyboardButton("Back", callback_data="edit:back")],
    ]
    await query.edit_message_text(
        f"Editing {student.get('name', student_key)}. Choose an action:",
        reply_markup=InlineKeyboardMarkup(menu_buttons),
    )


@admin_only
async def edit_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle menu actions after a student has been chosen."""
    query = update.callback_query
    await query.answer()
    data = query.data
    action = data.split(":", 2)[2] if ":" in data else ""
    student_key = context.user_data.get("edit_student_key")
    students = load_students()
    student = students.get(student_key) if student_key else None
    if not student:
        await query.edit_message_text("Student not found")
        return
    if action == "length":
        duration = student.get("class_duration_hours", DEFAULT_DURATION_HOURS)
        context.user_data["edit_state"] = "await_length"
        await query.edit_message_text(
            f"Current class length: {duration}h. Enter new length (0.5-4.0):"
        )
    elif action == "addweekly":
        context.user_data["edit_state"] = "await_addweekly"
        await query.edit_message_text("Enter new weekly slot (e.g., 'Tuesday 19:00'):")
    elif action == "delweekly":
        pattern = student.get("schedule_pattern", "")
        entries = [e.strip() for e in pattern.split(",") if e.strip()]
        if not entries:
            await query.edit_message_text("No weekly slots to delete.")
            return
        buttons = []
        for idx, entry in enumerate(entries):
            buttons.append(
                [InlineKeyboardButton(entry, callback_data=f"edit:delweekly:{student_key}:{idx}")]
            )
        await query.edit_message_text(
            "Select slot to remove:", reply_markup=InlineKeyboardMarkup(buttons)
        )
    elif action == "changetime":
        pattern = student.get("schedule_pattern", "")
        entries = [e.strip() for e in pattern.split(",") if e.strip()]
        if not entries:
            await query.edit_message_text("No weekly slots to edit.")
            return
        buttons = []
        for idx, entry in enumerate(entries):
            buttons.append([
                InlineKeyboardButton(entry, callback_data=f"edit:time:slot:{student_key}:{idx}")
            ])
        buttons.append([InlineKeyboardButton("Back", callback_data=f"edit:pick:{student_key}")])
        await query.edit_message_text(
            "Select slot to change:", reply_markup=InlineKeyboardMarkup(buttons)
        )
    elif action == "cancel":
        context.user_data["edit_state"] = "await_cancel"
        await query.edit_message_text(
            "Enter class datetime to cancel (e.g., '2025-08-25T17:00+07:00')."
        )
    else:
        await query.edit_message_text("Unknown action.")


@admin_only
async def edit_delweekly_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a weekly slot from a student's schedule."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    if len(parts) != 4:
        await query.edit_message_text("Invalid selection.")
        return
    student_key = parts[2]
    try:
        index = int(parts[3])
    except ValueError:
        await query.edit_message_text("Invalid selection.")
        return
    students = load_students()
    student = students.get(student_key)
    if not student:
        await query.edit_message_text("Student not found")
        return
    pattern = student.get("schedule_pattern", "")
    entries = [e.strip() for e in pattern.split(",") if e.strip()]
    if index < 0 or index >= len(entries):
        await query.edit_message_text("Invalid selection.")
        return
    removed = entries.pop(index)
    student["schedule_pattern"] = ", ".join(entries)
    # Regenerate future classes from now
    start = datetime.now(BASE_TZ).date()
    cycle_weeks = student.get("cycle_weeks", DEFAULT_CYCLE_WEEKS)
    student["class_dates"] = parse_schedule(
        student.get("schedule_pattern", ""), start_date=start, cycle_weeks=cycle_weeks
    )
    save_students(students)
    schedule_student_reminders(context.application, student_key, student)
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now(BASE_TZ).isoformat(),
            "status": "pattern_updated",
            "note": f"removed {removed}",
            "admin": update.effective_user.id if update.effective_user else None,
        }
    )
    save_logs(logs)
    await refresh_student_menu(student_key, student, getattr(context, "bot", None))
    await query.edit_message_text(f"Removed weekly slot {removed} for {student['name']}.")


@admin_only
async def edit_time_slot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Present scope options after picking a slot."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    if len(parts) != 5:
        await query.edit_message_text("Invalid selection.")
        return
    student_key = parts[3]
    try:
        index = int(parts[4])
    except ValueError:
        await query.edit_message_text("Invalid selection.")
        return
    context.user_data["edit_time_slot_index"] = index
    buttons = [
        [
            InlineKeyboardButton(
                "Just once",
                callback_data=f"edit:time:scope:once:{student_key}:{index}",
            )
        ],
        [
            InlineKeyboardButton(
                "All future",
                callback_data=f"edit:time:scope:all:{student_key}:{index}",
            )
        ],
        [InlineKeyboardButton("Back", callback_data="edit:option:changetime")],
    ]
    await query.edit_message_text(
        "Change this slot once or all future?", reply_markup=InlineKeyboardMarkup(buttons)
    )


@admin_only
async def edit_time_scope_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle scope selection for changing class time."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    if len(parts) != 6:
        await query.edit_message_text("Invalid selection.")
        return
    scope = parts[3]
    student_key = parts[4]
    try:
        index = int(parts[5])
    except ValueError:
        await query.edit_message_text("Invalid selection.")
        return
    students = load_students()
    student = students.get(student_key)
    if not student:
        await query.edit_message_text("Student not found")
        return
    pattern = student.get("schedule_pattern", "")
    entries = [e.strip() for e in pattern.split(",") if e.strip()]
    if index < 0 or index >= len(entries):
        await query.edit_message_text("Invalid selection.")
        return
    if scope == "all":
        context.user_data["edit_state"] = "await_time_all"
        context.user_data["edit_slot_index"] = index
        context.user_data["edit_old_entry"] = entries[index]
        await query.edit_message_text(
            "Enter new day and time (e.g., 'Tuesday 19:00')."
        )
    elif scope == "once":
        entry = entries[index]
        now = datetime.now(BASE_TZ)
        upcoming = []
        for dt_str in student.get("class_dates", []):
            dt = parse_student_datetime(dt_str, student)
            if dt >= now and dt.strftime("%A %H:%M") == entry:
                upcoming.append(dt)
            if len(upcoming) >= 6:
                break
        if not upcoming:
            await query.edit_message_text("No upcoming classes for that slot.")
            return
        buttons = []
        for dt in upcoming:
            iso = dt.isoformat()
            buttons.append(
                [
                    InlineKeyboardButton(
                        iso,
                        callback_data=f"edit:time:oncepick:{student_key}:{iso}",
                    )
                ]
            )
        buttons.append([InlineKeyboardButton("Back", callback_data="edit:option:changetime")])
        await query.edit_message_text(
            "Select occurrence to reschedule:", reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await query.edit_message_text("Invalid selection.")


@admin_only
async def edit_time_oncepick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt for new time after picking a single occurrence."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    if len(parts) != 4:
        await query.edit_message_text("Invalid selection.")
        return
    student_key = parts[2]
    old_dt = parts[3]
    context.user_data["edit_state"] = "await_time_once"
    context.user_data["edit_once_old_dt"] = old_dt
    await query.edit_message_text(
        "Enter new day and time (e.g., 'Tuesday 19:00' or '19:00')."
    )


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
        if month_start <= parse_log_date(entry["date"]) <= month_end
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
        await maybe_send_balance_warning(getattr(context, "bot", None), student)
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
            "date": class_time_str,
            "status": log_status,
            "note": "admin confirm cancel",
        }
    )
    save_logs(logs)
    await update.message.reply_text(response)
    await refresh_student_menu(student_key, student, getattr(context, "bot", None))
    await refresh_student_my_classes(student_key, student, getattr(context, "bot", None))


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
            "date": old_item,
            "status": "rescheduled",
            "note": f"to {new_dt_str}",
            "admin": update.effective_user.id,
        }
    )
    save_logs(logs)

    msg = f"Rescheduled {student.get('name', student_key)} from {old_item} to {new_dt_str}."
    if warn_msg:
        msg += f" {warn_msg}"
    await update.message.reply_text(msg)
    await refresh_student_menu(student_key, student, getattr(context, "bot", None))


@admin_only
async def remove_student_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a student record and cancel scheduled reminders.

    Usage: /removestudent <student_key> confirm [purge] [reason]
    Run once to prompt confirmation, then repeat with ``confirm`` to finalize.
    Include ``purge`` after ``confirm`` to also delete other records sharing the
    same Telegram ID or handle.
    """
    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: /removestudent <student_key> confirm [purge] [reason]"
        )
        return

    student_key_input = args[0]
    confirm = len(args) > 1 and args[1].lower() == "confirm"
    purge = False
    reason = ""
    if confirm:
        remaining = list(args[2:])
        if remaining and remaining[0].lower() == "purge":
            purge = True
            remaining = remaining[1:]
        reason = " ".join(remaining) if remaining else ""

    students = load_students()
    student_key, student = resolve_student(students, student_key_input)
    if not student:
        await update.message.reply_text(f"Student '{student_key_input}' not found.")
        return

    if not confirm:
        await update.message.reply_text(
            "Are you sure you want to remove"
            f" {student.get('name', student_key)}? "
            f"Run /removestudent {student_key} confirm [purge] [reason] to confirm."
        )
        return

    telegram_id = str(student.get("telegram_id", ""))
    handle = normalize_handle(student.get("telegram_handle"))

    duplicates = set()
    for k, v in students.items():
        if k == student_key:
            continue
        if telegram_id and str(v.get("telegram_id", "")) == telegram_id:
            duplicates.add(k)
            continue
        v_handle = normalize_handle(v.get("telegram_handle"))
        if handle and v_handle and v_handle == handle:
            duplicates.add(k)

    keys_to_delete = {student_key}
    if purge:
        keys_to_delete.update(duplicates)

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

    msg = f"Removed {student.get('name', student_key)} from records."
    if duplicates and not purge:
        msg += (
            f" {len(duplicates)} other record(s) share this contact. "
            f"Run /removestudent {student_key} confirm purge to remove them."
        )
    elif duplicates and purge:
        msg = (
            f"Removed {student.get('name', student_key)} and {len(duplicates)} "
            "duplicate record(s)."
        )
    await update.message.reply_text(msg)


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
# Diagnostic and admin helper commands


@admin_only
async def selftest_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Self-test: tap Ping, then My Classes in student chat.",
        reply_markup=build_debug_keyboard(),
    )
    logging.warning("SELFTEST marker fired")


@admin_only
async def datacheck_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    students = load_students()
    logs = load_logs()
    total = len(students)
    both_fields = sum(
        1
        for s in students.values()
        if s.get("telegram_id") and s.get("telegram_handle")
    )
    mismatch = sum(
        1
        for key, s in students.items()
        if s.get("telegram_id") and key != str(s.get("telegram_id"))
    )
    pending_cancel = sum(1 for s in students.values() if s.get("pending_cancel"))
    logging.info(
        "datacheck stats total=%s both=%s mismatch=%s pending_cancel=%s",
        total,
        both_fields,
        mismatch,
        pending_cancel,
    )

    canonical: Dict[str, str] = {}
    for key, s in students.items():
        tid = s.get("telegram_id")
        handle = normalize_handle(s.get("telegram_handle"))
        canon = str(tid) if tid else (handle or normalize_handle(key))
        canonical[key] = canon

    rekeyed = 0
    if any(key != canon for key, canon in canonical.items()):
        new_students: Dict[str, Any] = {}
        for key, s in students.items():
            canon = canonical[key]
            existing = new_students.get(canon)
            if existing:
                for k, v in s.items():
                    if k not in existing:
                        existing[k] = v
            else:
                new_students[canon] = s
            if key != canon:
                rekeyed += 1
        students.clear()
        students.update(new_students)

    id_map = {
        str(s.get("telegram_id")): key
        for key, s in students.items()
        if s.get("telegram_id")
    }
    handle_map: Dict[str, str] = {}
    for key, s in students.items():
        handle = normalize_handle(s.get("telegram_handle"))
        if handle:
            handle_map[handle] = key
        handle_map[normalize_handle(key)] = key

    logs_fixed = 0
    for entry in logs:
        val = entry.get("student")
        canon = None
        if val in students:
            canon = val
        else:
            val_str = str(val)
            norm = normalize_handle(val_str)
            if val_str in students:
                canon = val_str
            elif val_str in id_map:
                canon = id_map[val_str]
            elif norm in handle_map:
                canon = handle_map[norm]
        if canon and canon != val:
            entry["student"] = canon
            logs_fixed += 1

    if rekeyed:
        save_students(students)
    if logs_fixed:
        save_logs(logs)

    await update.message.reply_text(
        f"DataCheck: students={total}, rekeyed={rekeyed}, logs_fixed={logs_fixed}, pending_cancel={pending_cancel}"
    )


def check_log_students(
    students: Dict[str, Any], logs: List[Dict[str, Any]]
) -> Tuple[int, int, int, List[str]]:
    """Return statistics on log entries matching known students."""

    total = len(logs)
    matched = 0
    unmatched = 0
    samples: List[str] = []

    for entry in logs:
        student_field = entry.get("student")
        normalized = normalize_handle(str(student_field))
        canonical, _ = resolve_student(students, normalized)
        if canonical is not None:
            matched += 1
        else:
            unmatched += 1
            if len(samples) < 10:
                samples.append(str(student_field))

    return total, matched, unmatched, samples


@admin_only
async def checklogs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    students = load_students()
    logs = load_logs()
    total, matched, unmatched, samples = check_log_students(students, logs)

    if unmatched:
        sample_text = ", ".join(samples)
        message = (
            "📝 Log Check:\n"
            f"Total logs: {total}\n"
            f"Matched: {matched}\n"
            f"Unmatched: {unmatched}\n"
            f"Sample bad keys: {sample_text}"
        )
    else:
        message = (
            "📝 Log Check:\n"
            f"Total logs: {total}\n"
            f"Matched: {matched}\n"
            f"Unmatched: {unmatched}\n"
            "No problems found ✅"
        )

    await update.message.reply_text(message)


@admin_only
async def nukepending_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or context.args[0].lower() != "confirm":
        await update.message.reply_text("Usage: /nukepending confirm")
        return
    students = load_students()
    count = 0
    for s in students.values():
        if s.pop("pending_cancel", None):
            count += 1
    if count:
        save_students(students)
    await update.message.reply_text(
        f"Cleared pending_cancel for {count} students."
    )


# -----------------------------------------------------------------------------
# Student interface handlers
# -----------------------------------------------------------------------------


def build_start_message(student: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    """Return the welcome text and keyboard for a student."""
    upcoming = get_upcoming_classes(student, count=1)
    next_class_str = (
        upcoming[0].strftime("%A %d %b %Y at %H:%M") if upcoming else "No upcoming classes set"
    )
    classes_remaining = student.get("classes_remaining", 0)
    renewal = student.get("renewal_date", "N/A")
    lines = [
        f"Hello, {student['name']}!",
        f"Your next class: {next_class_str}",
        f"Classes remaining: {classes_remaining}",
        f"Plan renews on: {renewal}",
    ]
    if student.get("paused"):
        lines.append("Your plan is currently paused. Contact your teacher to resume.")
    buttons = [
        [InlineKeyboardButton("📅 My Classes", callback_data="my_classes")],
        [InlineKeyboardButton("❌ Cancel Class", callback_data="cancel_class")],
    ]
    if student.get("free_class_credit", 0) > 0:
        buttons.append([InlineKeyboardButton("🎁 Free Class Credit", callback_data="free_credit")])
    return "\n".join(lines), InlineKeyboardMarkup(buttons)


async def refresh_student_menu(
    student_key: str, student: Dict[str, Any], bot
) -> None:
    """Send an updated /start summary to the student's chat."""
    chat_id = student.get("telegram_id")
    if not chat_id or bot is None:
        return
    text, markup = build_start_message(student)
    try:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)
    except Exception:
        logging.warning("Failed to refresh menu for %s", student.get("name", student_key))


async def refresh_student_my_classes(
    student_key: str, student: Dict[str, Any], bot
) -> None:
    """Send the current "My Classes" view to the student's chat."""
    chat_id = student.get("telegram_id")
    if not chat_id or bot is None:
        return
    text = build_student_classes_text(student, limit=5, student_key=student_key)
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]]
    )
    try:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
    except Exception:
        logging.warning(
            "Failed to refresh My Classes for %s", student.get("name", student_key)
        )


async def safe_edit_or_send(
    query,
    text: str,
    reply_markup=None,
    parse_mode=None,
    disable_web_page_preview: bool = True,
) -> None:
    """Safely edit a message, or send a new one if editing fails."""
    kwargs = {"text": text, "reply_markup": reply_markup}
    try:
        sig = inspect.signature(query.edit_message_text)
        if "parse_mode" in sig.parameters:
            kwargs["parse_mode"] = parse_mode
        if "disable_web_page_preview" in sig.parameters:
            kwargs["disable_web_page_preview"] = disable_web_page_preview
    except Exception:
        kwargs["parse_mode"] = parse_mode
        kwargs["disable_web_page_preview"] = disable_web_page_preview
    try:
        await query.edit_message_text(**kwargs)
    except BadRequest as e:
        logging.warning(
            "edit\u2192send fallback user=%s data=%s err=%s",
            query.from_user.id if query and query.from_user else None,
            getattr(query, "data", None),
            str(e),
        )
        if DEBUG_MODE:
            try:
                await query.answer("fallback: sent new message", show_alert=False)
            except Exception:
                pass
        await query.message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )
    except Exception:
        logging.exception("safe_edit_or_send unexpected error")
        if DEBUG_MODE:
            try:
                await query.answer("fallback: unexpected", show_alert=False)
            except Exception:
                pass
        await query.message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )


def build_debug_keyboard() -> InlineKeyboardMarkup:
    """Return a keyboard with a single ping button for diagnostics."""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔧 Ping", callback_data="__ping__")]]
    )


async def debug_ping_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Respond to ping button presses to verify callback handling."""
    await update.callback_query.answer("pong", show_alert=False)
    await safe_edit_or_send(
        update.callback_query,
        "Debug pong ✅",
        reply_markup=build_debug_keyboard() if DEBUG_MODE else None,
    )


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
    text, markup = build_start_message(student)
    await update.message.reply_text(text, reply_markup=markup)


async def student_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button presses from students (inline keyboard)."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    data = query.data
    message_id = getattr(getattr(query, "message", None), "message_id", None)
    logging.info(
        "student_button_handler entry user=%s username=%s data=%s msg_id=%s",
        user.id if user else None,
        getattr(user, "username", None),
        data,
        message_id,
    )
    if DEBUG_MODE:
        try:
            await query.answer(f"tap:{data}", show_alert=False)
        except Exception:
            pass
    student_key = None
    student = None
    try:
        students = load_students()
        student_key, student = resolve_student(students, str(user.id))
        if not student and user.username:
            student_key, student = resolve_student(students, user.username)
        if not student:
            logging.warning(
                "student_button_handler unresolved student",
                extra={
                    "user_id": user.id,
                    "username": user.username,
                    "callback_data": query.data,
                },
            )
            await safe_edit_or_send(
                query, "You are not recognised. Please contact your teacher."
            )
            return
        logging.info("student_button_handler: user=%s data=%s", user.id, data)
        if data == "my_classes":
            await show_my_classes(
                query, student_key, student, show_pending=bool(student.get("pending_cancel"))
            )
        elif data == "cancel_class":
            await initiate_cancel_class(query, student)
        elif data == "free_credit":
            await show_free_credit(query, student)
        elif data == "cancel_withdraw":
            await handle_cancel_withdraw(
                query, student_key, student, students, context
            )
        elif data == "cancel_dismiss":
            await handle_cancel_dismiss(query, student_key, student)
        elif data == "back_to_start":
            text, markup = build_start_message(student)
            await safe_edit_or_send(query, text, reply_markup=markup)
        else:
            await safe_edit_or_send(query, "Unknown action.")
    except Exception:
        logging.exception(
            "student_button_handler error user=%s data=%s",
            user.id if user else None,
            data,
        )
        await safe_edit_or_send(
            query, "Something went wrong. Refreshing your menu…"
        )
        if student:
            await refresh_student_menu(
                student_key or str(user.id),
                student,
                getattr(context, "bot", None),
            )
        else:
            try:
                await query.message.reply_text("Please send /start again.")
            except Exception:
                pass
        return


def build_student_classes_text(
    student: Dict[str, Any], *, limit: int = 5, student_key: Optional[str] = None
) -> str:
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

    if student_key:
        logs = load_logs()

        def _matches(entry_key, target_key) -> bool:
            return (
                entry_key == target_key
                or str(entry_key) == str(target_key)
                or normalize_handle(str(entry_key)) == normalize_handle(str(target_key))
            )

        student_logs = [e for e in logs if _matches(e.get("student"), student_key)]

        def _parse_entry_dt(entry: Dict[str, Any]) -> datetime:
            dt_str = entry.get("date", "")
            try:
                return datetime.fromisoformat(dt_str)
            except Exception:
                try:
                    return datetime.fromisoformat(dt_str + "T00:00")
                except Exception:
                    return datetime.min

        student_logs.sort(key=_parse_entry_dt, reverse=True)
        recent_logs = student_logs[:2]
        if recent_logs:
            lines.append("")
            lines.append("Recent classes:")
            tz = student_timezone(student)
            for entry in recent_logs:
                try:
                    status = (entry.get("status") or "").lower()
                    if status == "completed":
                        symbol = "✅"
                    elif status.startswith("missed") or status.startswith("cancelled") or status.startswith(
                        "rescheduled"
                    ):
                        symbol = "❌"
                    else:
                        symbol = "•"
                    dt_txt = entry.get("date", "")
                    try:
                        dt = datetime.fromisoformat(dt_txt)
                        if dt.tzinfo is None:
                            dt = safe_localize(tz, dt)
                        dt_txt = dt.astimezone(tz).strftime("%Y-%m-%d")
                    except Exception:
                        pass
                    note = entry.get("note") or ""
                    if note:
                        lines.append(f"{symbol} {dt_txt} – {note}")
                    else:
                        lines.append(f"{symbol} {dt_txt}")
                except Exception:
                    continue

    return "\n".join(lines)


async def show_my_classes(
    query,
    student_key: str,
    student: Dict[str, Any],
    *,
    show_pending: bool = False,
) -> None:
    """Display upcoming scheduled classes and remaining credits.

    If ``show_pending`` is True and the student has a ``pending_cancel`` entry,
    prepend a banner describing the pending cancellation and include a button to
    withdraw or dismiss the request.
    """

    pending_banner = ""
    buttons: List[List[InlineKeyboardButton]] = []
    pending = student.get("pending_cancel") if show_pending else None
    if pending:
        try:
            tz = student_timezone(student)
            class_dt = parse_student_datetime(pending.get("class_time", ""), student)
            class_str = class_dt.astimezone(tz).strftime("%a %d %b %H:%M")
        except Exception:
            class_str = pending.get("class_time", "")
        pending_banner = (
            f"⚠️ Cancellation requested for {class_str} – awaiting teacher confirmation."
        )
        buttons.append(
            [InlineKeyboardButton("Withdraw request", callback_data="cancel_withdraw")]
        )
        buttons.append([InlineKeyboardButton("Dismiss", callback_data="cancel_dismiss")])
    try:
        text = build_student_classes_text(student, limit=5, student_key=student_key)
    except Exception:
        logging.warning("show_my_classes build text failed", exc_info=True)
        text = "Your classes are loading… (temporary issue)."
    if pending_banner:
        text = f"{pending_banner}\n\n{text}"

    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")])
    keyboard = InlineKeyboardMarkup(buttons)
    await safe_edit_or_send(query, text, reply_markup=keyboard)


async def handle_cancel_withdraw(
    query,
    student_key: str,
    student: Dict[str, Any],
    students: Dict[str, Any],
    context,
) -> None:
    """Withdraw a pending cancellation request."""
    pending = student.pop("pending_cancel", None)
    save_students(students)
    if pending:
        student_name = student.get("name", student_key)
        class_time_str = pending.get("class_time", "")
        try:
            tz = student_timezone(student)
            dt = parse_student_datetime(class_time_str, student)
            class_time_str = dt.astimezone(tz).strftime("%a %d %b %H:%M")
        except Exception:
            pass
        admin_message = (
            f"ℹ️ {student_name} withdrew the cancellation request for {class_time_str}."
        )
        for admin_id in ADMIN_IDS:
            try:
                await getattr(context, "bot", None).send_message(
                    chat_id=admin_id, text=admin_message
                )
            except Exception as e:
                logging.warning(
                    "Failed to notify admin %s about withdrawal from %s: %s",
                    admin_id,
                    student_name,
                    e,
                )
    await show_my_classes(query, student_key, student)
    await refresh_student_menu(student_key, student, getattr(context, "bot", None))


async def handle_cancel_dismiss(query, student_key: str, student: Dict[str, Any]) -> None:
    """Dismiss the pending cancellation banner without clearing the request."""
    await show_my_classes(query, student_key, student, show_pending=False)


async def initiate_cancel_class(query, student: Dict[str, Any]) -> None:
    """Begin the cancellation process.  Show a list of upcoming classes."""
    upcoming_list = get_upcoming_classes(student, count=5)
    if not upcoming_list:
        await safe_edit_or_send(query, "You have no classes to cancel.")
        return
    buttons = []
    for idx, dt in enumerate(upcoming_list):
        label = dt.strftime("%a %d %b %H:%M")
        callback = f"cancel_selected:{idx}"
        buttons.append([InlineKeyboardButton(label, callback_data=callback)])
    keyboard = InlineKeyboardMarkup(buttons)
    cutoff_hours = student.get("cutoff_hours", DEFAULT_CUTOFF_HOURS)
    intro_lines = []
    if student.get("pending_cancel"):
        intro_lines.append(
            "You already have a pending cancellation. Selecting a new class will replace it."
        )
    intro_lines.append("Select a class to cancel:\n")
    intro_lines.append(
        f"Cancel more than {cutoff_hours} hours before the class to avoid a deduction."
    )
    intro = "\n".join(intro_lines)
    await safe_edit_or_send(query, intro, reply_markup=keyboard)


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
        await safe_edit_or_send(query, "You are not recognised. Please contact your teacher.")
        return
    _, index_str = query.data.split(":")
    try:
        idx = int(index_str)
    except ValueError:
        await safe_edit_or_send(query, "Invalid selection.")
        return
    upcoming = get_upcoming_classes(student, count=5)
    if idx >= len(upcoming):
        await safe_edit_or_send(query, "Invalid class selected.")
        return
    selected_dt = upcoming[idx]
    tz = student_timezone(student)
    now_tz = datetime.now(tz)
    cutoff_hours = student.get("cutoff_hours", DEFAULT_CUTOFF_HOURS)
    cutoff_dt = selected_dt - timedelta(hours=cutoff_hours)
    cancel_type = "early" if now_tz <= cutoff_dt else "late"
    existing_pending = student.get("pending_cancel")
    student["pending_cancel"] = {
        "class_time": selected_dt.isoformat(),
        "requested_at": now_tz.isoformat(),
        "type": cancel_type,
    }
    save_students(students)
    cutoff_str = cutoff_dt.astimezone(tz).strftime("%a %d %b %H:%M")
    prefix = (
        "Your previous cancellation request has been replaced with this new one. "
        if existing_pending
        else ""
    )
    if cancel_type == "early":
        message = (
            f"{prefix}Cancellation request sent to your teacher. "
            f"Cancel before {cutoff_str} ({cutoff_hours} hours in your timezone) = no deduction."
        )
    else:
        message = (
            f"{prefix}Cancellation request sent to your teacher. "
            f"You are within {cutoff_hours} hours (cutoff: {cutoff_str} your time) = one class deducted."
        )
    await safe_edit_or_send(query, message)
    await refresh_student_menu(student_key, student, getattr(context, "bot", None))

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
            await getattr(context, "bot", None).send_message(chat_id=admin_id, text=admin_message)
        except Exception as e:
            logging.warning(
                "Failed to notify admin %s about cancellation from %s: %s",
                admin_id,
                student_name,
                e,
            )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle free-form messages for both admins and students."""
    user_id = update.effective_user.id if update.effective_user else None
    if user_id in ADMIN_IDS:
        state = context.user_data.get("edit_state")
        student_key = context.user_data.get("edit_student_key")
        if state and student_key:
            students = load_students()
            student = students.get(student_key)
            if not student:
                await update.message.reply_text("Student not found.")
                context.user_data.pop("edit_state", None)
                return
            if state == "await_length":
                try:
                    duration = float(update.message.text.strip())
                    if duration < 0.5 or duration > 4.0:
                        raise ValueError
                except ValueError:
                    await update.message.reply_text(
                        "Invalid duration. Enter a number between 0.5 and 4.0:"
                    )
                    return
                duration = round(duration * 4) / 4
                student["class_duration_hours"] = duration
                save_students(students)
                logs = load_logs()
                logs.append(
                    {
                        "student": student_key,
                        "date": datetime.now(BASE_TZ).isoformat(),
                        "status": "length_changed",
                        "note": f"set to {duration}h",
                        "admin": user_id,
                    }
                )
                save_logs(logs)
                await update.message.reply_text(
                    f"Class length updated to {duration}h for {student['name']}."
                )
                await refresh_student_menu(student_key, student, getattr(context, "bot", None))
                context.user_data.pop("edit_state", None)
                return
            if state == "await_addweekly":
                slot = update.message.text.strip()
                pattern = student.get("schedule_pattern", "")
                pattern = f"{pattern}, {slot}" if pattern else slot
                student["schedule_pattern"] = pattern
                start = datetime.now(BASE_TZ).date()
                cycle_weeks = student.get("cycle_weeks", DEFAULT_CYCLE_WEEKS)
                student["class_dates"] = parse_schedule(
                    pattern, start_date=start, cycle_weeks=cycle_weeks
                )
                save_students(students)
                schedule_student_reminders(context.application, student_key, student)
                logs = load_logs()
                logs.append(
                    {
                        "student": student_key,
                        "date": datetime.now(BASE_TZ).isoformat(),
                        "status": "pattern_updated",
                        "note": f"added {slot}",
                        "admin": user_id,
                    }
                )
                save_logs(logs)
                await update.message.reply_text(
                    f"Added weekly class {slot} for {student['name']}."
                )
                await refresh_student_menu(student_key, student, getattr(context, "bot", None))
                context.user_data.pop("edit_state", None)
                return
            if state == "await_time_all":
                index = context.user_data.get("edit_slot_index")
                old_entry = context.user_data.get("edit_old_entry")
                if index is None or old_entry is None:
                    await update.message.reply_text("No slot selected.")
                    context.user_data.pop("edit_state", None)
                    return
                new_entry = update.message.text.strip()
                try:
                    edit_weekly_slot(
                        student_key,
                        student,
                        index,
                        new_entry,
                        now=datetime.now(BASE_TZ),
                        application=context.application,
                    )
                except ValueError:
                    await update.message.reply_text(
                        "Invalid day/time. Use format like 'Tuesday 19:00'."
                    )
                    return
                save_students(students)
                logs = load_logs()
                logs.append(
                    {
                        "student": student_key,
                        "date": datetime.now(BASE_TZ).isoformat(),
                        "status": "pattern_updated",
                        "note": f"slot {index} {old_entry}->{parse_day_time(new_entry)}",
                        "admin": user_id,
                    }
                )
                save_logs(logs)
                await update.message.reply_text(
                    f"Updated slot {index} from {old_entry} → {parse_day_time(new_entry)}.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back", callback_data=f"edit:pick:{student_key}")]]
                    ),
                )
                await refresh_student_menu(student_key, student, getattr(context, "bot", None))
                context.user_data.pop("edit_state", None)
                context.user_data.pop("edit_slot_index", None)
                context.user_data.pop("edit_old_entry", None)
                return
            if state == "await_time_once":
                old_dt_str = context.user_data.get("edit_once_old_dt")
                if not old_dt_str:
                    await update.message.reply_text("No class selected.")
                    context.user_data.pop("edit_state", None)
                    return
                new_input = update.message.text.strip()
                old_dt = datetime.fromisoformat(old_dt_str)
                tz = student_timezone(student)
                try:
                    if "T" in new_input:
                        new_dt = parse_student_datetime(new_input, student)
                    else:
                        norm = parse_day_time(new_input)
                        if norm is None:
                            hour, minute = map(int, new_input.split(":"))
                            target_weekday = old_dt.weekday()
                        else:
                            day_name, time_part = norm.split()
                            target_weekday = WEEKDAY_MAP[day_name.lower()]
                            hour, minute = map(int, time_part.split(":"))
                        delta = (target_weekday - old_dt.weekday()) % 7
                        new_dt = tz.normalize(old_dt + timedelta(days=delta))
                        new_dt = new_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    reschedule_single_class(
                        student_key,
                        student,
                        old_dt_str,
                        new_dt.isoformat(),
                        now=datetime.now(BASE_TZ),
                        application=context.application,
                        log=False,
                    )
                except Exception:
                    await update.message.reply_text("Invalid datetime.")
                    return
                save_students(students)
                logs = load_logs()
                logs.append(
                    {
                        "student": student_key,
                        "date": datetime.now(BASE_TZ).isoformat(),
                        "status": "rescheduled",
                        "note": f"{old_dt_str} -> {new_dt.isoformat()}",
                        "admin": user_id,
                    }
                )
                save_logs(logs)
                await update.message.reply_text(
                    f"Rescheduled class from {old_dt_str} to {new_dt.isoformat()}.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Back", callback_data=f"edit:pick:{student_key}")]]
                    ),
                )
                await refresh_student_menu(student_key, student, getattr(context, "bot", None))
                context.user_data.pop("edit_state", None)
                context.user_data.pop("edit_once_old_dt", None)
                return
            if state == "await_cancel":
                dt_input = update.message.text.strip()
                try:
                    dt = parse_student_datetime(dt_input, student)
                except ValueError:
                    await update.message.reply_text("Invalid datetime.")
                    return
                cancel_single_class(
                    student_key,
                    student,
                    dt.isoformat(),
                    grant_credit=True,
                    application=context.application,
                    log=False,
                )
                save_students(students)
                logs = load_logs()
                logs.append(
                    {
                        "student": student_key,
                        "date": datetime.now(BASE_TZ).isoformat(),
                        "status": "cancelled (admin)",
                        "note": dt.isoformat(),
                        "admin": user_id,
                    }
                )
                save_logs(logs)
                await update.message.reply_text(
                    f"Cancelled class on {dt.strftime('%d %b %H:%M')}, credit granted.",
                )
                await refresh_student_menu(student_key, student, getattr(context, "bot", None))
                context.user_data.pop("edit_state", None)
                return
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
    await safe_edit_or_send(query, msg)


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
    bot = getattr(context, "bot", None)
    students = load_students()
    changed = False
    for student in students.values():
        if await maybe_send_balance_warning(bot, student):
            changed = True
    if changed:
        save_students(students)


async def monthly_export_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """At the end of the month, send the logs JSON to the admin(s)."""
    bot = getattr(context, "bot", None)
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
    month_logs = [
        entry
        for entry in logs
        if month_start <= parse_log_date(entry["date"]) <= month_end
    ]
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
    application.add_handler(CommandHandler("liststudents", list_students_command))
    application.add_handler(CommandHandler("edit", edit_command))
    application.add_handler(CommandHandler("dashboard", dashboard_command))
    application.add_handler(CommandHandler("downloadmonth", download_month_command))
    application.add_handler(CommandHandler("selftest", selftest_command))
    application.add_handler(CommandHandler("datacheck", datacheck_command))
    application.add_handler(CommandHandler("checklogs", checklogs_command))
    application.add_handler(CommandHandler("nukepending", nukepending_command))
    application.add_handler(CommandHandler("confirmcancel", confirm_cancel_command))
    application.add_handler(CommandHandler("reschedulestudent", reschedule_student_command))
    application.add_handler(CommandHandler("removestudent", remove_student_command))
    application.add_handler(CommandHandler("viewstudent", view_student))
    application.add_handler(CommandHandler("dayview", dayview_command))

    # Student handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(dayview_callback, pattern="^dayview:"))
    application.add_handler(CallbackQueryHandler(edit_pick_callback, pattern="^edit:pick:"))
    application.add_handler(CallbackQueryHandler(edit_menu_callback, pattern="^edit:option:"))
    application.add_handler(CallbackQueryHandler(edit_delweekly_callback, pattern="^edit:delweekly:"))
    application.add_handler(CallbackQueryHandler(edit_time_slot_callback, pattern="^edit:time:slot:"))
    application.add_handler(CallbackQueryHandler(edit_time_scope_callback, pattern="^edit:time:scope:"))
    application.add_handler(CallbackQueryHandler(edit_time_oncepick_callback, pattern="^edit:time:oncepick:"))
    application.add_handler(CallbackQueryHandler(handle_cancel_selection, pattern=r"^cancel_selected:"))
    application.add_handler(CallbackQueryHandler(admin_cancel_callback, pattern="^admin_cancel_sel:"))
    application.add_handler(CallbackQueryHandler(debug_ping_callback, pattern="^__ping__$"))
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
