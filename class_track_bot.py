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
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import inspect
from datetime import datetime, timedelta, time, date, timezone
from typing import Dict, Any, List, Optional, Tuple, Union, Set

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

# New modular dispatch helpers
from admin_flows import (
    handle_student_action,
    handle_class_selection,
    handle_class_confirmation,
    handle_log_action,
    renew_received_count,
    renew_confirm,
)
from keyboard_builders import (
    build_student_submenu as kb_build_student_submenu,
    build_student_detail_view as kb_build_student_detail_view,
)

import data_store
from helpers import try_ack


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


# Simple helper to check admin privileges
def is_admin(user_id: Optional[int]) -> bool:
    return user_id in ADMIN_IDS if user_id is not None else False


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
DEFAULT_REMINDER_MINUTES = int(DEFAULT_REMINDER_OFFSET.total_seconds() // 60)

# Supported reminder choices exposed to students (minutes -> label)
REMINDER_OPTIONS: List[Tuple[int, str]] = [
    (60, "1 hour (default)"),
    (30, "30 minutes"),
    (15, "15 minutes"),
    (5, "5 minutes"),
    (0, "None"),
]

# Base timezone for all operations (Bangkok time)
BASE_TZ = pytz.timezone("Asia/Bangkok")

# Simple fixed Bangkok timezone using standard library
BKK_TZ = timezone(timedelta(hours=7))  # Asia/Bangkok, simple/fixed


def ensure_bangkok(dt_or_str):
    """
    Accepts datetime or ISO string and returns tz-aware datetime in Bangkok.
    If string, parses with datetime.fromisoformat (handles '+07:00').
    Naive datetimes are localized to Bangkok; aware datetimes are converted.
    """
    if isinstance(dt_or_str, str):
        dt = datetime.fromisoformat(dt_or_str)
    else:
        dt = dt_or_str
    if dt.tzinfo is None:
        logging.warning("Localized naive datetime to Bangkok: %s", dt_or_str)
        dt = dt.replace(tzinfo=BKK_TZ)
    else:
        dt = dt.astimezone(BKK_TZ)
    return dt


def bkk_min():
    """Return minimal aware datetime in Bangkok."""
    return datetime.min.replace(tzinfo=BKK_TZ)


def _ordinal_suffix(day: int) -> str:
    """Return the ordinal suffix for ``day`` (1 -> "st", etc.)."""

    if 10 <= day % 100 <= 20:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")


def fmt_bkk(dt, add_label: bool = False):
    """Format datetime for human display in Bangkok timezone.

    Parameters
    ----------
    dt:
        Datetime, date or parseable string.
    add_label:
        If True, append ``" ICT"`` to the formatted string.
    """

    if isinstance(dt, date) and not isinstance(dt, datetime):
        dt = datetime.combine(dt, time.min)
    dt = ensure_bangkok(dt)

    day_suffix = _ordinal_suffix(dt.day)
    text = f"{dt.strftime('%a')} {dt.day}{day_suffix}"
    if any((dt.hour, dt.minute, dt.second, dt.microsecond)):
        text += dt.strftime(" %H:%M")
    if add_label:
        text += " ICT"
    return text


REMINDER_LABEL_MAP = dict(REMINDER_OPTIONS)
REMINDER_VALID_MINUTES = set(REMINDER_LABEL_MAP)

# Student-facing messages used when a plan is paused.
PAUSED_ACTION_MESSAGE = "Plan is paused - contact your teacher."
PAUSED_SETTINGS_MESSAGE = "Plan is paused."


def normalize_reminder_minutes(value: Any) -> int:
    """Return a supported reminder offset in minutes from arbitrary input."""

    try:
        minutes = int(value)
    except (TypeError, ValueError):
        return DEFAULT_REMINDER_MINUTES
    if minutes < 0:
        return 0
    if minutes not in REMINDER_VALID_MINUTES:
        return DEFAULT_REMINDER_MINUTES
    return minutes


def get_student_reminder_minutes(student: Dict[str, Any]) -> int:
    """Return the reminder preference for ``student`` in minutes."""

    raw = student.get("reminder_offset_minutes", DEFAULT_REMINDER_MINUTES)
    return normalize_reminder_minutes(raw)


def reminder_setting_sentence(minutes: int) -> str:
    """Return a human sentence fragment describing ``minutes`` before class."""

    if minutes == 0:
        return "no reminders"
    label = REMINDER_LABEL_MAP.get(minutes)
    if label:
        base = label.replace(" (default)", "")
        return f"{base} before class"
    return f"{minutes} minutes before class"


def reminder_setting_summary(minutes: int) -> str:
    """Return text describing the reminder preference for menu displays."""

    if minutes == 0:
        return "None (reminders off)"
    label = REMINDER_LABEL_MAP.get(minutes)
    if label:
        if " (default)" in label:
            base = label.replace(" (default)", "")
            return f"{base} before class (default)"
        return f"{label} before class"
    return f"{minutes} minutes before class"


def build_notification_settings_keyboard(current_minutes: int) -> InlineKeyboardMarkup:
    """Return the inline keyboard for the notification settings view."""

    rows: List[List[InlineKeyboardButton]] = []
    for minutes, label in REMINDER_OPTIONS:
        prefix = "✅ " if minutes == current_minutes else ""
        rows.append(
            [
                InlineKeyboardButton(
                    f"{prefix}{label}",
                    callback_data=f"notification_set:{minutes}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")])
    return InlineKeyboardMarkup(rows)


def build_notification_settings_view(
    student: Dict[str, Any],
    *,
    status: Optional[str] = None,
) -> Tuple[str, InlineKeyboardMarkup]:
    """Return text and keyboard for the notification settings screen."""

    current_minutes = get_student_reminder_minutes(student)
    lines: List[str] = []
    if status:
        lines.append(status)
        lines.append("")
    lines.append("Choose when you'd like to receive reminders before class.")
    lines.append(f"Current setting: {reminder_setting_summary(current_minutes)}.")
    return "\n".join(lines), build_notification_settings_keyboard(current_minutes)


def _parse_iso(dt_str: str) -> datetime:
    """Parse an ISO timestamp string into a timezone-aware ``datetime``."""
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_last_class(student: Dict[str, Any]):
    """Return the last scheduled class as a ``datetime`` or ``None``.

    If there are no class dates, returns ``None``. Otherwise returns the
    latest parsed datetime.
    """
    class_dates = student.get("class_dates")
    if not class_dates:
        return None
    cancelled_dates = set(student.get("cancelled_dates", []))
    filtered_dates = [
        dt_str for dt_str in class_dates if dt_str and dt_str not in cancelled_dates
    ]
    if not filtered_dates:
        return None
    dates = [_parse_iso(x) for x in filtered_dates]
    return max(dates) if dates else None

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
    """Parse ``dt_str`` and return a Bangkok-aware datetime."""
    dt_str = dt_str.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(dt_str)
    except ValueError as e:
        raise ValueError(f"Invalid datetime: {dt_str}") from e
    return ensure_bangkok(dt)


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


def ensure_numeric_student_ids(students: Dict[str, Any]) -> bool:
    """Ensure all keys and ``telegram_id`` values are numeric strings."""

    changed = False
    flagged = False
    new_students: Dict[str, Any] = {}
    for key, student in list(students.items()):
        tid = student.get("telegram_id")
        sid: Optional[str] = None
        if isinstance(tid, int) or (isinstance(tid, str) and str(tid).isdigit()):
            sid = str(int(tid))
        elif str(key).isdigit():
            sid = str(int(key))
            student["telegram_id"] = int(sid)
        if sid is None:
            handle = normalize_handle(student.get("telegram_handle")) or normalize_handle(key)
            student["telegram_handle"] = handle
            student["needs_id"] = True
            new_students[handle] = student
            flagged = True
            continue
        if normalize_handle(student.get("telegram_handle")):
            student["telegram_handle"] = normalize_handle(student.get("telegram_handle"))
        new_students[sid] = student
        if sid != str(key):
            changed = True
    if flagged:
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


def is_premium(student: dict) -> bool:
    """Return True if the student has premium status."""
    return bool(student.get("premium"))


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
        raw_minutes = student.get("reminder_offset_minutes")
        normalized_minutes = normalize_reminder_minutes(
            DEFAULT_REMINDER_MINUTES if raw_minutes is None else raw_minutes
        )
        if raw_minutes != normalized_minutes:
            student["reminder_offset_minutes"] = normalized_minutes
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
    ADD_CLASSES,
    ADD_SCHEDULE,
    ADD_CUTOFF,
    ADD_DURATION,
    ADD_TELEGRAM_CHOICE,
) = range(7)

def load_students() -> Dict[str, Any]:
    """Load students from the JSON file and normalize legacy records."""
    if not os.path.exists(STUDENTS_FILE):
        return {}
    with open(STUDENTS_FILE, "r", encoding="utf-8-sig") as f:
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
    if ensure_numeric_student_ids(students):
        changed = True
    if changed and students:
        # One-time migration: persist upgraded student records (non-empty only)
        save_students(students)
    return students


def save_students(students: Dict[str, Any]) -> None:
    """Persist students dict to JSON enforcing numeric keys."""

    if not students:
        logging.warning("Refusing to overwrite students.json with empty data")
        return
    ensure_numeric_student_ids(students)
    tmp_path = f"{STUDENTS_FILE}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(students, f, indent=2, ensure_ascii=False, sort_keys=True)
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                # Best-effort: fsync may not be available on some platforms
                pass
        os.replace(tmp_path, STUDENTS_FILE)
    except Exception as e:
        logging.error(
            "Failed to save students.json atomically; original file left unchanged: %s",
            e,
        )
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def normalize_log_students(
    logs: List[Dict[str, Any]], students: Dict[str, Any]
) -> bool:
    """Normalise ``student`` fields in log entries to canonical keys.

    Returns True if any log entries were modified.
    """

    changed = False
    for entry in list(logs):
        student_field = entry.get("student")
        if student_field is None:
            continue
        normalized = normalize_handle(str(student_field))
        canonical, _ = resolve_student(students, normalized)
        if canonical is None:
            if normalized.isdigit():
                new_key = normalized
            else:
                logs.remove(entry)
                changed = True
                continue
        else:
            new_key = canonical
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


def get_admin_future_classes(
    student: Dict[str, Any], include_cancelled: bool = False
) -> List[str]:
    """Return ``class_dates`` for admin views with optional cancelled filtering."""

    class_dates = student.get("class_dates", [])
    if not isinstance(class_dates, list):
        return []

    cancelled: Set[str] = set()
    if not include_cancelled:
        raw_cancelled = student.get("cancelled_dates", [])
        if isinstance(raw_cancelled, list):
            cancelled = {str(item) for item in raw_cancelled if item}

    filtered: List[str] = []
    for item in class_dates:
        if not item:
            continue
        if isinstance(item, str):
            dt_str = item
        elif hasattr(item, "isoformat"):
            dt_str = item.isoformat()
        else:
            dt_str = str(item)
        if not include_cancelled and dt_str in cancelled:
            continue
        filtered.append(dt_str)

    return sorted(filtered)


def get_student_visible_classes(student: Dict[str, Any], count: int = 5) -> List[datetime]:
    """Return upcoming classes that a *student* should see.

    ``student['class_dates']`` stores concrete class datetimes as ISO 8601
    strings in the base timezone.  This helper converts them to timezone aware
    ``datetime`` objects, removes past occurrences and any dates present in
    ``cancelled_dates`` and returns the next ``count`` items.

    Non-premium students will never see more entries than indicated by their
    ``classes_remaining`` value, ensuring the UI stays aligned with purchased
    credit.
    """
    now = ensure_bangkok(datetime.now())
    cancelled = set(student.get("cancelled_dates", []))
    results: List[datetime] = []
    for item in student.get("class_dates", []):
        try:
            dt = ensure_bangkok(item)
        except Exception:
            try:
                dt = ensure_bangkok(datetime.strptime(item, "%Y-%m-%d %H:%M"))
            except Exception:
                continue
        if dt <= now:
            continue
        if item in cancelled or dt.isoformat() in cancelled:
            continue
        results.append(dt)
    results.sort()

    try:
        requested_count = int(count)
    except (TypeError, ValueError):
        requested_count = 0
    if requested_count < 0:
        requested_count = 0

    if requested_count == 0:
        return []

    if is_premium(student):
        visible_cap = requested_count
    else:
        remaining_raw = student.get("classes_remaining")
        try:
            remaining = int(remaining_raw)
        except (TypeError, ValueError):
            remaining = 0
        if remaining < 0:
            remaining = 0
        visible_cap = min(requested_count, remaining)

    return results[:visible_cap]


def get_student_cancellable_classes(student: Dict[str, Any]) -> List[datetime]:
    """Return all upcoming classes that a student can cancel."""

    now = ensure_bangkok(datetime.now())
    cancelled = set(student.get("cancelled_dates", []))
    results: List[datetime] = []
    for item in student.get("class_dates", []):
        try:
            dt = ensure_bangkok(item)
        except Exception:
            try:
                dt = ensure_bangkok(datetime.strptime(item, "%Y-%m-%d %H:%M"))
            except Exception:
                continue
        if dt <= now:
            continue
        if item in cancelled or dt.isoformat() in cancelled:
            continue
        results.append(dt)
    results.sort()
    if is_premium(student):
        return results

    remaining_raw = student.get("classes_remaining")
    try:
        remaining = int(remaining_raw)
    except (TypeError, ValueError):
        remaining = 0
    if remaining < 0:
        remaining = 0

    return results[:remaining]


def get_admin_visible_classes(
    student_id: str, student: Dict[str, Any], count: int = 5
) -> List[datetime]:
    """Return past class datetimes that still need logging.

    This powers the **Log Class** admin menu and must never include classes that
    have already been logged as completed, cancelled, rescheduled or removed.
    Future classes are ignored here; admins see only past, unlogged entries.
    """

    logs = load_logs()
    now = ensure_bangkok(datetime.now())
    results: List[datetime] = []
    for item in student.get("class_dates", []):
        try:
            dt = ensure_bangkok(item)
        except Exception:
            try:
                dt = ensure_bangkok(datetime.strptime(item, "%Y-%m-%d %H:%M"))
            except Exception:
                continue
        if dt > now:
            continue
        if data_store.is_class_logged(student_id, dt.isoformat(), logs):
            continue
        results.append(dt)
    results.sort()
    return results[:count]


def get_admin_upcoming_classes(
    student_id: str, student: Dict[str, Any], count: int = 5
) -> List[datetime]:
    """Return upcoming class datetimes not yet logged by admins."""

    logs = load_logs()
    now = ensure_bangkok(datetime.now())
    results: List[datetime] = []
    for item in student.get("class_dates", []):
        try:
            dt = ensure_bangkok(item)
        except Exception:
            try:
                dt = ensure_bangkok(datetime.strptime(item, "%Y-%m-%d %H:%M"))
            except Exception:
                continue
        if dt <= now:
            continue
        if data_store.is_class_logged(student_id, dt.isoformat(), logs):
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
    if (
        not student
        or student.get("paused")
        or not student.get("telegram_mode", True)
    ):
        return
    chat_id = (
        student.get("telegram_id") if student.get("telegram_mode", True) else None
    )
    if not chat_id:
        return
    try:
        class_dt = ensure_bangkok(class_dt_str)
    except Exception:
        return
    msg = f"Reminder: you have a class at {fmt_bkk(class_dt)}"
    try:
        await getattr(context, "bot", None).send_message(chat_id=chat_id, text=msg)
    except Exception:
        logging.warning("Failed to send class reminder to %s", student.get("name"))


def resolve_reminder_offset(
    student: Dict[str, Any], reminder_offset: Optional[Union[int, timedelta]]
) -> Optional[timedelta]:
    """Return a :class:`timedelta` for reminder scheduling or ``None`` to skip."""

    if isinstance(reminder_offset, timedelta):
        if reminder_offset.total_seconds() <= 0:
            return None
        return reminder_offset
    if reminder_offset is None:
        minutes = get_student_reminder_minutes(student)
    else:
        minutes = normalize_reminder_minutes(reminder_offset)
    if minutes <= 0:
        return None
    return timedelta(minutes=minutes)


def schedule_class_reminder(
    application: Application,
    student_key: str,
    student: Dict[str, Any],
    class_dt_str: str,
    reminder_offset: Optional[Union[int, timedelta]] = None,
) -> None:
    reminder_delta = resolve_reminder_offset(student, reminder_offset)
    if reminder_delta is None:
        return
    now = ensure_bangkok(datetime.now())
    try:
        class_dt = ensure_bangkok(class_dt_str)
    except Exception:
        return
    run_time = class_dt - reminder_delta
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
    reminder_offset: Optional[Union[int, timedelta]] = None,
) -> None:
    """Schedule reminder jobs for all future classes of a student."""
    if not student.get("telegram_mode", True):
        return
    if not student.get("telegram_id"):
        return
    reminder_delta = resolve_reminder_offset(student, reminder_offset)
    prefix = f"class_reminder:{student_key}:"
    # remove existing reminder jobs for this student
    for job in application.job_queue.jobs():
        if job.name and job.name.startswith(prefix):
            job.schedule_removal()
    if reminder_delta is None:
        return
    for item in student.get("class_dates", []):
        if item in student.get("cancelled_dates", []):
            continue
        schedule_class_reminder(
            application, student_key, student, item, reminder_delta
        )


async def send_low_balance_if_threshold(app: Application, student_key: str, student: Dict[str, Any]):
    """Warn when a student reaches the low balance threshold."""
    remaining = student.get("classes_remaining")
    if remaining != 2:
        return
    chat_id = student.get("telegram_id")
    msg = "You have 2 classes remaining in your current set."
    if chat_id:
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg)
        except Exception:
            logging.warning("Failed to send low balance notice to %s", student.get("name"))
    notice = f"{student.get('name')} has 2 classes remaining."
    for admin_id in ADMIN_IDS:
        try:
            await app.bot.send_message(chat_id=admin_id, text=notice)
        except Exception:
            continue


def schedule_final_set_notice(app: Application, student_key: str, student: Dict[str, Any], offset: timedelta = timedelta(hours=1)) -> None:
    """Schedule a notice before the final class in the current set."""
    if is_premium(student):
        return
    if not student.get("telegram_mode", True):
        return
    last_class = get_last_class(student)
    if not last_class:
        return
    run_time = last_class - offset
    job_name = f"final_notice:{student_key}"
    for job in app.job_queue.jobs():
        if job.name == job_name:
            job.schedule_removal()
    app.job_queue.run_once(send_final_set_notice, when=run_time, name=job_name, data={"student_key": student_key})


async def send_final_set_notice(context: ContextTypes.DEFAULT_TYPE):
    student_key = context.job.data.get("student_key")
    student = data_store.get_student_by_id(student_key)
    if not student or not student.get("telegram_mode", True):
        return
    chat_id = student.get("telegram_id")
    if not chat_id:
        return
    last_class = get_last_class(student)
    if not last_class:
        return
    msg = f"Final class of your current set is at {fmt_bkk(last_class)} today. Good luck!"
    try:
        await context.bot.send_message(chat_id=chat_id, text=msg)
    except Exception:
        logging.warning("Failed to send final set notice to %s", student.get("name"))


def ensure_future_class_dates(student: Dict[str, Any], horizon_weeks: Optional[int] = None) -> bool:
    """Ensure class_dates extend at least ``horizon_weeks`` into the future."""
    if horizon_weeks is None:
        horizon_weeks = student.get("cycle_weeks", DEFAULT_CYCLE_WEEKS)
    now = ensure_bangkok(datetime.now())
    class_dates = student.get("class_dates", [])
    original_len = len(class_dates)

    parsed: List[datetime] = []
    for item in class_dates:
        try:
            dt = ensure_bangkok(item)
        except Exception:
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
                dt = ensure_bangkok(dt_str)
                if latest and dt <= latest:
                    continue
                parsed.append(dt)
                added = True
    parsed.sort()
    student["class_dates"] = [dt.isoformat() for dt in parsed]
    return added or len(student["class_dates"]) != original_len


def regenerate_future_class_dates(student: Dict[str, Any], *, now: Optional[datetime] = None) -> None:
    """Regenerate future ``class_dates`` based on ``schedule_pattern``.

    Past class dates are preserved. Future dates are generated from the
    current ``schedule_pattern`` from ``now`` forward. ``class_dates`` remain
    sorted and de-duplicated and cancelled dates are skipped.
    """
    if now is None:
        now = ensure_bangkok(datetime.now())
    pattern = student.get("schedule_pattern", "")
    entries = [e.strip() for e in pattern.split(",") if e.strip()]
    past: List[datetime] = []
    for item in student.get("class_dates", []):
        try:
            dt = ensure_bangkok(item)
        except Exception:
            continue
        if dt <= now:
            past.append(dt)
    horizon_weeks = student.get("cycle_weeks", DEFAULT_CYCLE_WEEKS)
    future: List[datetime] = []
    if entries:
        gen = parse_schedule(
            ", ".join(entries), start_date=now.date(), cycle_weeks=horizon_weeks
        )
        cancelled = set(student.get("cancelled_dates", []))
        for dt_str in gen:
            try:
                dt = ensure_bangkok(dt_str)
            except Exception:
                continue
            if dt <= now:
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
    q = update.callback_query
    if q:
        await try_ack(q)
    message = update.effective_message
    await message.reply_text(
        "Adding a new student. Please enter the student's name:",
        reply_markup=ReplyKeyboardRemove(),
    )
    context.user_data.clear()
    return ADD_NAME


async def add_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = (update.message.text or "").strip()
    if not name:
        await update.message.reply_text(
            "Name can't be empty. Please enter the student's name:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ADD_NAME
    context.user_data["name"] = name
    context.user_data.pop("telegram_mode", None)
    context.user_data.pop("student_key", None)
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📱 Has Telegram", callback_data="student_has_telegram"
                )
            ],
            [
                InlineKeyboardButton(
                    "📵 No Telegram Yet", callback_data="student_no_telegram"
                )
            ],
            [
                InlineKeyboardButton("❌ Cancel", callback_data="student_cancel")
            ],
        ]
    )
    await update.message.reply_text(
        "Does this student use Telegram?",
        reply_markup=keyboard,
    )
    return ADD_TELEGRAM_CHOICE


async def add_telegram_choice(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ADD_TELEGRAM_CHOICE
    await try_ack(query)
    choice = query.data or ""
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    if choice == "student_has_telegram":
        context.user_data["telegram_mode"] = True
        context.user_data.pop("student_key", None)
        context.user_data.setdefault("telegram_id", None)
        context.user_data.setdefault("telegram_handle", None)
        await query.message.reply_text(
            "Enter the student's Telegram @handle or numeric ID:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ADD_HANDLE
    if choice == "student_no_telegram":
        context.user_data["telegram_mode"] = False
        context.user_data["telegram_id"] = None
        context.user_data["telegram_handle"] = None
        context.user_data.pop("needs_id", None)
        name = context.user_data.get("name", "")
        student_key = name.lower().replace(" ", "_")
        context.user_data["student_key"] = student_key
        await query.message.reply_text(
            "Enter number of classes in the plan (e.g., 8):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ADD_CLASSES
    if choice == "student_cancel":
        context.user_data.clear()
        await query.message.reply_text(
            "Operation cancelled.", reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    await query.message.reply_text(
        "Please choose one of the provided options.",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📱 Has Telegram", callback_data="student_has_telegram"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "📵 No Telegram Yet", callback_data="student_no_telegram"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "❌ Cancel", callback_data="student_cancel"
                    )
                ],
            ]
        ),
    )
    return ADD_TELEGRAM_CHOICE


async def add_handle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("telegram_mode") is False:
        await update.message.reply_text(
            "Please choose whether the student has Telegram using the buttons provided.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ADD_TELEGRAM_CHOICE
    handle = update.message.text.strip()
    if handle.startswith("@"):  # strip leading @
        handle = handle[1:]
    if handle.isdigit():
        context.user_data["telegram_id"] = int(handle)
        context.user_data["telegram_handle"] = None
        context.user_data.pop("needs_id", None)
    else:
        normalized = normalize_handle(handle)
        context.user_data["telegram_handle"] = normalized
        try:
            chat = await context.application.bot.get_chat(f"@{normalized}")
            context.user_data["telegram_id"] = int(chat.id)
            context.user_data["telegram_handle"] = chat.username or normalized
            context.user_data.pop("needs_id", None)
        except Exception:
            context.user_data["telegram_id"] = None
            context.user_data["needs_id"] = True
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
    schedule_pattern = context.user_data.get("schedule_pattern", "")
    classes_remaining = context.user_data.get("classes_remaining")
    cycle_weeks = DEFAULT_CYCLE_WEEKS
    weekly_slots = 0

    if isinstance(schedule_pattern, str) and schedule_pattern:
        entries = [item.strip() for item in schedule_pattern.split(",") if item.strip()]
        for entry in entries:
            if parse_day_time(entry):
                weekly_slots += 1

    if (
        isinstance(classes_remaining, int)
        and classes_remaining > 0
        and weekly_slots > 0
    ):
        cycle_weeks = (classes_remaining + weekly_slots - 1) // weekly_slots
        if cycle_weeks < DEFAULT_CYCLE_WEEKS:
            cycle_weeks = DEFAULT_CYCLE_WEEKS

    context.user_data["cycle_weeks"] = cycle_weeks
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

    # Build student record
    students = load_students()
    telegram_mode = context.user_data.get("telegram_mode")
    if telegram_mode is None:
        telegram_mode = True
    if telegram_mode:
        telegram_id = context.user_data.get("telegram_id")
        handle = normalize_handle(context.user_data.get("telegram_handle"))
        key = str(int(telegram_id)) if telegram_id else handle
    else:
        telegram_id = None
        handle = None
        key = context.user_data.get("student_key")
        if not key:
            name = context.user_data.get("name", "")
            key = name.lower().replace(" ", "_")
            context.user_data["student_key"] = key
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
        "telegram_id": int(telegram_id) if telegram_id else None,
        "telegram_handle": handle,
        "classes_remaining": context.user_data.get("classes_remaining"),
        "class_dates": class_dates,
        "schedule_pattern": schedule_pattern,
        "cutoff_hours": context.user_data.get("cutoff_hours"),
        "cycle_weeks": cycle_weeks,
        "class_duration_hours": context.user_data.get("class_duration_hours"),
        "reminder_offset_minutes": DEFAULT_REMINDER_MINUTES,
        "paused": False,
        "free_class_credit": 0,
        "reschedule_credit": 0,
        "notes": [],
        "telegram_mode": bool(telegram_mode),
    }
    if telegram_mode and not telegram_id:
        student["needs_id"] = True
    ensure_future_class_dates(student)
    students[key] = student
    save_students(students)
    if telegram_mode and telegram_id:
        schedule_student_reminders(context.application, key, student)
        await update.message.reply_text(
            f"Added student {context.user_data.get('name')} successfully!"
        )
    elif telegram_mode:
        await update.message.reply_text(
            "Student added with handle only. Reminders will start once they /start the bot or you run /resolveids."
        )
    else:
        await update.message.reply_text(
            "Student added without Telegram. You can connect them later from their profile."
        )
    return ConversationHandler.END


# Minimal legacy compatibility: add_renewal handler
async def add_renewal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Deprecated legacy handler retained for test compatibility.

    This no-op handler logs a deprecation warning and ends any conversation.
    It does not attempt to write a renewal_date or mutate schedules here;
    the new renewal flow lives in admin_flows.py.
    """
    logging.warning("add_renewal is deprecated; writing minimal record for tests.")
    try:
        students = data_store.load_students()
        telegram_mode = context.user_data.get("telegram_mode")
        if telegram_mode is None:
            telegram_mode = True
        if telegram_mode:
            key = str(
                context.user_data.get("telegram_id")
                or context.user_data.get("telegram_handle")
            )
        else:
            name = context.user_data.get("name", "")
            key = context.user_data.get("student_key") or name.lower().replace(" ", "_")
            context.user_data["student_key"] = key
        student = {
            "name": context.user_data.get("name"),
            "telegram_id": context.user_data.get("telegram_id") if telegram_mode else None,
            "telegram_handle": context.user_data.get("telegram_handle") if telegram_mode else None,
            "class_dates": context.user_data.get("class_dates", []),
            "classes_remaining": context.user_data.get("classes_remaining", 0),
            "cancelled_dates": [],
            "reminder_offset_minutes": DEFAULT_REMINDER_MINUTES,
            "telegram_mode": bool(telegram_mode),
        }
        if key:
            students[str(key)] = student
            data_store.save_students(students)
            # Trigger reminder scheduling when we have a numeric id
            if (
                telegram_mode
                and context.user_data.get("telegram_id")
                and hasattr(context, "application")
            ):
                try:
                    schedule_student_reminders(context.application, key, student)
                except Exception:
                    pass
    except Exception:
        logging.exception("add_renewal minimal save failed")
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
    # Admins see upcoming scheduled classes minus already logged ones
    upcoming_list = get_admin_upcoming_classes(student_key, student, count=8)
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
    await try_ack(query)
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
    # Use admin visibility to ensure cancelled or completed classes are excluded
    upcoming_list = get_admin_upcoming_classes(student_key, student, count=8)
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
    if is_premium(student):
        await update.message.reply_text(
            f"Note: {student['name']} is Premium; award/renew not required."
        )
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
    """Legacy command; use admin flow for renewals."""
    await update.message.reply_text(
        "This command is deprecated; use the in-bot renew flow instead."
    )


@admin_only
async def set_premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Toggle a student's premium status.

    Usage: /setpremium <student_ref> <on|off>
    """

    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Usage: /setpremium <student_ref> <on|off>")
        return

    student_ref, flag = args
    students = load_students()
    student_key, student = resolve_student(students, student_ref)
    if not student:
        await update.message.reply_text(f"Student '{student_ref}' not found.")
        return

    flag_lower = flag.lower()
    if flag_lower in {"on", "true", "1", "enable", "enabled"}:
        value = True
    elif flag_lower in {"off", "false", "0", "disable", "disabled"}:
        value = False
    else:
        await update.message.reply_text("Flag must be 'on' or 'off'.")
        return

    student["premium"] = value
    save_students(students)
    if value:
        await update.message.reply_text(
            f"🌟 Premium ENABLED for {student['name']} — unlimited hours (∞)"
        )
    else:
        await update.message.reply_text(f"Premium DISABLED for {student['name']}")


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
    try:
        now = datetime.now(BASE_TZ)  # type: ignore[arg-type]
    except TypeError:
        now = datetime.now(BKK_TZ)
    today = now.date()
    month_start = date(now.year, now.month, 1)
    premium_count = sum(1 for s in students.values() if is_premium(s))

    active_students = [s for s in students.values() if not s.get("paused")]
    total_hours = 0.0
    for s in active_students:
        entries = [e for e in s.get("schedule_pattern", "").split(",") if e.strip()]
        duration = s.get("class_duration_hours", DEFAULT_DURATION_HOURS)
        total_hours += len(entries) * duration

    today_class_entries: List[Tuple[datetime, str]] = []
    low_balance: List[str] = []
    upcoming_renewals: List[str] = []
    overdue_renewals: List[str] = []
    paused_students: List[str] = []
    free_credits: List[str] = []
    completed = missed = cancelled = rescheduled = 0
    skipped_logs = 0

    for entry in logs:
        date_value = entry.get("date") or entry.get("at")
        if not date_value:
            print(f"⚠️ Skipping malformed log entry (no date/at): {entry}")
            skipped_logs += 1
            continue
        entry_date = parse_log_date(str(date_value))
        if entry_date < month_start:
            continue
        raw_status = (entry.get("status") or entry.get("type") or "")
        status = str(raw_status).lower()
        if status.startswith("class_"):
            status = status[6:]
        if status == "completed":
            completed += 1
        elif status.startswith("missed"):
            missed += 1
        elif "cancelled" in status:
            cancelled += 1
        elif "rescheduled" in status:
            rescheduled += 1

    for sid, student in students.items():
        tz = student_timezone(student)
        today_student = today

        if student.get("paused"):
            paused_students.append(student["name"])
        else:
            cancelled_dates = set(student.get("cancelled_dates", []))
            for dt_str in student.get("class_dates", []):
                if not dt_str:
                    continue
                if dt_str in cancelled_dates:
                    continue
                try:
                    class_dt = parse_student_datetime(dt_str, student)
                except ValueError:
                    logging.warning(
                        "Skipping invalid class date for %s: %s",
                        student.get("name", sid),
                        dt_str,
                    )
                    continue
                class_dt_local = class_dt.astimezone(tz)
                if class_dt_local.date() == today_student:
                    today_class_entries.append(
                        (class_dt_local, f"{student['name']} at {class_dt_local.strftime('%H:%M')}")
                    )

        if not is_premium(student):
            remaining = student.get("classes_remaining", 0)
            if remaining <= 2:
                low_balance.append(student["name"])

            last_class = get_last_class(student)
            if last_class:
                last_date = last_class.date()
                if last_date < today:
                    overdue_renewals.append(
                        f"{student['name']} ({fmt_bkk(last_date)})"
                    )
                elif today <= last_date <= today + timedelta(days=7):
                    upcoming_renewals.append(
                        f"{student['name']} ({fmt_bkk(last_date)})"
                    )

        if student.get("free_class_credit", 0) > 0:
            free_credits.append(
                f"{student['name']} ({student['free_class_credit']})"
            )

    today_classes = [
        label for _, label in sorted(today_class_entries, key=lambda item: item[0])
    ]

    lines: List[str] = ["📊 Dashboard Summary", ""]
    lines.append(
        f"Active students: {len(active_students)} ({premium_count} premium)"
    )
    lines.append(f"Total scheduled hours/week: {total_hours:.1f}")
    lines.append("")
    lines.append(f"Unlogged classes (today) ({fmt_bkk(today)}):")
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

    lines.append("Set ends in next 7 days:")
    if upcoming_renewals:
        lines.extend(f"- {item}" for item in upcoming_renewals)
    else:
        lines.append("- None")
    lines.append("")

    lines.append("Overdue set ends:")
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

    if skipped_logs:
        lines.append("")
        lines.append(
            f"Note: {skipped_logs} logs were ignored due to missing date/at."
        )

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
    text = "Select a day:"
    markup = InlineKeyboardMarkup(buttons)
    if update.message:
        await update.message.reply_text(text, reply_markup=markup)
    else:
        await safe_edit_or_send(update.callback_query, text, reply_markup=markup)


@admin_only
async def dayview_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle callback for a specific day's class view."""
    query = update.callback_query
    await try_ack(query)
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
        for item in student.get("class_dates", []):
            try:
                dt = parse_student_datetime(item, student).astimezone(BASE_TZ)
            except Exception:
                continue
            if dt.date() != target_date:
                continue
            if item in cancelled:
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
    buttons = [
        [InlineKeyboardButton("🕒 Pending Actions", callback_data="admin_pending")]
    ]
    markup = InlineKeyboardMarkup(buttons)
    if update.message:
        await update.message.reply_text(summary, reply_markup=markup)
    else:
        await safe_edit_or_send(update.callback_query, summary, reply_markup=markup)


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id if update.effective_user else None):
        return
    text = "🔧 Admin Menu"
    kb = build_admin_menu_kb()
    if update.message:
        await update.message.reply_text(text, reply_markup=kb)
    else:
        await safe_edit_or_send(update.callback_query, text, reply_markup=kb)


async def render_admin_pending(target, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Render the list of pending cancellation actions for admins."""

    students = load_students()
    pending_items: List[Tuple[str, Dict[str, Any]]] = [
        (sid, s) for sid, s in students.items() if s.get("pending_cancel")
    ]

    lines: List[str] = [f"Pending cancels: {len(pending_items)}"]
    buttons: List[List[InlineKeyboardButton]] = []

    for sid, student in pending_items:
        pending = student.get("pending_cancel")
        class_time = pending.get("class_time") if pending else None
        try:
            dt = ensure_bangkok(class_time)
            class_str = fmt_bkk(dt, add_label=False)
        except Exception:
            logging.warning(
                "Bad pending_cancel for %s: %s", sid, student.get("pending_cancel")
            )
            continue
        handle = student.get("telegram_handle")
        name = student.get("name", sid)
        display = name
        if handle:
            display += f" (@{normalize_handle(handle)})"
        cancel_type = pending.get("type") if pending else None
        type_suffix = f" {cancel_type}" if cancel_type else ""
        lines.append(f"• {display} — {class_str}{type_suffix}")

        buttons.append(
            [
                InlineKeyboardButton(
                    f"✅ Confirm — {name}", callback_data=f"confirm_pending:{sid}"
                )
            ]
        )

    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")])
    text = "\n".join(lines)
    await safe_edit_or_send(target, text, reply_markup=InlineKeyboardMarkup(buttons))


@admin_only
async def pending_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Entry point for /pending command."""

    await render_admin_pending(update.message, context)


@admin_only
async def admin_pending_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle callback for pending actions list."""

    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass
    await render_admin_pending(query, context)


@admin_only
async def confirm_pending_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """One-tap confirmation of pending cancellations."""

    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    try:
        await query.answer()
    except Exception:
        pass
    async def answer_alert(message: str) -> None:
        try:
            await query.answer(message, show_alert=True)
        except TypeError:
            try:
                await query.answer(message)
            except TypeError:
                await query.answer()
    try:
        _, raw_student = data.split(":", 1)
    except ValueError:
        logging.warning("Malformed confirm pending callback: %s", data)
        await answer_alert("Invalid request")
        return
    students = load_students()
    student_id, student = resolve_student(students, raw_student)
    if not student:
        logging.warning("Unable to resolve student %s for pending confirmation", raw_student)
        await answer_alert("Student not found")
        await admin_pending_callback(update, context)
        return
    student_id = str(student_id)
    student = students.get(student_id, student)
    if not student.get("pending_cancel"):
        await answer_alert("Nothing pending for this student.")
        await admin_pending_callback(update, context)
        return

    try:
        await confirm_cancel_for_student(context, students, student_id, student)
    except ValueError as exc:
        await answer_alert(str(exc))
        await admin_pending_callback(update, context)
        return

    await query.answer("Cancel confirmed.")
    await admin_pending_callback(update, context)


async def admin_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(q.from_user.id if q and q.from_user else None):
        await try_ack(q, text="Admins only.", show_alert=True)
        return
    action = q.data.split(":", 1)[1]

    try:
        if action == "dayview":
            return await dayview_command(update, context)
        elif action == "dashboard":
            return await dashboard_command(update, context)
        elif action == "students":
            students = load_students()
            kb = build_students_page_kb(students, page=0)
            return await safe_edit_or_send(q, "👥 Students", reply_markup=kb)
        elif action == "logs":
            await try_ack(q)
            return await safe_edit_or_send(
                q,
                "📂 Logs / Exports\nUse /downloadmonth for now. Menu coming soon.",
            )
        elif action == "settings":
            await try_ack(q)
            return await safe_edit_or_send(
                q,
                "⚙️ Settings\nQuick toggles coming soon.",
            )
        elif action == "root":
            return await admin_command(update, context)
        else:
            await try_ack(q, text="Unknown admin action.", show_alert=False)
            return
    except Exception:
        logging.exception("ADMIN MENU ACTION CRASH action=%s", action)
        await safe_edit_or_send(q, "Temporary issue opening that admin view.")


async def admin_students_page_callback(update, context):
    q = update.callback_query
    if not is_admin(q.from_user.id):
        return await q.answer("Admins only.", show_alert=True)
    students = load_students()
    page = 0
    data = q.data
    if ":page:" in data:
        try:
            page = int(data.rsplit(":", 1)[-1])
        except Exception:
            page = 0
    kb = build_students_page_kb(students, page=page)
    await safe_edit_or_send(q, "👥 Students", reply_markup=kb)


async def admin_pick_student_callback(update, context):
    q = update.callback_query
    if not is_admin(q.from_user.id):
        return await q.answer("Admins only.", show_alert=True)
    data = q.data or ""
    try:
        _, _, student_key = data.split(":", 2)
    except ValueError:
        await q.answer("Student not found.", show_alert=True)
        return await admin_students_page_callback(update, context)
    students = load_students()
    student_id, student = resolve_student(students, student_key)
    if not student or not student_id:
        await q.answer("Student not found.", show_alert=True)
        return await admin_students_page_callback(update, context)
    context.user_data["admin_selected_student_id"] = student_id
    nameline = display_name(student_id, student)
    text = f"👤 {nameline}\nChoose an action:"
    kb = build_student_submenu_kb(student_id)
    await safe_edit_or_send(q, text, reply_markup=kb)


async def _send_admin_student_detail(
    context: ContextTypes.DEFAULT_TYPE,
    connect_state: Dict[str, Any],
    student_key: str,
    student: Dict[str, Any],
    fallback_message=None,
) -> None:
    """Update the admin detail message or send a new one as a fallback."""

    text, markup = build_student_detail_view(student_key, student)
    bot = getattr(context, "bot", None) or getattr(
        getattr(context, "application", None), "bot", None
    )
    chat_id = connect_state.get("prompt_chat_id")
    message_id = connect_state.get("prompt_message_id")
    if (
        bot
        and hasattr(bot, "edit_message_text")
        and chat_id is not None
        and message_id is not None
    ):
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=markup,
            )
            return
        except Exception:
            logging.debug("Failed to edit admin detail message; sending fallback.")
    if fallback_message is not None and hasattr(fallback_message, "reply_text"):
        await fallback_message.reply_text(text, reply_markup=markup)


async def connect_student_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Prompt the admin to supply Telegram details for an offline student."""

    query = update.callback_query
    if not query:
        return
    user_id = getattr(getattr(query, "from_user", None), "id", None)
    if not is_admin(user_id):
        await try_ack(query, text="Admins only.", show_alert=True)
        return
    await try_ack(query)
    data = query.data or ""
    parts = data.split(":", 2)
    if len(parts) < 3:
        await safe_edit_or_send(query, "Student not found.")
        return
    raw_student_id = parts[2]
    students = load_students()
    student_key, student = resolve_student(students, raw_student_id)
    if not student or not student_key:
        await safe_edit_or_send(query, "Student not found.")
        return
    if student.get("telegram_mode", True):
        await try_ack(query, text="Already connected.", show_alert=False)
        text, markup = build_student_detail_view(student_key, student)
        await safe_edit_or_send(query, text, reply_markup=markup)
        return
    message = getattr(query, "message", None)
    chat = getattr(message, "chat", None)
    context.user_data["connect_student"] = {
        "student_key": student_key,
        "prompt_chat_id": getattr(chat, "id", None),
        "prompt_message_id": getattr(message, "message_id", None),
    }
    prompt = (
        f"Please send the student's Telegram @handle or numeric ID for "
        f"{student.get('name', student_key)}.\n"
        "Type 'cancel' to abort."
    )
    await safe_edit_or_send(query, prompt)


async def admin_view_for_student(student_id: str, query, context):
    students = load_students()
    student = students.get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    text, markup = build_student_detail_view(student_id, student)
    return await safe_edit_or_send(query, text, reply_markup=markup)


async def admin_logclass_for_student(student_id: str, query, context):
    student = load_students().get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    return await initiate_log_class(query, context, student_id, student)


async def admin_cancel_for_student(student_id: str, query, context):
    student = load_students().get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    return await initiate_cancel_class_admin(query, context, student_id, student)


async def admin_renew_for_student(student_id: str, query, context):
    student = load_students().get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    return await initiate_renew_student(query, context, student_id, student)


async def admin_free_for_student(student_id: str, query, context):
    student = load_students().get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    return await initiate_award_free(query, context, student_id, student)


async def admin_resched_for_student(student_id: str, query, context):
    student = load_students().get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    return await initiate_reschedule_student(query, context, student_id, student)


async def admin_length_for_student(student_id: str, query, context):
    student = load_students().get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    return await initiate_change_length(query, context, student_id, student)


async def admin_schedule_for_student(student_id: str, query, context):
    student = load_students().get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    return await initiate_edit_schedule(query, context, student_id, student)


async def admin_pause_for_student(student_id: str, query, context):
    student = load_students().get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    return await initiate_pause_toggle(query, context, student_id, student)


async def admin_remove_for_student(student_id: str, query, context):
    student = load_students().get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")
    return await initiate_remove_student(query, context, student_id, student)


async def admin_student_action_callback(update, context):
    q = update.callback_query
    if not is_admin(q.from_user.id):
        return await q.answer("Admins only.", show_alert=True)
    data_parts = q.data.split(":", 3)
    student_id = data_parts[2] if len(data_parts) > 2 else ""
    action = q.data.rsplit(":", 1)[-1]
    students = load_students()
    if not student_id or student_id not in students:
        await safe_edit_or_send(q, "Student not found.")
        return await admin_students_page_callback(update, context)
    try:
        if action == "view":
            return await admin_view_for_student(student_id, q, context)
        elif action == "log":
            return await admin_logclass_for_student(student_id, q, context)
        elif action == "cancel":
            return await admin_cancel_for_student(student_id, q, context)
        elif action == "renew":
            return await admin_renew_for_student(student_id, q, context)
        elif action == "free":
            return await admin_free_for_student(student_id, q, context)
        elif action == "resched":
            return await admin_resched_for_student(student_id, q, context)
        elif action == "length":
            return await admin_length_for_student(student_id, q, context)
        elif action == "schedule":
            return await admin_schedule_for_student(student_id, q, context)
        elif action == "pause":
            return await admin_pause_for_student(student_id, q, context)
        elif action == "remove":
            return await admin_remove_for_student(student_id, q, context)
    except Exception:
        logging.exception(
            "ADMIN STUDENT ACTION CRASH sid=%s action=%s", student_id, action
        )
        return await safe_edit_or_send(q, "Temporary issue handling that action.")


async def initiate_log_class(query, context, student_id, student):
    return await safe_edit_or_send(
        query, "Logging flow not yet implemented for this student."
    )


async def initiate_cancel_class_admin(query, context, student_id, student):
    # Admin-specific view ignores cancelled_dates and past logs
    upcoming_list = get_admin_upcoming_classes(student_id, student, count=8)
    if not upcoming_list:
        return await safe_edit_or_send(query, "No upcoming classes to cancel.")
    context.user_data["admin_cancel"] = {
        "student_key": student_id,
        "late": False,
        "note": "",
    }
    buttons = []
    for idx, dt in enumerate(upcoming_list):
        label = dt.strftime("%a %d %b %H:%M")
        buttons.append([
            InlineKeyboardButton(label, callback_data=f"admin_cancel_sel:{idx}")
        ])
    kb = InlineKeyboardMarkup(buttons)
    return await safe_edit_or_send(query, "Select class to cancel:", reply_markup=kb)


async def initiate_renew_student(query, context, student_id, student):
    return await safe_edit_or_send(query, "Renewal flow not yet implemented.")


async def initiate_award_free(query, context, student_id, student):
    students = load_students()
    s = students.get(student_id)
    if not s:
        return await safe_edit_or_send(query, "Student not found.")
    if is_premium(s):
        return await safe_edit_or_send(
            query, f"Note: {s['name']} is Premium; award/renew not required."
        )
    s["free_class_credit"] = s.get("free_class_credit", 0) + 1
    save_students(students)
    logs = load_logs()
    logs.append(
        {
            "student": student_id,
            "date": datetime.now(student_timezone(s)).isoformat(),
            "status": "free_credit_awarded",
            "note": "admin award free credit",
        }
    )
    save_logs(logs)
    return await safe_edit_or_send(
        query,
        f"Awarded a free class credit to {s['name']}. They now have {s['free_class_credit']} free credit(s).",
    )


async def initiate_reschedule_student(query, context, student_id, student):
    return await safe_edit_or_send(query, "Reschedule flow not yet implemented.")


async def initiate_change_length(query, context, student_id, student):
    return await safe_edit_or_send(query, "Change length flow not yet implemented.")


async def initiate_edit_schedule(query, context, student_id, student):
    return await safe_edit_or_send(query, "Edit schedule flow not yet implemented.")


async def initiate_pause_toggle(query, context, student_id, student):
    students = load_students()
    s = students.get(student_id)
    if not s:
        return await safe_edit_or_send(query, "Student not found.")
    s["paused"] = not s.get("paused", False)
    save_students(students)
    state = "paused" if s["paused"] else "resumed"
    return await safe_edit_or_send(
        query, f"{s.get('name', student_id)} has been {state}."
    )


async def initiate_remove_student(query, context, student_id, student):
    return await safe_edit_or_send(query, "Removal flow not yet implemented.")


async def log_unknown_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log any unrecognised callback data for diagnostics."""

    query = update.callback_query
    logging.warning(
        "UNKNOWN CALLBACK data=%s user=%s",
        getattr(query, "data", None),
        getattr(getattr(query, "from_user", None), "id", None),
    )
    try:
        await query.answer("Unknown action.", show_alert=False)
    except Exception:
        pass


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
        if not s.get("telegram_mode", True):
            ident = "offline"
        elif handle:
            ident = f"@{handle}"
        elif s.get("telegram_id"):
            ident = f"id {s['telegram_id']}"
        else:
            ident = "no handle"
        if s.get("needs_id") and s.get("telegram_mode", True):
            ident += " (needs ID)"
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
        if student.get("needs_id"):
            label += " (needs ID)"
        buttons.append([InlineKeyboardButton(label, callback_data=f"edit:pick:{key}")])
    if not buttons:
        if update.message:
            await update.message.reply_text("No active students found.")
        else:
            await safe_edit_or_send(update.callback_query, "No active students found.")
        return
    text = "Select a student to edit:"
    markup = InlineKeyboardMarkup(buttons)
    if update.message:
        await update.message.reply_text(text, reply_markup=markup)
    else:
        await safe_edit_or_send(update.callback_query, text, reply_markup=markup)


@admin_only
async def edit_pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle selection of a student to edit."""
    query = update.callback_query
    await try_ack(query)
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
    await try_ack(query)
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
    await try_ack(query)
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
    await try_ack(query)
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
    await try_ack(query)
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
    await try_ack(query)
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
    month_logs: List[Dict[str, Any]] = []
    for entry in logs:
        date_value = entry.get("date") or entry.get("at")
        if not date_value:
            continue
        try:
            entry_date = parse_log_date(str(date_value))
        except Exception:
            logging.warning("Skipping log with invalid date for export: %s", entry)
            continue
        if month_start <= entry_date <= month_end:
            month_logs.append(entry)
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


async def confirm_cancel_for_student(
    context: ContextTypes.DEFAULT_TYPE,
    students: Dict[str, Any],
    student_key: str,
    student: Dict[str, Any],
) -> Tuple[str, str]:
    """Core logic to confirm a student's pending cancellation.

    Returns a tuple ``(response_message, class_time_str)``.  Raises ``ValueError``
    if there is no pending cancellation or the class time is malformed.
    """

    pending_cancel = student.get("pending_cancel")
    if not pending_cancel:
        raise ValueError("There is no pending cancellation to confirm.")

    class_time_str = pending_cancel.get("class_time")
    request_time: Optional[datetime] = None
    try:
        datetime.fromisoformat(class_time_str)
    except Exception as exc:  # pragma: no cover - malformed time
        raise ValueError("Invalid class time format; cancellation not confirmed.") from exc

    requested_at_str = pending_cancel.get("requested_at")
    if requested_at_str:
        try:
            request_time = datetime.fromisoformat(requested_at_str)
        except Exception:
            logging.warning(
                "Unable to parse requested_at '%s' for student %s", requested_at_str, student_key
            )
            request_time = None
        else:
            if request_time.tzinfo is None:
                request_time = request_time.replace(tzinfo=timezone.utc)

    premium_student = is_premium(student)
    cancel_type = pending_cancel.get("type", "late")
    cutoff_hours = 99999 if premium_student else student.get("cutoff_hours", 24)
    student_key_str = str(student_key)

    cancelled = data_store.cancel_single_class(
        student_key_str,
        class_time_str,
        cutoff_hours,
        log=False,
        request_time=request_time,
    )
    if not cancelled:
        raise ValueError("Class not found; cancellation not confirmed.")

    students = data_store.load_students()
    student = students.get(student_key_str)
    if not student:
        raise ValueError("Student record missing after cancellation.")

    student.pop("pending_cancel", None)
    # Only extend for premium or early cancellations
    # Late cancellations should not regenerate dates
    should_extend = False

    if is_premium(student):
        should_extend = True
    elif cancel_type == "early":
        should_extend = True

    if should_extend:
        ensure_future_class_dates(student)

    if premium_student:
        response = (
            f"Cancellation confirmed for {student['name']}. (Premium — no deduction.)"
        )
        log_status = "cancelled (premium)"
    elif cancel_type == "early":
        response = (
            f"Cancellation confirmed for {student['name']}. Replacement class scheduled automatically."
        )
        log_status = "cancelled (early)"
    else:
        await maybe_send_balance_warning(getattr(context, "bot", None), student)
        response = (
            f"Cancellation confirmed for {student['name']}. One class deducted."
        )
        log_status = "missed (late cancel)"

    data_store.save_students(students)

    # remove any scheduled reminder for this class and reschedule remaining
    app = getattr(context, "application", None)
    if app:
        for job in app.job_queue.jobs():
            if job.name == f"class_reminder:{student_key_str}:{class_time_str}":
                job.schedule_removal()
        schedule_student_reminders(app, student_key_str, student)
        await send_low_balance_if_threshold(app, student_key_str, student)
        schedule_final_set_notice(app, student_key_str, student)

    logs = load_logs()
    note = "admin confirm cancel"
    if is_premium(student):
        note += " (premium)"
    logs.append(
        {
            "student": student_key_str,
            "date": class_time_str,
            "status": log_status,
            "note": note,
        }
    )
    save_logs(logs)

    await refresh_student_menu(student_key_str, student, getattr(context, "bot", None))
    await refresh_student_my_classes(student_key_str, student, getattr(context, "bot", None))

    return response, class_time_str


@admin_only
async def confirm_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm a student's cancellation request via /confirmcancel."""

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

    try:
        response, _ = await confirm_cancel_for_student(
            context, students, student_key, student
        )
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

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
            "date": old_item,
            "status": "rescheduled",
            "note": f"to {new_dt_str}",
            "admin": update.effective_user.id,
        }
    )
    save_logs(logs)

    old_display = fmt_bkk(old_item)
    new_display = fmt_bkk(new_dt_str)
    msg = (
        f"Rescheduled {student.get('name', student_key)} "
        f"from {old_display} to {new_display}."
    )
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
    if not students:
        try:
            with open(STUDENTS_FILE, "w", encoding="utf-8") as f:
                json.dump({}, f, indent=2, ensure_ascii=False, sort_keys=True)
        except Exception:
            logging.warning("Failed to persist empty students.json during purge")

    for job in context.application.job_queue.jobs():
        name = job.name or ""
        if any(name.startswith(f"class_reminder:{k}:") for k in keys_to_delete):
            job.schedule_removal()

    logs = load_logs()
    try:
        removal_date = datetime.now(student_timezone(student))
    except TypeError:
        removal_date = ensure_bangkok(datetime.utcnow())
    logs.append(
        {
            "student": student_key,
            "date": removal_date.strftime("%Y-%m-%d"),
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

    # Retrieve admin-visible schedule: all upcoming unlogged classes
    schedule = get_admin_upcoming_classes(
        student_key, student, count=len(student.get("class_dates", []))
    )

    lines = [f"Student: {student.get('name', student_key)}"]
    lines.append(f"Classes remaining: {student.get('classes_remaining', 0)}")
    if schedule:
        lines.append("Schedule:")
        for dt in schedule:
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
    needs_id = [(k, s) for k, s in students.items() if s.get("needs_id")]
    logging.info(
        "datacheck stats total=%s both=%s mismatch=%s pending_cancel=%s needs_id=%s",
        total,
        both_fields,
        mismatch,
        pending_cancel,
        len(needs_id),
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
    lines = [
        f"DataCheck: students={total}, rekeyed={rekeyed}, logs_fixed={logs_fixed}, pending_cancel={pending_cancel}, needs_id={len(needs_id)}"
    ]
    if needs_id:
        lines.append("Needs ID:")
        for key, stu in needs_id:
            handle = stu.get("telegram_handle") or key
            name = stu.get("name", "")
            lines.append(f"- {handle} {name}".strip())
    await update.message.reply_text("\n".join(lines))


@admin_only
async def resolveids_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    students = load_students()
    logs = load_logs()
    resolved = 0
    failed = 0
    for key, student in list(students.items()):
        if not student.get("needs_id"):
            continue
        handle = student.get("telegram_handle") or normalize_handle(key)
        if not handle:
            failed += 1
            continue
        try:
            chat = await context.application.bot.get_chat(f"@{handle}")
            numeric_id = chat.id
        except Exception:
            failed += 1
            continue
        new_key = str(numeric_id)
        student["telegram_id"] = numeric_id
        student.pop("needs_id", None)
        students[new_key] = student
        if new_key != key:
            students.pop(key, None)
        for entry in logs:
            if entry.get("student") in {key, f"@{handle}"}:
                entry["student"] = new_key
        schedule_student_reminders(context.application, new_key, student)
        resolved += 1
    save_students(students)
    save_logs(logs)
    await update.message.reply_text(
        f"ResolveIDs: resolved={resolved}, failed={failed}"
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
async def fixlogs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    students = load_students()
    logs = load_logs()
    total = len(logs)
    cleaned: List[Dict[str, Any]] = []
    rewritten = 0
    dropped = 0
    for entry in logs:
        student_field = entry.get("student")
        if student_field is None:
            dropped += 1
            continue
        _, student = resolve_student(students, str(student_field))
        if student and student.get("telegram_id") is not None:
            canonical = str(student["telegram_id"])
            if entry.get("student") != canonical:
                entry["student"] = canonical
                rewritten += 1
            cleaned.append(entry)
        else:
            dropped += 1
    save_logs(cleaned)
    if rewritten or dropped:
        message = (
            "🛠 FixLogs:\n"
            f"Total logs processed: {total}\n"
            f"Rewritten: {rewritten}\n"
            f"Dropped: {dropped}"
        )
    else:
        message = "🛠 FixLogs:\nAll logs already clean ✅"
    await update.message.reply_text(message)


@admin_only
async def migrate_logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run schema migration ensuring all logs contain a ``date`` field."""

    try:
        migrated = data_store.migrate_log_schemas()
    except Exception as exc:
        await update.message.reply_text(f"❌ Migration failed: {exc}")
        return

    await update.message.reply_text(
        f"✅ Migration complete: {migrated} log entries updated."
    )


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


def build_admin_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📅 Day View", callback_data="admin:dayview"),
            InlineKeyboardButton("📊 Dashboard", callback_data="admin:dashboard"),
        ],
        [
            InlineKeyboardButton("👥 Students", callback_data="admin:students"),
            InlineKeyboardButton("➕ Add Student", callback_data="admin:addstudent"),
        ],
        [
            InlineKeyboardButton("📂 Logs / Exports", callback_data="admin:logs"),
            InlineKeyboardButton("⚙️ Settings", callback_data="admin:settings"),
        ],
    ]
    rows.append([InlineKeyboardButton("📩 Read Asks", callback_data="admin:asks")])
    return InlineKeyboardMarkup(rows)


def display_name(student_id: str, student: dict) -> str:
    handle = student.get("telegram_handle")
    name = student.get("name") or handle or student_id
    if handle and not handle.startswith("@"):
        handle = "@" + handle
    label = f"{name} {handle or ''}".strip()
    if student.get("needs_id"):
        label += " (needs ID)"
    return label


def build_students_page_kb(
    students: dict, page: int = 0, per_page: int = 10
) -> InlineKeyboardMarkup:
    ids = list(students.keys())
    start = max(0, page * per_page)
    end = min(len(ids), start + per_page)
    rows = []
    for sid in ids[start:end]:
        s = students[sid]
        rows.append(
            [InlineKeyboardButton(display_name(sid, s), callback_data=f"admin:pick:{sid}")]
        )
    nav = []
    if start > 0:
        nav.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:students:page:{page-1}")
        )
    if end < len(ids):
        nav.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"admin:students:page:{page+1}")
        )
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin:root")])
    return InlineKeyboardMarkup(rows)


def build_student_submenu_kb(student_id: str) -> InlineKeyboardMarkup:
    """Shim for legacy imports; delegates to :mod:`keyboard_builders`."""
    return kb_build_student_submenu(student_id)


def build_student_detail_view(student_id: str, student: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    """Shim delegating to :mod:`keyboard_builders`."""
    return kb_build_student_detail_view(student_id, student)


def build_start_message(student: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    """Return the welcome text and keyboard for a student."""
    upcoming = get_student_visible_classes(student, count=1)
    next_class_str = fmt_bkk(upcoming[0]) if upcoming else "No upcoming classes set"
    classes_remaining = student.get("classes_remaining", 0)
    lines = [f"Hello, {student['name']}!"]
    if is_premium(student):
        lines.append("🌟 Premium member")
    lines.append(f"Your next class: {next_class_str}")
    if is_premium(student):
        lines.append("Classes remaining: ∞")
    else:
        lines.append(f"Classes remaining: {classes_remaining}")
        last_class = get_last_class(student)
        if last_class:
            lines.append(f"Set ends: {last_class.date().isoformat()}")
    reminder_minutes = get_student_reminder_minutes(student)
    lines.append(f"Reminder notifications: {reminder_setting_summary(reminder_minutes)}")
    if student.get("paused"):
        lines.append("Your plan is currently paused. Contact your teacher to resume.")
    buttons = [
        [InlineKeyboardButton("📅 My Classes", callback_data="my_classes")],
        [InlineKeyboardButton("❌ Cancel Class", callback_data="cancel_class")],
        [InlineKeyboardButton("🔔 Notification Settings", callback_data="notification_settings")],
    ]
    student_id = student.get("telegram_id")
    try:
        ask_target = int(student_id) if student_id is not None else None
    except (TypeError, ValueError):
        ask_target = None
    if ask_target is not None:
        buttons.append(
            [
                InlineKeyboardButton(
                    "❓ Ask Tutor", callback_data=f"ask:start:{ask_target}"
                )
            ]
        )
    if student.get("free_class_credit", 0) > 0:
        buttons.append([InlineKeyboardButton("🎁 Free Class Credit", callback_data="free_credit")])
    return "\n".join(lines), InlineKeyboardMarkup(buttons)


async def refresh_student_menu(
    student_key: str, student: Dict[str, Any], bot
) -> None:
    """Send an updated /start summary to the student's chat."""
    if not student.get("telegram_mode", True):
        return
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
    if not student.get("telegram_mode", True):
        return
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
        student.pop("needs_id", None)
        save_students(students)
    else:
        updated = False
        if student.get("telegram_id") != int(user_id):
            student["telegram_id"] = int(user_id)
            updated = True
        if new_handle and student.get("telegram_handle") != new_handle:
            student["telegram_handle"] = new_handle
            updated = True
        if student.get("needs_id"):
            student.pop("needs_id", None)
            updated = True
        if updated:
            save_students(students)
    text, markup = build_start_message(student)
    await update.message.reply_text(text, reply_markup=markup)


async def student_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button presses from students (inline keyboard)."""
    query = update.callback_query
    await try_ack(query)
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
            await try_ack(query, text=f"tap:{data}", show_alert=False)
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
            user_id = user.id if user else None
            if user_id in ADMIN_IDS:
                summary = generate_dashboard_summary()
                kb = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🕒 Pending Actions", callback_data="admin_pending")]]
                )
                await safe_edit_or_send(query, summary, reply_markup=kb)
                return
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
            if student.get("paused"):
                await safe_edit_or_send(query, PAUSED_ACTION_MESSAGE)
            else:
                await initiate_cancel_class(query, student)
        elif data == "free_credit":
            await show_free_credit(query, student)
        elif data == "notification_settings":
            if student.get("paused"):
                await safe_edit_or_send(query, PAUSED_SETTINGS_MESSAGE)
            else:
                await show_notification_settings(query, student)
        elif data and data.startswith("notification_set:"):
            if student.get("paused"):
                await safe_edit_or_send(query, PAUSED_SETTINGS_MESSAGE)
            else:
                try:
                    minutes = int(data.split(":", 1)[1])
                except (TypeError, ValueError):
                    try:
                        await query.answer("Invalid option.", show_alert=True)
                    except Exception:
                        pass
                else:
                    await update_notification_setting(
                        query, student_key, student, students, minutes, context
                    )
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
    upcoming_list = get_student_visible_classes(student, count=limit)
    if upcoming_list:
        lines = [f"Upcoming classes for {student['name']}:"]
        if is_premium(student):
            lines.append("🌟 Premium member — unlimited hours (∞), no time limit.")
        lines.append("All times shown in Thai time (ICT).")
        for dt in upcoming_list:
            lines.append(f"  - {fmt_bkk(dt, add_label=False)}")
    else:
        lines = ["You have no classes scheduled.", "All times shown in Thai time (ICT)."]
        if is_premium(student):
            lines.insert(1, "🌟 Premium member — unlimited hours (∞), no time limit.")
    if is_premium(student):
        lines.append("Classes remaining: ∞")
    else:
        lines.append(f"Classes remaining: {student.get('classes_remaining', 0)}")
        last_class = get_last_class(student)
        if last_class:
            lines.append(f"Set ends: {last_class.date().isoformat()}")
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

        student_logs: List[Dict[str, Any]] = []
        for entry in logs:
            try:
                if not _matches(entry.get("student"), student_key):
                    logging.debug(
                        "LOG ENTRY student mismatch: expected=%s entry=%s",
                        student_key,
                        entry,
                    )
                    continue
                dt_str = entry.get("date") or entry.get("at", "")
                try:
                    parsed_dt = ensure_bangkok(dt_str)
                except Exception:
                    try:
                        parsed_dt = ensure_bangkok(dt_str + "T00:00")
                    except Exception:
                        logging.warning(
                            "LOG DATE PARSE FAIL for student=%s entry=%s",
                            student_key,
                            entry,
                        )
                        parsed_dt = bkk_min()
                entry["_parsed_dt"] = parsed_dt
                student_logs.append(entry)
            except Exception:
                logging.exception("BAD LOG ENTRY skipped: %s", entry)
                continue

        student_logs.sort(key=lambda e: e.get("_parsed_dt", bkk_min()), reverse=True)
        recent_logs = student_logs[:2]
        lines.append("")
        lines.append("Recent classes:")
        if recent_logs:
            for entry in recent_logs:
                try:
                    raw_status = (entry.get("status") or entry.get("type") or "")
                    status = str(raw_status).lower()
                    if status.startswith("class_"):
                        status = status[6:]
                    if status == "completed":
                        symbol = "✅"
                    elif status.startswith("missed") or status.startswith("cancelled") or status.startswith(
                        "rescheduled"
                    ):
                        symbol = "❌"
                    else:
                        symbol = "•"
                    dt = entry.get("_parsed_dt")
                    dt_txt = (
                        fmt_bkk(dt, add_label=False)
                        if isinstance(dt, datetime)
                        else entry.get("date")
                        or entry.get("at", "")
                    )
                    note = entry.get("note") or ""
                    if note:
                        lines.append(f"{symbol} {dt_txt} – {note}")
                    else:
                        lines.append(f"{symbol} {dt_txt}")
                except Exception:
                    logging.exception("BAD LOG ENTRY skipped: %s", entry)
                    continue
        else:
            lines.append("(No recent logs)")

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
            class_dt = ensure_bangkok(pending.get("class_time", ""))
            class_str = fmt_bkk(class_dt)
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
    if student.get("paused"):
        await safe_edit_or_send(query, PAUSED_ACTION_MESSAGE)
        return
    upcoming_list = get_student_cancellable_classes(student)
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
    await try_ack(query)
    user = query.from_user
    students = load_students()
    student_key, student = resolve_student(students, str(user.id))
    if not student and user.username:
        student_key, student = resolve_student(students, user.username)
    if not student:
        await safe_edit_or_send(query, "You are not recognised. Please contact your teacher.")
        return
    if student.get("paused"):
        await safe_edit_or_send(query, PAUSED_ACTION_MESSAGE)
        return
    _, index_str = query.data.split(":")
    try:
        idx = int(index_str)
    except ValueError:
        await safe_edit_or_send(query, "Invalid selection.")
        return
    upcoming = get_student_cancellable_classes(student)
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


async def process_connect_student_reply(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    students: Dict[str, Any],
) -> bool:
    """Handle admin responses when linking an offline student to Telegram."""

    connect_state = context.user_data.get("connect_student")
    if not connect_state:
        return False
    message = update.message
    if not message or not hasattr(message, "text"):
        return True
    raw_text = (message.text or "").strip()
    if not raw_text:
        await message.reply_text("Please enter a Telegram handle or numeric ID.")
        return True
    if raw_text.lower() in {"cancel", "stop"}:
        context.user_data.pop("connect_student", None)
        original_key = str(connect_state.get("student_key", ""))
        student_key, student = resolve_student(students, original_key)
        if student:
            await _send_admin_student_detail(context, connect_state, student_key, student, message)
        await message.reply_text("Connect to Telegram cancelled.")
        return True

    original_key = str(connect_state.get("student_key", ""))
    student_key, student = resolve_student(students, original_key)
    if not student or not student_key:
        context.user_data.pop("connect_student", None)
        await message.reply_text("Student not found.")
        return True
    if student.get("telegram_mode", True):
        context.user_data.pop("connect_student", None)
        await message.reply_text("This student is already connected to Telegram.")
        await _send_admin_student_detail(context, connect_state, student_key, student, message)
        return True

    input_text = raw_text.lstrip()
    if input_text.startswith("@"):
        input_text = input_text[1:]
    telegram_id: Optional[int] = None
    handle_value: Optional[str] = None
    if input_text.isdigit() and not raw_text.startswith("@"):
        telegram_id = int(input_text)
        handle_value = student.get("telegram_handle")
    else:
        normalized = normalize_handle(input_text)
        if not normalized:
            await message.reply_text("Please enter a valid Telegram handle or numeric ID.")
            return True
        bot = getattr(context, "bot", None) or getattr(
            getattr(context, "application", None), "bot", None
        )
        handle_value = normalized
        if bot and hasattr(bot, "get_chat"):
            try:
                chat = await bot.get_chat(f"@{normalized}")
                telegram_id = int(chat.id)
                username = getattr(chat, "username", None)
                handle_value = normalize_handle(username) if username else normalized
            except Exception:
                telegram_id = None
        else:
            telegram_id = None

    student["telegram_mode"] = True
    if telegram_id:
        student["telegram_id"] = int(telegram_id)
        student.pop("needs_id", None)
    else:
        student["telegram_id"] = None
        student["needs_id"] = True
    student["telegram_handle"] = handle_value

    new_key = student_key
    if telegram_id:
        new_key = str(int(telegram_id))
    if new_key != student_key:
        if new_key in students:
            await update.message.reply_text(
                "❌ Error: A student with that Telegram ID already exists. Please check the ID and try again."
            )
            return
        students.pop(student_key, None)
    students[new_key] = student
    save_students(students)

    if new_key != student_key:
        logs = load_logs()
        changed = False
        for entry in logs:
            if entry.get("student") == student_key:
                entry["student"] = new_key
                changed = True
        if changed:
            save_logs(logs)

    application = getattr(context, "application", None)
    job_queue = getattr(application, "job_queue", None) if application else None
    if (
        telegram_id
        and application
        and job_queue
        and hasattr(job_queue, "run_once")
        and callable(getattr(job_queue, "jobs", None))
    ):
        try:
            schedule_student_reminders(application, new_key, student)
            schedule_final_set_notice(application, new_key, student)
        except Exception:
            logging.warning("Failed to schedule reminders for %s", student.get("name", new_key))

    bot = getattr(context, "bot", None) or getattr(application, "bot", None)
    if telegram_id and bot:
        try:
            await refresh_student_menu(new_key, student, bot)
        except Exception:
            logging.debug("Failed to refresh student menu for %s", new_key)

    context.user_data.pop("connect_student", None)

    student_name = student.get("name", new_key)
    if telegram_id:
        handle_label = f" (@{handle_value})" if handle_value else ""
        summary = (
            f"Connected {student_name} to Telegram ID {telegram_id}{handle_label}."
        )
    else:
        summary = (
            f"Stored @{handle_value} for {student_name}. "
            "They will be fully linked once they /start the bot or you run /resolveids."
        )
    await message.reply_text(summary)
    await _send_admin_student_detail(context, connect_state, new_key, student, message)
    return True


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle free-form messages for both admins and students."""
    user = update.effective_user

    # Automatically re-key handle-based student records once the
    # user has sent a message and their numeric Telegram ID is known.
    students = load_students()
    if user:
        tid = str(user.id)
        handle = getattr(user, "username", None)
        if handle and handle in students and tid not in students:
            student = students.pop(handle)
            student["telegram_id"] = user.id
            students[tid] = student
            save_students(students)
            logging.info("Rekeyed student record from handle '%s' to ID '%s'", handle, tid)

    user_id = user.id if user else None
    if user_id in ADMIN_IDS:
        if await process_connect_student_reply(update, context, students):
            return
        renew_id = context.user_data.get("renew_waiting_for_qty")
        if renew_id:
            await renew_received_count(update, context)
            return
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
                old_display = fmt_bkk(old_dt_str)
                new_display = fmt_bkk(new_dt)
                await update.message.reply_text(
                    f"Rescheduled class from {old_display} to {new_display}.",
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


async def show_notification_settings(query, student: Dict[str, Any]) -> None:
    """Display the notification settings view to a student."""
    if student.get("paused"):
        await safe_edit_or_send(query, PAUSED_SETTINGS_MESSAGE)
        return
    text, markup = build_notification_settings_view(student)
    await safe_edit_or_send(query, text, reply_markup=markup)


async def update_notification_setting(
    query,
    student_key: str,
    student: Dict[str, Any],
    students: Dict[str, Any],
    minutes: int,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Persist a new reminder preference and reschedule jobs."""

    if minutes not in REMINDER_VALID_MINUTES:
        try:
            await query.answer("Unsupported option.", show_alert=True)
        except Exception:
            pass
        return
    minutes = normalize_reminder_minutes(minutes)
    if student.get("paused"):
        await safe_edit_or_send(query, PAUSED_SETTINGS_MESSAGE)
        return
    current = get_student_reminder_minutes(student)
    if current != minutes:
        student["reminder_offset_minutes"] = minutes
        save_students(students)
        if hasattr(context, "application"):
            schedule_student_reminders(context.application, student_key, student)
        status = f"✅ Reminders updated to {reminder_setting_sentence(minutes)}."
        await refresh_student_menu(student_key, student, getattr(context, "bot", None))
    else:
        status = f"Your reminders are already set to {reminder_setting_sentence(minutes)}."
    text, markup = build_notification_settings_view(student, status=status)
    await safe_edit_or_send(query, text, reply_markup=markup)


# -----------------------------------------------------------------------------
# Automatic jobs (balance warnings, monthly export)
# -----------------------------------------------------------------------------
async def maybe_send_balance_warning(bot, student) -> bool:
    """Send a warning to the student if their balance is low.

    Returns True if the student's record was modified (i.e., a warning was sent).
    """
    if (
        student.get("paused")
        or not student.get("telegram_mode", True)
        or is_premium(student)
    ):
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
    month_logs: List[Dict[str, Any]] = []
    for entry in logs:
        date_value = entry.get("date") or entry.get("at")
        if not date_value:
            continue
        try:
            entry_date = parse_log_date(str(date_value))
        except Exception:
            logging.warning("Skipping log with invalid date for export: %s", entry)
            continue
        if month_start <= entry_date <= month_end:
            month_logs.append(entry)
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

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the exception and inform the user of a generic error."""
    logging.error("Unhandled exception", exc_info=context.error)
    message = getattr(update, "effective_message", None)
    if message:
        try:
            await message.reply_text("Error occurred")
        except Exception:
            pass

def build_application() -> Application:
    """Return an application with core command and student handlers.

    This helper is used by tests and diagnostic tools to inspect the
    configured handlers without starting the bot.
    """
    app = ApplicationBuilder().token(TOKEN).build()
    if hasattr(app, "add_error_handler"):
        app.add_error_handler(global_error_handler)
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(
        CallbackQueryHandler(
            connect_student_callback, pattern=r"^stu:CONNECT:[^:]+$"
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            handle_student_action,
            pattern=r"^stu:(LOG|CANCEL|RESHED|RENEW|RENEW_SAME|RENEW_ENTER|LENGTH|EDIT|FREECREDIT|PAUSE|REMOVE|VIEW|ADHOC):[^:]+$",
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            handle_class_selection, pattern=r"^cls:(LOG|CANCEL|RESHED):"
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            handle_log_action,
            pattern=r"^log:(COMPLETE|CANCEL_EARLY|CANCEL_LATE|RESCHEDULED|UNLOG):",
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            handle_class_confirmation, pattern=r"^cfm:(CANCEL|RESHED):"
        )
    )
    return app

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
    if hasattr(application, "add_error_handler"):
        application.add_error_handler(global_error_handler)

    # Conversation handler for adding student
    async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Fallback handler to gracefully cancel the add‑student conversation."""
        await update.message.reply_text("Operation cancelled.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("addstudent", add_student_command),
            CallbackQueryHandler(add_student_command, pattern=r"^admin:addstudent$", block=True),
        ],
        states={
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name)],
            ADD_TELEGRAM_CHOICE: [
                CallbackQueryHandler(
                    add_telegram_choice,
                    pattern=r"^student_(has_telegram|no_telegram|cancel)$",
                )
            ],
            ADD_HANDLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_handle)],
            ADD_CLASSES: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_classes)],
            ADD_SCHEDULE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_schedule)],
            ADD_CUTOFF: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_cutoff)],
            ADD_DURATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_duration)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
        allow_reentry=True,
    )
    application.add_handler(conv_handler)

    import ask_tutor

    ask_tutor.register_handlers(application, admin_ids=ADMIN_IDS)
    # Admin commands
    application.add_handler(CommandHandler("logclass", log_class_command))
    application.add_handler(CommandHandler("cancelclass", cancel_class_command))
    application.add_handler(CommandHandler("awardfree", award_free_command))
    application.add_handler(CommandHandler("renewstudent", renew_student_command))
    application.add_handler(CommandHandler("setpremium", set_premium_command))
    application.add_handler(CommandHandler("pause", pause_student_command))
    application.add_handler(CommandHandler("liststudents", list_students_command))
    application.add_handler(CommandHandler("edit", edit_command))
    application.add_handler(CommandHandler("dashboard", dashboard_command))
    application.add_handler(CommandHandler("downloadmonth", download_month_command))
    application.add_handler(CommandHandler("selftest", selftest_command))
    application.add_handler(CommandHandler("datacheck", datacheck_command))
    application.add_handler(CommandHandler("resolveids", resolveids_command))
    application.add_handler(CommandHandler("checklogs", checklogs_command))
    application.add_handler(CommandHandler("fixlogs", fixlogs_command))
    application.add_handler(CommandHandler("migratelogs", migrate_logs_command))
    application.add_handler(CommandHandler("nukepending", nukepending_command))
    application.add_handler(CommandHandler("confirmcancel", confirm_cancel_command))
    application.add_handler(CommandHandler("reschedulestudent", reschedule_student_command))
    application.add_handler(CommandHandler("removestudent", remove_student_command))
    application.add_handler(CommandHandler("viewstudent", view_student))
    application.add_handler(CommandHandler("dayview", dayview_command))
    application.add_handler(CommandHandler("admin", admin_command))

    # Student handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(dayview_callback, pattern="^dayview:"))
    application.add_handler(CallbackQueryHandler(edit_pick_callback, pattern="^edit:pick:"))
    application.add_handler(CallbackQueryHandler(edit_menu_callback, pattern="^edit:option:"))
    application.add_handler(CallbackQueryHandler(edit_delweekly_callback, pattern="^edit:delweekly:"))
    application.add_handler(CallbackQueryHandler(edit_time_slot_callback, pattern="^edit:time:slot:"))
    application.add_handler(CallbackQueryHandler(edit_time_scope_callback, pattern="^edit:time:scope:"))
    application.add_handler(CallbackQueryHandler(edit_time_oncepick_callback, pattern="^edit:time:oncepick:"))
    application.add_handler(
        CallbackQueryHandler(
            connect_student_callback, pattern=r"^stu:CONNECT:[^:]+$"
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handle_student_action,
            pattern=r"^stu:(LOG|CANCEL|RESHED|RENEW|RENEW_SAME|RENEW_ENTER|LENGTH|EDIT|FREECREDIT|PAUSE|REMOVE|VIEW|ADHOC):[^:]+$",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handle_class_selection, pattern=r"^cls:(LOG|CANCEL|RESHED):"
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handle_log_action,
            pattern=r"^log:(COMPLETE|CANCEL_EARLY|CANCEL_LATE|RESCHEDULED|UNLOG):",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handle_class_confirmation, pattern=r"^cfm:(CANCEL|RESHED):"
        )
    )
    application.add_handler(
        CallbackQueryHandler(renew_confirm, pattern=r"^cfm:RENEW:")
    )
    application.add_handler(
        CallbackQueryHandler(
            admin_pick_student_callback, pattern=r"^admin:pick:[^:]+$"
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            admin_students_page_callback,
            pattern=r"^admin:students(:page:(-?\d+))?$",
        )
    )
    application.add_handler(CallbackQueryHandler(admin_menu_callback, pattern=r"^admin:"))
    application.add_handler(CallbackQueryHandler(handle_cancel_selection, pattern=r"^cancel_selected:"))
    application.add_handler(CallbackQueryHandler(admin_cancel_callback, pattern="^admin_cancel_sel:"))
    application.add_handler(CallbackQueryHandler(debug_ping_callback, pattern="^__ping__$"))
    application.add_handler(CallbackQueryHandler(admin_pending_callback, pattern=r"^admin_pending$"))
    application.add_handler(
        CallbackQueryHandler(confirm_pending_callback, pattern=r"^confirm_pending:([^:]+)$")
    )
    application.add_handler(
        CallbackQueryHandler(
            student_button_handler,
            pattern=r"^(my_classes|cancel_class|free_credit|cancel_withdraw|cancel_dismiss|back_to_start|notification_settings|notification_set:(?:0|5|15|30|60))$",
        )
    )
    application.add_handler(CallbackQueryHandler(log_unknown_callback, pattern=r".*"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # Ensure schedules extend into the future and reminders are set
    students = load_students()
    for key, student in students.items():
        schedule_student_reminders(application, key, student)
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
