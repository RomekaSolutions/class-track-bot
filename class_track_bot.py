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
and various flags (paused, silent mode, free class credits etc.).
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
    filters,
)


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
ADMIN_IDS = {123456789}


# Paths to the JSON database files.  Adjust if you wish to store them elsewhere.
STUDENTS_FILE = "students.json"
LOGS_FILE = "logs.json"


# Conversation states for adding a student
(
    ADD_NAME,
    ADD_HANDLE,
    ADD_PRICE,
    ADD_CLASSES,
    ADD_SCHEDULE,
    ADD_RENEWAL,
    ADD_COLOR,
    CONFIRM_ADD,
) = range(8)

# States for rescheduling classes
(RESCHEDULE_SELECT, RESCHEDULE_TIME, RESCHEDULE_CONFIRM) = range(20, 23)


def load_students() -> Dict[str, Any]:
    """Load students from the JSON file.  If none exists return an empty dict."""
    if not os.path.exists(STUDENTS_FILE):
        return {}
    with open(STUDENTS_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            # If stored as list in earlier versions, convert to dict keyed by telegram_id
            if isinstance(data, list):
                return {str(item["telegram_id"]): item for item in data}
        except json.JSONDecodeError:
            logging.error("Failed to parse students.json; starting with empty database.")
            return {}
    return {}


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


def parse_schedule(schedule_str: str) -> List[str]:
    """Parse a comma‑separated list of schedule entries into a list.

    Users can input schedules like "Monday 17:00, Thursday 17:00".
    We store the stripped entries as they are; a later function will
    compute concrete datetimes from these patterns.
    """
    entries = []
    for item in schedule_str.split(","):
        entry = item.strip()
        if entry:
            entries.append(entry)
    return entries


def parse_renewal_date(date_str: str) -> Optional[str]:
    """Validate a date string in YYYY‑MM‑DD format.  Returns the same
    string if valid, otherwise None."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        return None


def next_occurrence(day_time_str: str, now: Optional[datetime] = None) -> datetime:
    """Given a schedule entry like "Monday 17:00", return the next datetime
    after `now` that matches that weekday and time.

    If `now` is None the current UTC time is used.  The function
    currently assumes naive datetime objects representing local time.
    
    Example: if today is Monday at 16:00 and schedule is "Monday 17:00",
    the next occurrence will be today at 17:00.  If it's already past
    17:00, the result will be next week.
    """
    if now is None:
        now = datetime.now()
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
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)
        # If candidate is before now, push to next week
        if candidate <= now:
            candidate += timedelta(days=7)
        return candidate
    except Exception:
        # If parsing fails just return now + 1 hour as fallback
        return now + timedelta(hours=1)


def get_upcoming_classes(student: Dict[str, Any], count: int = 5) -> List[datetime]:
    """Given a student record, return the next `count` class datetimes.

    This looks at the student's ``class_schedule`` entries (e.g.,
    ["Monday 17:00", "Thursday 17:00"]) and computes the next
    occurrences.  Results are sorted chronologically.  If the schedule
    list is empty, returns an empty list.

    The returned datetimes are naive (no timezone) and represent local
    time; adjust accordingly if you require timezone awareness.
    """
    schedule_entries = student.get("class_schedule", []) or []
    now = datetime.now()
    occurrences: List[datetime] = []
    # Compute next occurrence for each schedule entry, then iterate to build list
    for entry in schedule_entries:
        next_time = next_occurrence(entry, now)
        occurrences.append(next_time)
    # For additional classes (beyond one per entry) we step by weeks
    results: List[datetime] = []
    while len(results) < count and occurrences:
        soonest = min(occurrences)
        results.append(soonest)
        # Move the used occurrence one week ahead for its entry
        idx = occurrences.index(soonest)
        entry = schedule_entries[idx]
        occurrences[idx] = soonest + timedelta(days=7)
    return results


def admin_only(func):
    """Decorator to ensure a command is executed by an admin user."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("Sorry, you are not authorized to perform this command.")
            return
        return await func(update, context)

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
    # Remove leading @ if present
    if handle.startswith("@"):  # store as handle for now
        context.user_data["telegram_handle"] = handle
    else:
        try:
            context.user_data["telegram_id"] = int(handle)
        except ValueError:
            context.user_data["telegram_handle"] = handle
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
    if schedule_input:
        schedule = parse_schedule(schedule_input)
    else:
        schedule = []
    context.user_data["class_schedule"] = schedule
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
    await update.message.reply_text(
        "Optional: assign a color code for external planner reference (or type 'skip'):"
    )
    return ADD_COLOR


async def add_color(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    color_input = update.message.text.strip()
    if color_input.lower() != "skip":
        context.user_data["color_code"] = color_input
    # Build student record
    students = load_students()
    # Determine Telegram ID; if only handle provided, we'll store as string until the user interacts
    telegram_id = context.user_data.get("telegram_id")
    handle = context.user_data.get("telegram_handle")
    # Use handle or id as key
    key = str(telegram_id) if telegram_id else handle
    if key in students:
        await update.message.reply_text("A student with this identifier already exists. Aborting.")
        return ConversationHandler.END
    students[key] = {
        "name": context.user_data.get("name"),
        "telegram_id": telegram_id,
        "telegram_handle": handle,
        "classes_remaining": context.user_data.get("classes_remaining"),
        "plan_price": context.user_data.get("plan_price"),
        "renewal_date": context.user_data.get("renewal_date"),
        "class_schedule": context.user_data.get("class_schedule"),
        "paused": False,
        "silent_mode": False,
        "free_class_credit": 0,
        "reschedule_credit": 0,
        "color_code": context.user_data.get("color_code", ""),
        "notes": [],
    }
    save_students(students)
    await update.message.reply_text(f"Added student {context.user_data.get('name')} successfully!")
    return ConversationHandler.END


@admin_only
async def log_class_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log a completed class for a student.

    Usage: /logclass <student_key> [optional note]
    student_key can be telegram_id or handle as stored in students.json.
    """
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /logclass <student_key> [note]")
        return
    student_key = args[0]
    note = " ".join(args[1:]) if len(args) > 1 else ""
    students = load_students()
    if student_key not in students:
        await update.message.reply_text(f"Student '{student_key}' not found.")
        return
    student = students[student_key]
    # Deduct a class.  If they have free class credit, consume that first.
    if student.get("free_class_credit", 0) > 0:
        student["free_class_credit"] -= 1
    else:
        if student.get("classes_remaining", 0) > 0:
            student["classes_remaining"] -= 1
        else:
            await update.message.reply_text(
                f"Warning: {student['name']} has no classes remaining. Logging anyway."
            )
    save_students(students)
    # Record log
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "status": "completed",
            "note": note,
        }
    )
    save_logs(logs)
    await update.message.reply_text(
        f"Logged class for {student['name']}. Note: '{note}'", reply_markup=ReplyKeyboardRemove()
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
    student_key = args[0]
    students = load_students()
    if student_key not in students:
        await update.message.reply_text(f"Student '{student_key}' not found.")
        return
    student = students[student_key]
    # Award a reschedule credit
    student["reschedule_credit"] = student.get("reschedule_credit", 0) + 1
    save_students(students)
    # Log
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now().strftime("%Y-%m-%d"),
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
    student_key = args[0]
    students = load_students()
    if student_key not in students:
        await update.message.reply_text(f"Student '{student_key}' not found.")
        return
    student = students[student_key]
    student["free_class_credit"] = student.get("free_class_credit", 0) + 1
    save_students(students)
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "status": "free_credit_awarded",
            "note": "",
        }
    )
    save_logs(logs)
    await update.message.reply_text(
        f"Awarded a free class credit to {student['name']}. They now have {student['free_class_credit']} free credit(s)."
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
    student_key = args[0]
    students = load_students()
    if student_key not in students:
        await update.message.reply_text(f"Student '{student_key}' not found.")
        return
    student = students[student_key]
    student["paused"] = not student.get("paused", False)
    save_students(students)
    state = "paused" if student["paused"] else "resumed"
    await update.message.reply_text(f"{student['name']}'s tracking has been {state}.")


def generate_dashboard_summary() -> str:
    """Generate a textual summary for the dashboard command.

    The summary includes today's classes, students with low class
    balances, upcoming renewals, paused students, free class credits and
    statistics about classes this month.
    """
    students = load_students()
    logs = load_logs()
    now = datetime.now()
    today_date = now.date()
    month_start = date(now.year, now.month, 1)

    today_classes: List[str] = []
    low_balances: List[str] = []
    upcoming_renewals: List[str] = []
    paused_students: List[str] = []
    free_credits: List[str] = []
    # Stats counters
    completed = 0
    missed = 0
    cancelled = 0
    rescheduled = 0

    # Compute simple stats from logs for current month
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

    for key, student in students.items():
        # Skip paused students for most checks
        if student.get("paused"):
            paused_students.append(student["name"])
            continue
        # Check upcoming classes for today
        for dt in get_upcoming_classes(student, count=3):
            if dt.date() == today_date:
                today_classes.append(f"{student['name']} at {dt.strftime('%H:%M')}")
                break
        # Low class warnings
        if student.get("classes_remaining", 0) <= 2:
            low_balances.append(f"{student['name']} ({student['classes_remaining']} left)")
        # Upcoming renewals within next 7 days
        try:
            renewal_date = datetime.strptime(student["renewal_date"], "%Y-%m-%d").date()
            if 0 <= (renewal_date - today_date).days <= 7:
                upcoming_renewals.append(f"{student['name']} ({student['renewal_date']})")
        except Exception:
            pass
        # Free credits
        if student.get("free_class_credit", 0) > 0:
            free_credits.append(f"{student['name']} ({student['free_class_credit']})")

    summary_lines = []
    summary_lines.append("📊 Dashboard Summary\n")
    summary_lines.append(f"Today's classes ({today_date.isoformat()}):")
    summary_lines.extend([f"  - {item}" for item in (today_classes or ["None"])])
    summary_lines.append("")
    summary_lines.append("Students with low class balance:")
    summary_lines.extend([f"  - {item}" for item in (low_balances or ["None"])] )
    summary_lines.append("")
    summary_lines.append("Upcoming payment renewals (next 7 days):")
    summary_lines.extend([f"  - {item}" for item in (upcoming_renewals or ["None"])] )
    summary_lines.append("")
    summary_lines.append("Paused students:")
    summary_lines.extend([f"  - {item}" for item in (paused_students or ["None"])] )
    summary_lines.append("")
    summary_lines.append("Free class credits:")
    summary_lines.extend([f"  - {item}" for item in (free_credits or ["None"])] )
    summary_lines.append("")
    summary_lines.append("Class statistics (this month):")
    summary_lines.append(f"  - Completed: {completed}")
    summary_lines.append(f"  - Missed/late cancels: {missed}")
    summary_lines.append(f"  - Cancelled: {cancelled}")
    summary_lines.append(f"  - Rescheduled: {rescheduled}")

    summary = "\n".join(summary_lines)
    return summary


@admin_only
async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display a summary dashboard to the admin."""
    summary = generate_dashboard_summary()
    await update.message.reply_text(summary)


@admin_only
async def confirm_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm a student's cancellation request.

    Usage: /confirmcancel <student_key>

    When a student requests to cancel a class more than 24h in advance,
    they are awarded a reschedule credit.  The admin can confirm by
    invoking this command.  If there is a pending cancellation for the
    student, the bot marks it as confirmed; otherwise it does nothing.
    """
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /confirmcancel <student_key>")
        return
    student_key = args[0]
    students = load_students()
    if student_key not in students:
        await update.message.reply_text(f"Student '{student_key}' not found.")
        return
    student = students[student_key]
    pending_cancel = student.get("pending_cancel")
    if not pending_cancel:
        await update.message.reply_text("There is no pending cancellation to confirm.")
        return
    # Confirm the cancellation: increase reschedule credit, clear pending
    student["reschedule_credit"] = student.get("reschedule_credit", 0) + 1
    student.pop("pending_cancel", None)
    save_students(students)
    # Log
    logs = load_logs()
    logs.append(
        {
            "student": student_key,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "status": "cancel_confirmed",
            "note": "",
        }
    )
    save_logs(logs)
    await update.message.reply_text(f"Cancellation confirmed for {student['name']}.")


# -----------------------------------------------------------------------------
# Student interface handlers
# -----------------------------------------------------------------------------

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start for students.  Show upcoming class, remaining credits and renewal date."""
    students = load_students()
    user = update.effective_user
    user_id = str(user.id)
    # Try lookup by telegram_id
    student = students.get(user_id)
    # If not found, attempt to match by handle stored in record
    if not student:
        # match by handle if we have one
        for s in students.values():
            if s.get("telegram_handle"):
                # remove leading @ for comparison
                handle = s["telegram_handle"].lstrip("@").lower()
                if (user.username or "").lower() == handle:
                    student = s
                    # assign telegram_id now for future lookups
                    s["telegram_id"] = user.id
                    students[str(user.id)] = s
                    # remove old handle key if present
                    # we can't remove because dictionary keyed by id.  We'll keep duplicate to be safe.
                    break
        if student:
            save_students(students)
    if not student:
        await update.message.reply_text(
            "Hello! You are not registered with this tutoring bot. Please contact your teacher to be added."
        )
        return
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
    # Show reschedule button only if they have credit
    if student.get("reschedule_credit", 0) > 0:
        buttons.append([InlineKeyboardButton("🔄 Reschedule Class", callback_data="reschedule_class")])
    # Show free class credit info if available
    if student.get("free_class_credit", 0) > 0:
        buttons.append([InlineKeyboardButton("🎁 Free Class Credit", callback_data="free_credit")])
    # Silent mode toggle
    if student.get("silent_mode"):
        buttons.append([InlineKeyboardButton("🔔 Enable Reminders", callback_data="silent_toggle")])
    else:
        buttons.append([InlineKeyboardButton("📳 Silent Mode", callback_data="silent_toggle")])
    reply_markup = InlineKeyboardMarkup(buttons)
    await update.message.reply_text("\n".join(message_lines), reply_markup=reply_markup)


async def student_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button presses from students (inline keyboard)."""
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    students = load_students()
    student = students.get(user_id)
    if not student:
        await query.edit_message_text("You are not recognised. Please contact your teacher.")
        return
    data = query.data
    if data == "my_classes":
        await show_my_classes(query, student)
    elif data == "cancel_class":
        await initiate_cancel_class(query, student)
    elif data == "reschedule_class":
        await initiate_reschedule(query, student, context)
    elif data == "free_credit":
        await show_free_credit(query, student)
    elif data == "silent_toggle":
        await toggle_silent_mode(query, students, student)
    else:
        await query.edit_message_text("Unknown action.")


async def show_my_classes(query, student: Dict[str, Any]) -> None:
    """Display upcoming scheduled classes and remaining credits."""
    upcoming_list = get_upcoming_classes(student, count=5)
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
    await query.edit_message_text("\n".join(lines), reply_markup=None)


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
    user_id = str(query.from_user.id)
    students = load_students()
    student = students.get(user_id)
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
    now = datetime.now()
    delta = selected_dt - now
    status: str
    # Determine if it's a late cancel (<24h) or reschedule credit
    if delta > timedelta(hours=24):
        # Early cancel: award pending cancellation that admin must confirm
        student["pending_cancel"] = {
            "class_time": selected_dt.strftime("%Y-%m-%d %H:%M"),
            "requested_at": now.strftime("%Y-%m-%d %H:%M"),
        }
        status = "requested"
        message = (
            "Cancellation request sent to your teacher. You will receive a reschedule "
            "credit once confirmed."
        )
    else:
        # Late cancel: class missed
        # Deduct a class
        if student.get("classes_remaining", 0) > 0:
            student["classes_remaining"] -= 1
        # Log missed
        logs = load_logs()
        logs.append(
            {
                "student": user_id,
                "date": now.strftime("%Y-%m-%d"),
                "status": "missed (late cancel)",
                "note": "",
            }
        )
        save_logs(logs)
        status = "missed"
        message = (
            "Class cancelled less than 24h before start and has been marked as missed. "
            "This class will be deducted from your plan."
        )
    save_students(students)
    # Notify user
    await query.edit_message_text(message)


async def initiate_reschedule(query, student: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> None:
    """Begin the rescheduling process.  Ask for proposed new time.

    This function now accepts the callback context so that we can store
    state in ``context.bot_data``.  A reschedule credit must be
    available for the student to proceed.  The student's ``reschedule_credit``
    count is not decremented here; it is decremented when the user
    actually submits a new time.
    """
    if student.get("reschedule_credit", 0) <= 0:
        await query.edit_message_text("You do not have any reschedule credits.")
        return
    # Ask the user to send a proposed new time
    await query.edit_message_text(
        "Please enter your proposed new class time (e.g., '2024-07-15 17:00'). "
        "Your teacher will confirm this change."
    )
    # Mark the user as awaiting reschedule time
    reschedule_ctx = context.bot_data.setdefault("reschedule_context", {})
    reschedule_ctx[query.from_user.id] = {
        "stage": RESCHEDULE_TIME,
    }


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Catch all handler for plain text messages from students, used for rescheduling."""
    user_id = update.effective_user.id
    # Check if the user is in reschedule context
    resched_ctx = context.bot_data.get("reschedule_context", {})
    if user_id in resched_ctx and resched_ctx[user_id]["stage"] == RESCHEDULE_TIME:
        proposed = update.message.text.strip()
        # Basic validation of datetime format
        try:
            proposed_dt = datetime.strptime(proposed, "%Y-%m-%d %H:%M")
        except ValueError:
            await update.message.reply_text("Invalid format. Please use YYYY-MM-DD HH:MM:")
            return
        students = load_students()
        student = students.get(str(user_id))
        if not student:
            await update.message.reply_text("You are not recognised. Please contact your teacher.")
            return
        # Deduct reschedule credit
        student["reschedule_credit"] -= 1
        # Save pending reschedule request
        student["pending_reschedule"] = {
            "proposed_time": proposed_dt.strftime("%Y-%m-%d %H:%M"),
            "requested_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        save_students(students)
        # Log reschedule request
        logs = load_logs()
        logs.append(
            {
                "student": str(user_id),
                "date": datetime.now().strftime("%Y-%m-%d"),
                "status": "reschedule_requested",
                "note": proposed_dt.strftime("%Y-%m-%d %H:%M"),
            }
        )
        save_logs(logs)
        # Clear reschedule context
        resched_ctx.pop(user_id, None)
        await update.message.reply_text(
            "Your reschedule request has been sent to your teacher. They will confirm with you shortly."
        )
        return
    # For any other plain message, we do nothing or inform
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


async def toggle_silent_mode(query, students: Dict[str, Any], student: Dict[str, Any]) -> None:
    """Toggle the student's silent mode state."""
    user_id = str(query.from_user.id)
    student["silent_mode"] = not student.get("silent_mode", False)
    save_students(students)
    if student["silent_mode"]:
        msg = "Renewal reminders have been silenced for this month."
    else:
        msg = "Renewal reminders have been re-enabled."
    await query.edit_message_text(msg)


# -----------------------------------------------------------------------------
# Automatic jobs (reminders, warnings, monthly export)
# -----------------------------------------------------------------------------

async def renewal_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send renewal reminders to students who are approaching their renewal date."""
    bot = context.bot
    students = load_students()
    today = datetime.now().date()
    for key, student in students.items():
        if student.get("paused"):
            continue
        if student.get("silent_mode"):
            continue
        try:
            renewal_date = datetime.strptime(student["renewal_date"], "%Y-%m-%d").date()
        except Exception:
            continue
        days_until = (renewal_date - today).days
        if days_until in {3, 1, 0}:
            text = (
                f"Hello {student['name']}, your plan renewal is due on {renewal_date}. "
                f"You have {student.get('classes_remaining', 0)} classes remaining."
            )
            telegram_id = student.get("telegram_id")
            if telegram_id:
                try:
                    await bot.send_message(chat_id=telegram_id, text=text)
                except Exception:
                    logging.warning(f"Failed to send renewal reminder to {student['name']}")


async def low_class_warning_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send warnings to students when their remaining classes are low."""
    bot = context.bot
    students = load_students()
    for key, student in students.items():
        if student.get("paused"):
            continue
        if student.get("classes_remaining", 0) in {2, 1}:
            telegram_id = student.get("telegram_id")
            if not telegram_id:
                continue
            text = (
                f"Hi {student['name']}, you have {student['classes_remaining']} class"
                f"{'es' if student['classes_remaining'] > 1 else ''} remaining in your plan. "
                "Please consider renewing soon."
            )
            try:
                await bot.send_message(chat_id=telegram_id, text=text)
            except Exception:
                logging.warning(f"Failed to send low class warning to {student['name']}")


async def monthly_export_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """At the end of the month, send the logs JSON to the admin(s)."""
    bot = context.bot
    logs = load_logs()
    # Determine current month range
    today = datetime.now().date()
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

    if TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
        logging.warning("Please set the TELEGRAM_BOT_TOKEN environment variable or edit the TOKEN constant.")

    application: Application = ApplicationBuilder().token(TOKEN).build()
    # Conversation handler for adding student
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("addstudent", add_student_command)],
        states={
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name)],
            ADD_HANDLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_handle)],
            ADD_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_price)],
            ADD_CLASSES: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_classes)],
            ADD_SCHEDULE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_schedule)],
            ADD_RENEWAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_renewal)],
            ADD_COLOR: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_color)],
        },
        fallbacks=[CommandHandler("cancel", lambda update, context: update.message.reply_text("Cancelled."))],
        allow_reentry=True,
    )
    application.add_handler(conv_handler)
    # Admin commands
    application.add_handler(CommandHandler("logclass", log_class_command))
    application.add_handler(CommandHandler("cancelclass", cancel_class_command))
    application.add_handler(CommandHandler("awardfree", award_free_command))
    application.add_handler(CommandHandler("pause", pause_student_command))
    application.add_handler(CommandHandler("dashboard", dashboard_command))
    application.add_handler(CommandHandler("confirmcancel", confirm_cancel_command))
    # Student handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(handle_cancel_selection, pattern=r"^cancel_selected:", block=False))
    application.add_handler(CallbackQueryHandler(student_button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # Job queue for reminders and monthly export
    # Renewal reminders at 09:00 every day
    application.job_queue.run_daily(renewal_reminder_job, time=time(hour=9, minute=0, second=0))
    # Low class warnings at 10:00 every day
    application.job_queue.run_daily(low_class_warning_job, time=time(hour=10, minute=0, second=0))
    # Monthly export on the last day at 23:00
    application.job_queue.run_monthly(monthly_export_job, day=0, time=time(hour=23, minute=0, second=0))
    # Start the bot
    application.run_polling()


if __name__ == "__main__":
    main()