import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Callable, List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

import data_store
import keyboard_builders
from helpers import (
    fmt_class_label,
    extract_weekly_pattern,
    generate_from_pattern,
)


async def safe_edit_or_send(target, text: str, reply_markup=None) -> None:
    """Edit message if possible, otherwise send a new message."""
    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(text, reply_markup=reply_markup)
    elif hasattr(target, "message") and target.message:
        await target.message.reply_text(text, reply_markup=reply_markup)
    else:
        await target.reply_text(text, reply_markup=reply_markup)


def _back_markup(student_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")]]
    )


def get_admin_visible_classes(
    student_id: str, student: Dict[str, Any], limit: int = 8
) -> List[str]:
    """Return class dates visible to admins.

    Admins should see the raw scheduled classes minus any that have already
    been logged as completed, cancelled or removed.  We intentionally ignore
    ``cancelled_dates`` here so that pending cancellations still appear until
    they are logged.
    """

    logs = data_store.load_logs()
    sid = str(student_id)
    logged: set[str] = set()
    for entry in logs:
        entry_sid = str(entry.get("student") or entry.get("student_id") or "")
        if entry_sid != sid:
            continue
        status = (entry.get("status") or entry.get("type") or "").lower()
        if status.startswith("class_"):
            status = status[6:]
        if status == "completed" or status == "removed" or status.startswith("cancelled"):
            dt = entry.get("date") or entry.get("at")
            if dt:
                logged.add(dt)

    dates = [dt for dt in sorted(student.get("class_dates", [])) if dt not in logged]
    return dates[:limit]


async def wrap_log_class(query, context, student_id: str, student: Dict[str, Any]):
    # Show classes that still need to be logged (past or future)
    visible = get_admin_visible_classes(student_id, student)
    if not visible:
        await safe_edit_or_send(
            query, "No upcoming classes to log", reply_markup=_back_markup(student_id)
        )
        return
    buttons = [
        [
            InlineKeyboardButton(
                fmt_class_label(dt), callback_data=f"cls:LOG:{student_id}:{dt}"
            )
        ]
        for dt in visible[:8]
    ]
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")])
    markup = InlineKeyboardMarkup(buttons)
    await safe_edit_or_send(query, "Select class to log:", reply_markup=markup)


async def wrap_cancel_class(query, context, student_id: str, student: Dict[str, Any]):
    visible = get_admin_visible_classes(student_id, student)
    if not visible:
        await safe_edit_or_send(
            query, "No upcoming classes to cancel", reply_markup=_back_markup(student_id)
        )
        return
    buttons = [
        [
            InlineKeyboardButton(
                fmt_class_label(dt), callback_data=f"cls:CANCEL:{student_id}:{dt}"
            )
        ]
        for dt in visible[:8]
    ]
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")])
    await safe_edit_or_send(
        query, "Select class to cancel:", reply_markup=InlineKeyboardMarkup(buttons)
    )


async def wrap_reschedule_class(query, context, student_id: str, student: Dict[str, Any]):
    visible = get_admin_visible_classes(student_id, student)
    if not visible:
        await safe_edit_or_send(
            query, "No upcoming classes to reschedule", reply_markup=_back_markup(student_id)
        )
        return
    buttons = [
        [
            InlineKeyboardButton(
                fmt_class_label(dt), callback_data=f"cls:RESHED:{student_id}:{dt}"
            )
        ]
        for dt in visible[:8]
    ]
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")])
    await safe_edit_or_send(
        query, "Select class to reschedule:", reply_markup=InlineKeyboardMarkup(buttons)
    )


async def wrap_pause_toggle(query, context, student_id: str, student: Dict[str, Any]):
    # Flip paused state, persist, log, and show updated detail view
    students = data_store.load_students()
    stu = students.get(str(student_id), {})
    new_value = not stu.get("paused", False)
    stu["paused"] = new_value
    students[str(student_id)] = stu
    data_store.save_students(students)
    data_store.append_log(
        {
            "type": "pause_toggled",
            "student_id": student_id,
            "new_value": new_value,
            "ts": datetime.utcnow().isoformat(),
        }
    )
    text, markup = keyboard_builders.build_student_detail_view(student_id, stu)
    await safe_edit_or_send(query, text, reply_markup=markup)


async def wrap_view_student(query, context, student_id: str, student: Dict[str, Any]):
    text, markup = keyboard_builders.build_student_detail_view(student_id, student)
    await safe_edit_or_send(query, text, reply_markup=markup)


def _is_cycle_finished(student: Dict[str, Any]) -> bool:
    now = datetime.now(timezone.utc)
    if student.get("classes_remaining", 0) != 0:
        return False
    for dt_str in student.get("class_dates", []):
        try:
            if datetime.fromisoformat(dt_str) > now:
                return False
        except Exception:
            continue
    return True


def _last_renewal_qty(student_id: str) -> int:
    logs = data_store.load_logs()
    for event in reversed(logs):
        if str(event.get("student_id")) == str(student_id) and event.get("type") == "renewal":
            try:
                qty = int(event.get("qty", 0))
            except Exception:
                qty = 0
            if qty > 0:
                return qty
    return 0


async def renew_start(query, context, student_id: str, student: Dict[str, Any]):
    if not _is_cycle_finished(student):
        await safe_edit_or_send(
            query,
            "Renewal is available only after the current set finishes.",
            reply_markup=_back_markup(student_id),
        )
        return
    text = (
        f"Renew classes for {student.get('name', student_id)}. "
        "Use same total as last set, or enter a new total?"
    )
    buttons = [
        [InlineKeyboardButton("Same total", callback_data=f"stu:RENEW_SAME:{student_id}")],
        [InlineKeyboardButton("Enter total", callback_data=f"stu:RENEW_ENTER:{student_id}")],
        [InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")],
    ]
    await safe_edit_or_send(query, text, reply_markup=InlineKeyboardMarkup(buttons))


async def renew_same(query, context, student_id: str, student: Dict[str, Any]):
    qty = _last_renewal_qty(student_id)
    if qty <= 0:
        await renew_ask_count(
            query,
            context,
            student_id,
            student,
            message="No previous total found. Enter total number of classes.",
        )
        return
    text = (
        f"New set for {student.get('name', student_id)}: {qty} classes. "
        "Schedule will follow your existing weekly pattern from the next suitable slot."
    )
    buttons = [
        [InlineKeyboardButton("Confirm", callback_data=f"cfm:RENEW:{student_id}:{qty}")],
        [InlineKeyboardButton("Cancel", callback_data=f"stu:VIEW:{student_id}")],
    ]
    await safe_edit_or_send(query, text, reply_markup=InlineKeyboardMarkup(buttons))


async def renew_ask_count(query, context, student_id: str, student: Dict[str, Any], message: str = None):
    if message is None:
        message = "Enter total number of classes for the new set (integer)."
    context.user_data["renew_waiting_for_qty"] = str(student_id)
    buttons = [[InlineKeyboardButton("Cancel", callback_data=f"stu:VIEW:{student_id}")]]
    await safe_edit_or_send(query, message, reply_markup=InlineKeyboardMarkup(buttons))


async def renew_received_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    student_id = context.user_data.get("renew_waiting_for_qty")
    if not student_id:
        return
    try:
        qty = int(update.message.text.strip())
        if qty <= 0:
            raise ValueError
    except Exception:
        await update.message.reply_text("Please send a positive integer.")
        return
    context.user_data.pop("renew_waiting_for_qty", None)
    student = data_store.resolve_student(student_id)
    if not student:
        await update.message.reply_text("Student not found.")
        return
    text = (
        f"New set for {student.get('name', student_id)}: {qty} classes. "
        "Schedule will follow your existing weekly pattern from the next suitable slot."
    )
    buttons = [
        [InlineKeyboardButton("Confirm", callback_data=f"cfm:RENEW:{student_id}:{qty}")],
        [InlineKeyboardButton("Cancel", callback_data=f"stu:VIEW:{student_id}")],
    ]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))


async def renew_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    match = re.match(r"^cfm:RENEW:(\d+):(\d+)$", query.data)
    if not match:
        return
    student_id, qty_str = match.groups()
    qty = int(qty_str)
    student = data_store.resolve_student(student_id)
    if not student:
        await safe_edit_or_send(query, "Student not found.")
        return
    if not _is_cycle_finished(student):
        text, markup = keyboard_builders.build_student_detail_view(student_id, student)
        await safe_edit_or_send(
            query,
            "Renewal is available only after the current set finishes.\n\n" + text,
            reply_markup=markup,
        )
        return

    logs = data_store.load_logs()
    history: List[datetime] = []
    for event in logs:
        if str(event.get("student_id")) != str(student_id):
            continue
        dt_str = None
        if event.get("type") == "class_completed":
            dt_str = event.get("at")
        elif event.get("type") == "class_cancelled":
            dt_str = event.get("at")
        elif event.get("type") == "class_rescheduled":
            dt_str = event.get("to")
        if dt_str:
            try:
                history.append(datetime.fromisoformat(dt_str))
            except Exception:
                continue
    history.sort()
    if not history:
        await safe_edit_or_send(
            query,
            "No prior weekly pattern found. Set a weekly schedule first.",
            reply_markup=_back_markup(student_id),
        )
        return
    last_dt = history[-1]
    pattern_dates = [dt.isoformat() for dt in history[-8:]]
    pattern = extract_weekly_pattern(pattern_dates)
    if not pattern:
        await safe_edit_or_send(
            query,
            "No prior weekly pattern found. Set a weekly schedule first.",
            reply_markup=_back_markup(student_id),
        )
        return
    generated = generate_from_pattern(last_dt, pattern, qty)
    if not generated:
        await safe_edit_or_send(
            query,
            "No prior weekly pattern found. Set a weekly schedule first.",
            reply_markup=_back_markup(student_id),
        )
        return

    students = data_store.load_students()
    stu = students.get(str(student_id), {})
    stu["class_dates"] = [dt.isoformat() for dt in generated]
    stu["classes_remaining"] = qty
    stu["renewal_date"] = generated[-1].isoformat()
    students[str(student_id)] = stu
    data_store.save_students(students)
    data_store.append_log(
        {
            "type": "renewal",
            "student_id": student_id,
            "qty": qty,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "schedule": "pattern_reused",
            "renewal_date": generated[-1].isoformat(),
        }
    )

    text, markup = keyboard_builders.build_student_detail_view(student_id, stu)
    msg = (
        f"Renewed {qty} for {stu.get('name', student_id)}. "
        f"New renewal date: {generated[-1].date().isoformat()}."
    )
    await safe_edit_or_send(query, f"{msg}\n\n{text}", reply_markup=markup)


async def initiate_change_length(query, context, student_id: str, student: Dict[str, Any]):
    await safe_edit_or_send(
        query,
        f"Coming soon: change length for {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


async def initiate_edit_schedule(query, context, student_id: str, student: Dict[str, Any]):
    await safe_edit_or_send(
        query,
        f"Coming soon: edit schedule for {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


async def initiate_free_credit(query, context, student_id: str, student: Dict[str, Any]):
    await safe_edit_or_send(
        query,
        f"Coming soon: free credit for {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


async def initiate_remove_student(query, context, student_id: str, student: Dict[str, Any]):
    await safe_edit_or_send(
        query,
        f"Coming soon: remove student {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


async def initiate_adhoc_class(query, context, student_id: str, student: Dict[str, Any]):
    await safe_edit_or_send(
        query,
        f"Coming soon: adhoc class for {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


actions_map: Dict[str, Callable] = {
    "LOG": wrap_log_class,
    "CANCEL": wrap_cancel_class,
    "RESHED": wrap_reschedule_class,
    "RENEW": renew_start,
    "RENEW_SAME": renew_same,
    "RENEW_ENTER": renew_ask_count,
    "LENGTH": initiate_change_length,
    "EDIT": initiate_edit_schedule,
    "FREECREDIT": initiate_free_credit,
    "PAUSE": wrap_pause_toggle,
    "REMOVE": initiate_remove_student,
    "VIEW": wrap_view_student,
    "ADHOC": initiate_adhoc_class,
}


async def handle_student_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dispatch student submenu actions based on callback_data."""
    query = update.callback_query
    if not query:
        return
    await query.answer()
    match = re.match(
        r"^stu:(LOG|CANCEL|RESHED|RENEW|RENEW_SAME|RENEW_ENTER|LENGTH|EDIT|FREECREDIT|PAUSE|REMOVE|VIEW|ADHOC):(\d+)$",
        query.data,
    )
    if not match:
        return
    action, student_id = match.group(1), match.group(2)
    student = data_store.resolve_student(student_id)
    if not student:
        await safe_edit_or_send(query, "Student not found.")
        return
    handler = actions_map.get(action)
    if handler:
        await handler(query, context, student_id, student)


async def handle_class_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle selection of a specific class for log/cancel/reschedule."""
    query = update.callback_query
    if not query:
        return
    await query.answer()
    match = re.match(r"^cls:(LOG|CANCEL|RESHED):(\d+):(.+)$", query.data)
    if not match:
        return
    action, student_id, iso_dt = match.groups()
    student = data_store.resolve_student(student_id)
    if not student or iso_dt not in student.get("class_dates", []):
        await safe_edit_or_send(query, "Class not found.", reply_markup=_back_markup(student_id))
        return

    if action == "LOG":
        text = f"Log class at {iso_dt}?"
        confirm = f"cfm:LOG:{student_id}:{iso_dt}"
        buttons = [
            [InlineKeyboardButton("Confirm", callback_data=confirm)],
            [InlineKeyboardButton("Cancel", callback_data=f"stu:VIEW:{student_id}")],
        ]
        await safe_edit_or_send(query, text, reply_markup=InlineKeyboardMarkup(buttons))
    elif action == "CANCEL":
        text = f"Cancel class at {iso_dt}?"
        confirm = f"cfm:CANCEL:{student_id}:{iso_dt}"
        buttons = [
            [InlineKeyboardButton("Confirm", callback_data=confirm)],
            [InlineKeyboardButton("Back", callback_data=f"stu:VIEW:{student_id}")],
        ]
        await safe_edit_or_send(query, text, reply_markup=InlineKeyboardMarkup(buttons))
    elif action == "RESHED":
        text = f"Reschedule class at {iso_dt}. Choose new time:"
        buttons = [
            [
                InlineKeyboardButton(
                    "+1h", callback_data=f"cfm:RESHED:{student_id}:{iso_dt}|AUTO:+1h"
                )
            ],
            [
                InlineKeyboardButton(
                    "Tomorrow same time",
                    callback_data=f"cfm:RESHED:{student_id}:{iso_dt}|AUTO:tomorrow",
                )
            ],
            [InlineKeyboardButton("Cancel", callback_data=f"stu:VIEW:{student_id}")],
        ]
        await safe_edit_or_send(query, text, reply_markup=InlineKeyboardMarkup(buttons))


async def handle_class_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle confirmation callbacks for class operations."""
    query = update.callback_query
    if not query:
        return
    await query.answer()
    match = re.match(r"^cfm:(LOG|CANCEL|RESHED):(\d+):(.+)$", query.data)
    if not match:
        return
    action, student_id, payload = match.groups()
    student = data_store.resolve_student(student_id)
    if not student:
        await safe_edit_or_send(query, "Student not found.")
        return

    if action == "LOG":
        iso_dt = payload
        data_store.mark_class_completed(student_id, iso_dt)
        msg = f"Class at {iso_dt} logged."
    elif action == "CANCEL":
        iso_dt = payload
        cutoff = student.get("cutoff_hours", 24)
        data_store.cancel_single_class(student_id, iso_dt, cutoff)
        msg = f"Class at {iso_dt} cancelled."
    elif action == "RESHED":
        if "|" in payload:
            iso_dt, extra = payload.split("|", 1)
        else:
            iso_dt, extra = payload, ""
        new_iso = iso_dt
        if extra.startswith("AUTO:"):
            option = extra.split("AUTO:", 1)[1]
            old_dt = datetime.fromisoformat(iso_dt)
            if option == "+1h":
                new_dt = old_dt + timedelta(hours=1)
            elif option == "tomorrow":
                new_dt = old_dt + timedelta(days=1)
            else:
                new_dt = old_dt
            new_iso = new_dt.isoformat()
        data_store.reschedule_single_class(student_id, iso_dt, new_iso)
        msg = f"Class moved from {iso_dt} to {new_iso}."
    else:
        return

    updated = data_store.resolve_student(student_id) or {}
    text, markup = keyboard_builders.build_student_detail_view(student_id, updated)
    await safe_edit_or_send(query, f"{msg}\n\n{text}", reply_markup=markup)
