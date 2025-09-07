import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

import data_store
import keyboard_builders
from helpers import fmt_class_label


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


async def wrap_log_class(query, context, student_id: str, student: Dict[str, Any]):
    # Show upcoming classes for logging
    now = datetime.now(timezone.utc)
    future = [
        dt for dt in sorted(student.get("class_dates", [])) if datetime.fromisoformat(dt) >= now
    ]
    if not future:
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
        for dt in future[:8]
    ]
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")])
    markup = InlineKeyboardMarkup(buttons)
    await safe_edit_or_send(query, "Select class to log:", reply_markup=markup)


async def wrap_cancel_class(query, context, student_id: str, student: Dict[str, Any]):
    now = datetime.now(timezone.utc)
    future = [
        dt for dt in sorted(student.get("class_dates", [])) if datetime.fromisoformat(dt) >= now
    ]
    if not future:
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
        for dt in future[:8]
    ]
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")])
    await safe_edit_or_send(
        query, "Select class to cancel:", reply_markup=InlineKeyboardMarkup(buttons)
    )


async def wrap_reschedule_class(query, context, student_id: str, student: Dict[str, Any]):
    now = datetime.now(timezone.utc)
    future = [
        dt for dt in sorted(student.get("class_dates", [])) if datetime.fromisoformat(dt) >= now
    ]
    if not future:
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
        for dt in future[:8]
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


async def initiate_renewal(query, context, student_id: str, student: Dict[str, Any]):
    await safe_edit_or_send(
        query,
        f"Coming soon: renewal for {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


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
    "RENEW": initiate_renewal,
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
        r"^stu:(LOG|CANCEL|RESHED|RENEW|LENGTH|EDIT|FREECREDIT|PAUSE|REMOVE|VIEW|ADHOC):(\d+)$",
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
