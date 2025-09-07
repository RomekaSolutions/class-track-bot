import re
from typing import Dict, Any, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

import data_store
import keyboard_builders


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
    await safe_edit_or_send(
        query,
        f"Coming soon: log class for {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


async def wrap_cancel_class(query, context, student_id: str, student: Dict[str, Any]):
    await safe_edit_or_send(
        query,
        f"Coming soon: cancel class for {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


async def wrap_reschedule_class(query, context, student_id: str, student: Dict[str, Any]):
    await safe_edit_or_send(
        query,
        f"Coming soon: reschedule class for {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


async def wrap_pause_toggle(query, context, student_id: str, student: Dict[str, Any]):
    await safe_edit_or_send(
        query,
        f"Coming soon: pause/resume for {student.get('name', student_id)}",
        reply_markup=_back_markup(student_id),
    )


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
