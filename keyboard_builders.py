from typing import Dict, Any, Tuple
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def build_student_submenu(student_id: str) -> InlineKeyboardMarkup:
    """Return the admin submenu for a student.

    Callback data strings follow the ``stu:<ACTION>:<id>`` convention.
    """
    buttons = [
        [
            InlineKeyboardButton("✅ Log Class", callback_data=f"stu:LOG:{student_id}"),
            InlineKeyboardButton("❌ Cancel Class", callback_data=f"stu:CANCEL:{student_id}"),
        ],
        [
            InlineKeyboardButton("🔄 Reschedule Class", callback_data=f"stu:RESHED:{student_id}"),
            InlineKeyboardButton("💰 Renew Plan", callback_data=f"stu:RENEW:{student_id}"),
        ],
        [
            InlineKeyboardButton("⏱ Change Class Length", callback_data=f"stu:LENGTH:{student_id}"),
            InlineKeyboardButton("📅 Edit Weekly Schedule", callback_data=f"stu:EDIT:{student_id}"),
        ],
        [
            InlineKeyboardButton("🎁 Award Free Credit", callback_data=f"stu:FREECREDIT:{student_id}"),
            InlineKeyboardButton("⏸ Pause / Resume", callback_data=f"stu:PAUSE:{student_id}"),
        ],
        [
            InlineKeyboardButton("🗑 Remove Student", callback_data=f"stu:REMOVE:{student_id}"),
            InlineKeyboardButton("👁 View Student", callback_data=f"stu:VIEW:{student_id}"),
        ],
        [
            InlineKeyboardButton("➕ Ad-hoc Class", callback_data=f"stu:ADHOC:{student_id}"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


def build_student_detail_view(student_id: str, student: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    """Return basic student details and the associated submenu."""
    text = f"Student: {student.get('name', student_id)}"
    return text, build_student_submenu(student_id)
