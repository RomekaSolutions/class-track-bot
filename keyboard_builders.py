from typing import Dict, Any, Tuple
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

try:  # pragma: no cover - circular import guard during module init
    from class_track_bot import fmt_bkk, get_admin_future_classes
except ImportError:  # pragma: no cover
    def fmt_bkk(dt, add_label: bool = False):
        from class_track_bot import fmt_bkk as _fmt_bkk

        return _fmt_bkk(dt, add_label=add_label)

    def get_admin_future_classes(student, include_cancelled: bool = False):
        from class_track_bot import get_admin_future_classes as _helper

        return _helper(student, include_cancelled=include_cancelled)


def build_student_submenu(student_id: str) -> InlineKeyboardMarkup:
    """Return the admin submenu for a student.

    Callback data strings follow the ``stu:<ACTION>:<id>`` convention.
    """
    student_id = str(student_id)
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
    """Return a detailed summary for ``student`` and the admin submenu."""

    name = student.get("name", student_id)
    if student.get("needs_id") and student.get("telegram_mode", True):
        name += " (needs ID)"
    remaining = student.get("classes_remaining", 0)

    # Upcoming class dates – show at most the next three entries
    class_dates = get_admin_future_classes(student, include_cancelled=False)
    upcoming = class_dates[:3]

    paused = student.get("paused", False)

    lines = [f"Student: {name}", f"Classes remaining: {remaining}"]

    if upcoming:
        lines.append("Upcoming classes:")
        for dt in upcoming:
            lines.append(f" - {fmt_bkk(dt)}")
    else:
        lines.append("No upcoming classes")

    lines.append(f"Paused: {'Yes' if paused else 'No'}")

    text = "\n".join(lines)
    submenu = build_student_submenu(student_id)
    keyboard = [row[:] for row in submenu.inline_keyboard]
    if student.get("telegram_mode") is False:
        keyboard.append(
            [
                InlineKeyboardButton(
                    "📱 Connect to Telegram", callback_data=f"stu:CONNECT:{student_id}"
                )
            ]
        )
    return text, InlineKeyboardMarkup(keyboard)
