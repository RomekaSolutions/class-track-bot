import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Callable, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

try:
    from telegram.error import BadRequest
except Exception:  # pragma: no cover - fallback when telegram is unavailable

    class BadRequest(Exception):
        """Fallback BadRequest exception for optional telegram dependency."""


import data_store
import keyboard_builders
from data_store import load_logs, save_logs, save_students
from helpers import (
    fmt_class_label,
    generate_from_pattern,
    get_weekly_pattern_from_history,
    slots_to_text,
    Slot,
)


try:
    from class_track_bot import (
        BASE_TZ,
        get_admin_future_classes,
        parse_day_time,
        resolve_student,
        WEEKDAY_MAP,
    )  # type: ignore
except Exception:  # pragma: no cover - fallback for circular import during init
    BASE_TZ = timezone(timedelta(hours=7))

    def get_admin_future_classes(student, include_cancelled: bool = False):
        class_dates = student.get("class_dates", [])
        if not isinstance(class_dates, list):
            return []
        if include_cancelled:
            return sorted(str(item) for item in class_dates if item)
        cancelled = student.get("cancelled_dates", [])
        if not isinstance(cancelled, list):
            cancelled = []
        cancelled_set = {str(item) for item in cancelled if item}
        return sorted(
            str(item)
            for item in class_dates
            if item and str(item) not in cancelled_set
        )

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

    def parse_day_time(text: str):
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

    def resolve_student(students: Dict[str, Any], key: str):
        student = students.get(key)
        if student:
            return key, student
        normalised = str(key).lstrip("@").lower()
        for skey, stu in students.items():
            handle = str(stu.get("telegram_handle", "")).lstrip("@").lower()
            if normalised == str(skey).lstrip("@").lower() or normalised == handle:
                return skey, stu
        return None, None

STUDENT_NOT_FOUND_MSG = (
    "❌ This student was not found — they may have been removed or renamed."
)


async def safe_edit_or_send(target, text: str, reply_markup=None) -> None:
    """Edit message if possible, otherwise send a new message."""
    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(text, reply_markup=reply_markup)
    elif hasattr(target, "message") and target.message:
        await target.message.reply_text(text, reply_markup=reply_markup)
    else:
        await target.reply_text(text, reply_markup=reply_markup)


async def _answer_with_alert(query, text: str) -> None:
    """Safely answer a callback with ``show_alert`` when supported."""

    async def _answer_without_alert() -> None:
        try:
            await query.answer(text)
        except TypeError:
            try:
                await query.answer()
            except TypeError as exc:  # pragma: no cover - unexpected signature
                logging.warning("Failed to answer callback (no-arg TypeError): %s", exc)
            except BadRequest as exc:  # pragma: no cover - depends on telegram
                logging.warning("Failed to answer callback (no-arg): %s", exc)
                await safe_edit_or_send(query, text)
        except BadRequest as exc:  # pragma: no cover - depends on telegram
            logging.warning("Failed to answer callback (text): %s", exc)
            await safe_edit_or_send(query, text)

    try:
        await query.answer(text, show_alert=True)
    except TypeError:
        await _answer_without_alert()
    except BadRequest as exc:  # pragma: no cover - depends on telegram
        logging.warning("Failed to answer callback with alert: %s", exc)
        await safe_edit_or_send(query, text)


def _back_markup(student_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")]]
    )


def _parse_iso(dt_str: str) -> datetime:
    """Convert ``dt_str`` to a timezone-aware ``datetime``."""
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _format_display_dt(iso_str: str) -> str:
    """Return a human-friendly representation of ``iso_str`` for admins."""

    try:
        dt = _parse_iso(iso_str).astimezone(BASE_TZ)
    except Exception:
        return iso_str
    return dt.strftime("%a %d %b %H:%M")


def get_admin_visible_classes(
    student_id: str, student: Dict[str, Any], limit: int = 8
) -> List[str]:
    """Return past class dates that still need logging.

    This powers the **Log Class** menu and excludes any class already logged
    as completed, cancelled, rescheduled or removed.
    """

    logs = data_store.load_logs()
    now = datetime.now(timezone.utc)
    visible: List[str] = []
    for dt in get_admin_future_classes(student, include_cancelled=True):
        try:
            aware = _parse_iso(dt)
        except Exception:
            continue
        if aware > now:
            continue
        if data_store.is_class_logged(student_id, dt, logs):
            continue
        visible.append(dt)
    return visible[:limit]


def get_admin_upcoming_classes(
    student_id: str, student: Dict[str, Any], limit: int = 8
) -> List[str]:
    """Return upcoming class dates with no existing logs."""

    logs = data_store.load_logs()
    now = datetime.now(timezone.utc)
    dates: List[str] = []
    for dt in get_admin_future_classes(student, include_cancelled=False):
        try:
            aware = _parse_iso(dt)
        except Exception:
            continue
        if aware <= now:
            continue
        if data_store.is_class_logged(student_id, dt, logs):
            continue
        dates.append(dt)
    return dates[:limit]


async def wrap_log_class(query, context, student_id: str, student: Dict[str, Any]):
    """Show past classes that still need to be logged."""
    visible = get_admin_visible_classes(student_id, student, limit=9999)
    if not visible:
        await safe_edit_or_send(
            query,
            "No unlogged past classes",
            reply_markup=_back_markup(student_id),
        )
        return
    buttons = [
        [
            InlineKeyboardButton(
                fmt_class_label(dt), callback_data=f"cls:LOG:{student_id}:{dt}"
            )
        ]
        for dt in visible
    ]
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")])
    markup = InlineKeyboardMarkup(buttons)
    await safe_edit_or_send(query, "Select class to log:", reply_markup=markup)


async def wrap_cancel_class(query, context, student_id: str, student: Dict[str, Any]):
    visible = get_admin_upcoming_classes(student_id, student)
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
    visible = get_admin_upcoming_classes(student_id, student)
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
    try:
        if not isinstance(student.get("class_dates"), list) or "classes_remaining" not in student:
            raise KeyError
        text, markup = keyboard_builders.build_student_detail_view(student_id, student)
    except Exception:
        await safe_edit_or_send(
            query,
            "Student record incomplete — recheck or renew manually",
            reply_markup=_back_markup(student_id),
        )
        return
    await safe_edit_or_send(query, text, reply_markup=markup)


def validate_student_record(student: Dict[str, Any]) -> Tuple[bool, str]:
    """Return ``(True, "")`` if ``student`` structure looks valid.

    Validation ensures ``class_dates`` is a non-empty list,
    ``classes_remaining`` is a positive integer and ``cancelled_dates``
    exists as a list.  On failure ``False`` and a short explanation are
    returned.
    """

    dates = student.get("class_dates")
    if not isinstance(dates, list) or not dates:
        return False, "class_dates must be a non-empty list"
    remaining = student.get("classes_remaining")
    if not isinstance(remaining, int) or remaining <= 0:
        return False, "classes_remaining must be a positive integer"
    cancelled = student.get("cancelled_dates")
    if not isinstance(cancelled, list):
        return False, "cancelled_dates must be a list"
    return True, ""


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


def _history_and_pattern(
    student_id: str,
) -> Tuple[List[datetime], Optional[List[Slot]]]:
    """Return class history and detected weekly pattern for ``student_id``."""

    student = data_store.get_student_by_id(student_id)
    schedule_slots: Optional[List[Slot]] = None
    if student:
        raw_pattern = student.get("schedule_pattern")
        entries: List[str] = []
        if isinstance(raw_pattern, str):
            entries = [item.strip() for item in raw_pattern.split(",") if item.strip()]
        elif isinstance(raw_pattern, list):
            entries = [str(item).strip() for item in raw_pattern if str(item).strip()]
        if entries:
            slots: List[Slot] = []
            seen = set()
            invalid_entry: Optional[str] = None
            for entry in entries:
                normalized = parse_day_time(entry)
                if not normalized:
                    invalid_entry = entry
                    break
                day_name, time_part = normalized.split()
                weekday_idx = WEEKDAY_MAP.get(day_name.lower())
                if weekday_idx is None:
                    invalid_entry = entry
                    break
                try:
                    hour, minute = map(int, time_part.split(":"))
                except ValueError:
                    invalid_entry = entry
                    break
                key = (weekday_idx, hour, minute, BASE_TZ)
                if key not in seen:
                    seen.add(key)
                    slots.append(key)
            if invalid_entry:
                logging.warning(
                    "Invalid schedule_pattern entry '%s' for student %s: %s",
                    invalid_entry,
                    student_id,
                    raw_pattern,
                )
            elif slots:
                slots.sort(key=lambda x: (x[0], x[1], x[2]))
                schedule_slots = slots

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
        elif event.get("status") in {"completed", "cancelled"}:
            dt_str = event.get("date")
        if dt_str:
            try:
                history.append(_parse_iso(dt_str))
            except Exception:
                continue
    history.sort()
    if schedule_slots is not None:
        return history, schedule_slots

    pattern = get_weekly_pattern_from_history(history)
    if not pattern and student and isinstance(student.get("class_dates"), list):
        dates: List[datetime] = []
        for iso in student.get("class_dates", []):
            try:
                dates.append(_parse_iso(iso))
            except Exception:
                continue
        pattern = get_weekly_pattern_from_history(dates)
    return history, pattern


async def renew_start(query, context, student_id: str, student: Dict[str, Any]):
    if not _is_cycle_finished(student):
        await safe_edit_or_send(
            query,
            "Renewal is available only after the current set finishes.",
            reply_markup=_back_markup(student_id),
        )
        return
    last_qty = _last_renewal_qty(student_id)
    same_text = f"Same total ({last_qty})" if last_qty > 0 else "Same total"
    text = (
        f"Renew classes for {student.get('name', student_id)}. "
        "Use same total as last set, or enter a new total?"
    )
    buttons = [
        [InlineKeyboardButton(same_text, callback_data=f"stu:RENEW_SAME:{student_id}")],
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
    _, pattern = _history_and_pattern(student_id)
    if not pattern:
        await safe_edit_or_send(
            query,
            "No prior weekly pattern found. Set a weekly schedule first.",
            reply_markup=_back_markup(student_id),
        )
        return
    schedule = slots_to_text(pattern)
    text = (
        f"New set for {student.get('name', student_id)}: {qty} classes. "
        f"Schedule: {schedule}"
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
    student = data_store.get_student_by_id(student_id)
    if not student:
        await update.message.reply_text(STUDENT_NOT_FOUND_MSG)
        return
    _, pattern = _history_and_pattern(student_id)
    if not pattern:
        await update.message.reply_text(
            "No prior weekly pattern found. Set a weekly schedule first.",
            reply_markup=_back_markup(student_id),
        )
        return
    schedule = slots_to_text(pattern)
    text = (
        f"New set for {student.get('name', student_id)}: {qty} classes. "
        f"Schedule: {schedule}"
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
    data = query.data or ""
    match = re.match(r"^cfm:RENEW:([^:]+):(\d+)$", data)
    if not match:
        logging.warning("Malformed renew confirmation callback: %s", data)
        await _answer_with_alert(query, "Invalid renewal request.")
        return
    raw_student, qty_str = match.groups()
    try:
        qty = int(qty_str)
    except ValueError:
        logging.warning("Invalid renewal quantity in callback: %s", data)
        await _answer_with_alert(query, "Invalid renewal request.")
        return
    if qty <= 0:
        logging.warning("Non-positive renewal quantity for callback: %s", data)
        await _answer_with_alert(query, "Invalid renewal request.")
        return
    students = data_store.load_students()
    student_id, student = resolve_student(students, raw_student)
    if not student:
        logging.warning("Unable to resolve student %s for renewal confirmation", raw_student)
        await _answer_with_alert(query, "Student not found")
        return
    student_id = str(student_id)
    await query.answer()
    if not _is_cycle_finished(student):
        text, markup = keyboard_builders.build_student_detail_view(student_id, student)
        await safe_edit_or_send(
            query,
            "Renewal is available only after the current set finishes.\n\n" + text,
            reply_markup=markup,
        )
        return

    history, pattern = _history_and_pattern(student_id)
    if not history or not pattern:
        await safe_edit_or_send(
            query,
            "No prior weekly pattern found. Set a weekly schedule first.",
            reply_markup=_back_markup(student_id),
        )
        return
    last_dt = history[-1]
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
    stu["cancelled_dates"] = stu.get("cancelled_dates", [])
    valid, reason = validate_student_record(stu)
    if not valid:
        logging.warning(
            "Rejecting renewal for %s due to invalid student record: %s",
            student_id,
            reason,
        )
        await safe_edit_or_send(
            query,
            f"Student record invalid: {reason}",
            reply_markup=_back_markup(student_id),
        )
        return
    students[str(student_id)] = stu
    data_store.save_students(students)
    data_store.append_log(
        {
            "type": "renewal",
            "student_id": student_id,
            "qty": qty,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "schedule": "pattern_reused",
        }
    )

    try:
        from class_track_bot import (
            schedule_student_reminders,
            send_low_balance_if_threshold,
            schedule_final_set_notice,
        )
    except Exception as exc:  # pragma: no cover - only triggered in test stubs
        logging.warning(
            "Skipping reminder scheduling for %s due to import error: %s",
            student_id,
            exc,
        )
    else:
        # In test contexts, ContextTypes.DEFAULT_TYPE may be a dummy without .application
        if not hasattr(context, "application"):
            return
        schedule_student_reminders(context.application, student_id, stu)
        await send_low_balance_if_threshold(context.application, student_id, stu)
        schedule_final_set_notice(context.application, student_id, stu)

    text, markup = keyboard_builders.build_student_detail_view(student_id, stu)
    msg = (
        f"Renewed {qty} for {stu.get('name', student_id)}. "
        f"Set ends: {generated[-1].date().isoformat()}."
    )
    await safe_edit_or_send(query, f"{msg}\n\n{text}", reply_markup=markup)


async def initiate_change_length(query, context, student_id: str, student: Dict[str, Any]):
    """Delegate to the existing /edit flow to change class length."""
    # Import locally to avoid circular imports at module load time
    import types
    from class_track_bot import edit_menu_callback

    # Reuse the /edit student logic by faking an ``edit:option:length`` callback
    context.user_data["edit_student_key"] = student_id
    fake_query = types.SimpleNamespace(
        data="edit:option:length",
        answer=query.answer,
        edit_message_text=query.edit_message_text,
        message=query.message,
    )
    fake_update = types.SimpleNamespace(callback_query=fake_query, effective_user=query.from_user)
    await edit_menu_callback(fake_update, context)


async def initiate_edit_schedule(query, context, student_id: str, student: Dict[str, Any]):
    """Show the /edit menu for the chosen student."""
    import types
    from class_track_bot import edit_pick_callback

    # Emulate selecting the student through the standard edit workflow
    fake_query = types.SimpleNamespace(
        data=f"edit:pick:{student_id}",
        answer=query.answer,
        edit_message_text=query.edit_message_text,
        message=query.message,
    )
    fake_update = types.SimpleNamespace(callback_query=fake_query, effective_user=query.from_user)
    await edit_pick_callback(fake_update, context)


async def initiate_free_credit(query, context, student_id: str, student: Dict[str, Any]):
    """Award a free class credit using existing logic."""
    from class_track_bot import initiate_award_free

    await initiate_award_free(query, context, student_id, student)


async def initiate_remove_student(query, context, student_id: str, student: Dict[str, Any]):
    """Remove a student by delegating to the existing command logic."""
    import types
    from class_track_bot import remove_student_command

    # ``remove_student_command`` expects command-style arguments and a message
    old_args = getattr(context, "args", None)
    context.args = [student_id, "confirm"]
    fake_update = types.SimpleNamespace(
        message=query.message,
        effective_user=query.from_user,
    )
    await remove_student_command(fake_update, context)
    # Restore context.args to its previous state
    if old_args is None:
        delattr(context, "args")
    else:
        context.args = old_args


async def initiate_adhoc_class(query, context, student_id: str, student: Dict[str, Any]):
    """Use one class credit for an adhoc/extra hour session."""

    students = data_store.load_students()
    student = students.get(student_id)
    if not student:
        return await safe_edit_or_send(query, "Student not found.")

    # Check if student has classes remaining
    classes_remaining = student.get("classes_remaining", 0)
    if classes_remaining <= 0:
        return await safe_edit_or_send(
            query,
            f"❌ {student.get('name', student_id)} has no classes remaining.",
            reply_markup=_back_markup(student_id),
        )

    # Subtract 1 from classes_remaining (that's all we do)
    student["classes_remaining"] = classes_remaining - 1

    save_students(students)

    # Log the action
    logs = load_logs()
    logs.append({
        "student": student_id,
        "date": datetime.now(BASE_TZ).isoformat(),
        "status": "adhoc_class_used",
        "note": "admin used class credit for adhoc/extra hour",
        "admin": query.from_user.id if query.from_user else None,
    })
    save_logs(logs)

    text = f"✅ Used 1 class credit for {student.get('name', student_id)}\n"
    text += f"Remaining classes: {student['classes_remaining']}"

    await safe_edit_or_send(query, text, reply_markup=_back_markup(student_id))


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
    logging.debug("handle_student_action data=%s", query.data)
    data = query.data or ""
    if not data.startswith("stu:"):
        return
    try:
        _, action, student_id = data.split(":", 2)
    except ValueError:
        logging.warning("Malformed student action callback: %s", data)
        return
    student = data_store.get_student_by_id(student_id)
    if not student:
        await safe_edit_or_send(query, STUDENT_NOT_FOUND_MSG)
        return
    handler = actions_map.get(action)
    if handler:
        await handler(query, context, student_id, student)
    else:
        logging.warning("Unhandled student action: %s", query.data)


# Callback handler for "cls:*" class selection buttons.
async def handle_class_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle selection of a specific class from "cls:*" buttons."""
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    match = re.match(r"^cls:(LOG|CANCEL|RESHED):([^:]+):(.+)$", data)
    if not match:
        logging.warning("Malformed class selection callback: %s", data)
        await _answer_with_alert(query, "Invalid selection")
        return
    action, raw_student, iso_dt = match.groups()
    students = data_store.load_students()
    student_id, student = resolve_student(students, raw_student)
    if not student:
        logging.warning("Unable to resolve student %s for class selection", raw_student)
        await _answer_with_alert(query, "Student not found")
        return
    student_id = str(student_id)
    if iso_dt not in student.get("class_dates", []):
        await query.answer()
        await safe_edit_or_send(query, "Class not found.", reply_markup=_back_markup(student_id))
        return

    await query.answer()
    if action == "LOG":
        has_log = data_store.is_class_logged(student_id, iso_dt)
        buttons = [
            [
                InlineKeyboardButton(
                    "✅ Completed",
                    callback_data=f"log:COMPLETE:{student_id}:{iso_dt}",
                )
            ],
            [
                InlineKeyboardButton(
                    "❌ Cancelled (Early)",
                    callback_data=f"log:CANCEL_EARLY:{student_id}:{iso_dt}",
                )
            ],
            [
                InlineKeyboardButton(
                    "❌ Cancelled (Late)",
                    callback_data=f"log:CANCEL_LATE:{student_id}:{iso_dt}",
                )
            ],
            [
                InlineKeyboardButton(
                    "🔁 Rescheduled",
                    callback_data=f"log:RESCHEDULED:{student_id}:{iso_dt}",
                )
            ],
        ]
        if has_log:
            buttons.append(
                [
                    InlineKeyboardButton(
                        "🔓 Unlog Class",
                        callback_data=f"log:UNLOG:{student_id}:{iso_dt}",
                    )
                ]
            )
        buttons.append(
            [InlineKeyboardButton("⬅ Back", callback_data=f"stu:VIEW:{student_id}")]
        )
        await safe_edit_or_send(
            query,
            f"Log class at {_format_display_dt(iso_dt)}:",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    elif action == "CANCEL":
        text = f"Cancel class at {_format_display_dt(iso_dt)}?"
        confirm = f"cfm:CANCEL:{student_id}:{iso_dt}"
        buttons = [
            [InlineKeyboardButton("Confirm", callback_data=confirm)],
            [InlineKeyboardButton("Back", callback_data=f"stu:VIEW:{student_id}")],
        ]
        await safe_edit_or_send(query, text, reply_markup=InlineKeyboardMarkup(buttons))
    elif action == "RESHED":
        text = f"Reschedule class at {_format_display_dt(iso_dt)}. Choose new time:"
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
    """Handle confirmation callbacks for "cfm:*" buttons."""
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    match = re.match(r"^cfm:(CANCEL|RESHED):([^:]+):(.+)$", data)
    if not match:
        logging.warning("Malformed class confirmation callback: %s", data)
        await _answer_with_alert(query, "Invalid confirmation")
        return
    action, raw_student, payload = match.groups()
    students = data_store.load_students()
    student_id, student = resolve_student(students, raw_student)
    if not student:
        logging.warning("Unable to resolve student %s for class confirmation", raw_student)
        await _answer_with_alert(query, "Student not found")
        return
    student_id = str(student_id)

    await query.answer()


    if action == "CANCEL":
        iso_dt = payload
        cutoff = student.get("cutoff_hours", 24)
        data_store.cancel_single_class(student_id, iso_dt, cutoff)
        student = data_store.get_student_by_id(student_id)
        from class_track_bot import (
            schedule_student_reminders,
            send_low_balance_if_threshold,
            schedule_final_set_notice,
        )
        if not hasattr(context, "application"):
            return
        if not hasattr(context, "application"):
            return
        schedule_student_reminders(context.application, student_id, student)
        await send_low_balance_if_threshold(context.application, student_id, student)
        schedule_final_set_notice(context.application, student_id, student)
        msg = f"Class at {_format_display_dt(iso_dt)} cancelled."
        await safe_edit_or_send(query, msg, reply_markup=_back_markup(student_id))
        return
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
        if not data_store.reschedule_single_class(student_id, iso_dt, new_iso):
            await safe_edit_or_send(
                query, "Failed to reschedule class.", reply_markup=_back_markup(student_id)
            )
            return
        student = data_store.get_student_by_id(student_id)
        from class_track_bot import (
            schedule_student_reminders,
            schedule_final_set_notice,
        )
        if not hasattr(context, "application"):
            return
        schedule_student_reminders(context.application, student_id, student)
        schedule_final_set_notice(context.application, student_id, student)
        text, markup = keyboard_builders.build_student_detail_view(student_id, student)
        msg = (
            f"Class moved from {_format_display_dt(iso_dt)} to "
            f"{_format_display_dt(new_iso)}.\n\n{text}"
        )
        await safe_edit_or_send(query, msg, reply_markup=markup)
        return
    else:
        return


# ---------------------------------------------------------------------------
# Callback handler for "log:*" buttons shown after selecting a class to log.
# ---------------------------------------------------------------------------
async def handle_log_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle logging status selections."""
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    match = re.match(
        r"^log:(COMPLETE|CANCEL_EARLY|CANCEL_LATE|RESCHEDULED|UNLOG):([^:]+):(.+)$",
        data,
    )
    if not match:
        logging.warning("Malformed log action callback: %s", data)
        await _answer_with_alert(query, "Invalid log action")
        return
    action, raw_student, iso_dt = match.groups()
    students = data_store.load_students()
    student_id, student = resolve_student(students, raw_student)
    if not student:
        logging.warning("Unable to resolve student %s for log action", raw_student)
        await _answer_with_alert(query, "Student not found")
        return
    student_id = str(student_id)

    await query.answer()

    if action == "UNLOG":
        removed = data_store.remove_class_log(student_id, iso_dt)
        if removed:
            msg = f"Log removed for {_format_display_dt(iso_dt)}."
        else:
            msg = "No matching log entry found."
    elif action == "COMPLETE":
        data_store.mark_class_completed(student_id, iso_dt)
        student = data_store.get_student_by_id(student_id)
        from class_track_bot import (
            schedule_student_reminders,
            send_low_balance_if_threshold,
            schedule_final_set_notice,
        )
        if context and hasattr(context, "application"):
            schedule_student_reminders(context.application, student_id, student)
            await send_low_balance_if_threshold(context.application, student_id, student)
            schedule_final_set_notice(context.application, student_id, student)
        msg = f"Class at {_format_display_dt(iso_dt)} logged as completed."
    elif action in {"CANCEL_EARLY", "CANCEL_LATE"}:
        cutoff = 99999 if action == "CANCEL_EARLY" else 0
        data_store.cancel_single_class(student_id, iso_dt, cutoff)
        student = data_store.get_student_by_id(student_id)
        from class_track_bot import (
            schedule_student_reminders,
            send_low_balance_if_threshold,
            schedule_final_set_notice,
        )
        if context and hasattr(context, "application"):
            schedule_student_reminders(context.application, student_id, student)
            await send_low_balance_if_threshold(context.application, student_id, student)
            schedule_final_set_notice(context.application, student_id, student)
        label = "cancelled late" if action == "CANCEL_LATE" else "cancelled early"
        msg = f"Class at {_format_display_dt(iso_dt)} logged as {label}."
    else:
        data_store.log_class_status(student_id, iso_dt, "rescheduled")
        msg = f"Class at {_format_display_dt(iso_dt)} logged as rescheduled."

    await safe_edit_or_send(query, msg, reply_markup=_back_markup(student_id))
