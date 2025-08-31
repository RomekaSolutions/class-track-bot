import types
import sys
import os
import types
import sys
from datetime import datetime, timedelta, date, tzinfo

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Stub telegram modules with minimal classes for tests
class InlineKeyboardButton:
    def __init__(self, text, callback_data=None):
        self.text = text
        self.callback_data = callback_data

class InlineKeyboardMarkup:
    def __init__(self, keyboard):
        self.inline_keyboard = keyboard

telegram_module = types.ModuleType("telegram")
for name, value in [
    ("Update", object),
    ("InlineKeyboardButton", InlineKeyboardButton),
    ("InlineKeyboardMarkup", InlineKeyboardMarkup),
    ("ReplyKeyboardMarkup", object),
    ("ReplyKeyboardRemove", object),
    ("KeyboardButton", object),
]:
    setattr(telegram_module, name, value)
telegram_ext_module = types.ModuleType("telegram.ext")
context_types = types.SimpleNamespace(DEFAULT_TYPE=object)
for name, value in [
    ("Application", object),
    ("ApplicationBuilder", object),
    ("CommandHandler", object),
    ("ContextTypes", context_types),
    ("ConversationHandler", object),
    ("MessageHandler", object),
    ("CallbackQueryHandler", object),
    ("JobQueue", object),
    ("filters", object),
]:
    setattr(telegram_ext_module, name, value)
sys.modules["telegram"] = telegram_module
sys.modules["telegram.ext"] = telegram_ext_module

# Patch pytz timezone
pytz_module = types.ModuleType("pytz")
class DummyTZ(tzinfo):
    def utcoffset(self, dt):
        return timedelta(0)
    def dst(self, dt):
        return timedelta(0)
    def tzname(self, dt):
        return "DummyTZ"
    def localize(self, dt, is_dst=None):
        return dt.replace(tzinfo=self)
    def normalize(self, dt):
        return dt
pytz_module.timezone = lambda name: DummyTZ()
pytz_module.AmbiguousTimeError = Exception
pytz_module.NonExistentTimeError = Exception
sys.modules["pytz"] = pytz_module
sys.modules["pytz.tzinfo"] = types.ModuleType("pytz.tzinfo")

import asyncio
import pytest
import class_track_bot as ctb


def test_set_class_length_and_slot_durations():
    student = {"class_duration_hours": 1.0, "slot_durations": {}}
    ctb.set_class_length(student, 1.5, slot="Monday 17:00")
    assert student["slot_durations"]["Monday 17:00"] == 1.5
    ctb.set_class_length(student, 2.0)
    assert student["class_duration_hours"] == 2.0
    # slot duration moves when editing slot
    student["schedule_pattern"] = "Monday 17:00"
    student["class_dates"] = []
    ctb.edit_weekly_slot("1", student, 0, "Tuesday 17:00")
    assert "Monday 17:00" not in student.get("slot_durations", {})
    assert student["slot_durations"]["Tuesday 17:00"] == 1.5
    ctb.delete_weekly_slot("1", student, 0)
    assert "Tuesday 17:00" not in student.get("slot_durations", {})


def test_reschedule_and_cancel_single_class(monkeypatch):
    old_dt = datetime(2025, 1, 5, 10, 0, tzinfo=ctb.BKK_TZ)
    new_dt = datetime(2025, 1, 6, 11, 0, tzinfo=ctb.BKK_TZ)
    student = {"class_dates": [old_dt.isoformat()], "cancelled_dates": []}
    calls = []
    monkeypatch.setattr(ctb, "schedule_student_reminders", lambda app, key, s: calls.append(key))
    now = datetime(2025, 1, 1, 0, 0, tzinfo=ctb.BKK_TZ)
    ctb.reschedule_single_class("1", student, old_dt.isoformat(), new_dt.isoformat(), now=now, application=object(), log=False)
    assert old_dt.isoformat() not in student["class_dates"]
    assert new_dt.isoformat() in student["class_dates"]
    assert calls == ["1"]
    # time-only reschedule
    another_old = datetime(2025, 1, 8, 10, 0, tzinfo=ctb.BKK_TZ)
    student["class_dates"] = [another_old.isoformat()]
    ctb.reschedule_single_class("1", student, another_old.isoformat(), "12:30", now=now, application=object(), log=False)
    assert any("12:30" in d for d in student["class_dates"])
    # cancel early
    student["classes_remaining"] = 2
    ctb.cancel_single_class("1", student, new_dt.isoformat(), grant_credit=True, application=object(), log=False)
    assert new_dt.isoformat() in student["cancelled_dates"]
    assert student["reschedule_credit"] == 1
    # cancel late
    later_dt = datetime(2025, 1, 7, 11, 0, tzinfo=ctb.BKK_TZ)
    student["class_dates"].append(later_dt.isoformat())
    student["cancelled_dates"].clear()
    ctb.cancel_single_class("1", student, later_dt.isoformat(), grant_credit=False, application=object(), log=False)
    student["classes_remaining"] -= 1
    assert later_dt.isoformat() in student["cancelled_dates"]
    assert student["classes_remaining"] == 1


def test_start_and_buttons(monkeypatch):
    start_dt = datetime(2025, 1, 10, 9, 0, tzinfo=ctb.BKK_TZ)
    student = {
        "name": "A",
        "class_dates": [start_dt.isoformat()],
        "classes_remaining": 3,
        "renewal_date": "2025-12-31",
    }
    students = {"1": student}
    monkeypatch.setattr(ctb, "load_students", lambda: students)
    monkeypatch.setattr(ctb, "save_students", lambda s: students.update(s))
    msg_calls = []
    class DummyMessage:
        async def reply_text(self, text, reply_markup=None):
            msg_calls.append((text, reply_markup))
    update = types.SimpleNamespace(effective_user=types.SimpleNamespace(id=1, username="u"), message=DummyMessage())
    asyncio.run(ctb.start_command(update, types.SimpleNamespace()))
    markup = msg_calls[0][1]
    callbacks = [b.callback_data for row in markup.inline_keyboard for b in row]
    assert "my_classes" in callbacks and "cancel_class" in callbacks
    # My classes flow
    class DummyQuery:
        def __init__(self, data):
            self.data = data
            self.edited = []
            self.message = types.SimpleNamespace(reply_text=lambda *a, **k: None)
            self.from_user = types.SimpleNamespace(id=1, username="u")
        async def answer(self):
            pass
        async def edit_message_text(self, text, reply_markup=None):
            self.edited.append((text, reply_markup))
    query = DummyQuery("my_classes")
    asyncio.run(ctb.student_button_handler(types.SimpleNamespace(callback_query=query), types.SimpleNamespace()))
    assert query.edited and query.edited[0][1] is not None
    # Cancel class flow
    query2 = DummyQuery("cancel_class")
    asyncio.run(ctb.student_button_handler(types.SimpleNamespace(callback_query=query2), types.SimpleNamespace()))
    assert query2.edited  # message sent


def test_pending_cancel_banner_withdraw_and_dismiss(monkeypatch):
    future_dt = datetime(2030, 1, 10, 9, 0, tzinfo=ctb.BKK_TZ)
    student = {
        "name": "A",
        "class_dates": [future_dt.isoformat()],
        "classes_remaining": 3,
        "renewal_date": "2030-12-31",
        "pending_cancel": {
            "class_time": future_dt.isoformat(),
            "requested_at": future_dt.isoformat(),
            "type": "early",
        },
    }
    students = {"1": student}
    monkeypatch.setattr(ctb, "load_students", lambda: students)
    monkeypatch.setattr(ctb, "save_students", lambda s: students.update(s))

    class DummyQuery:
        def __init__(self, data):
            self.data = data
            self.edited = []
            self.message = types.SimpleNamespace(reply_text=lambda *a, **k: None)
            self.from_user = types.SimpleNamespace(id=1, username="u")

        async def answer(self):
            pass

        async def edit_message_text(self, text, reply_markup=None):
            self.edited.append((text, reply_markup))

    # My Classes should show banner with withdraw and dismiss buttons
    query = DummyQuery("my_classes")
    asyncio.run(ctb.student_button_handler(types.SimpleNamespace(callback_query=query), types.SimpleNamespace()))
    text, markup = query.edited[0]
    assert "Cancellation requested" in text
    callbacks = [
        b.callback_data for row in markup.inline_keyboard for b in row
    ]
    assert "cancel_withdraw" in callbacks and "cancel_dismiss" in callbacks

    # Dismiss should keep pending_cancel but re-render without banner
    query_dismiss = DummyQuery("cancel_dismiss")
    asyncio.run(ctb.student_button_handler(types.SimpleNamespace(callback_query=query_dismiss), types.SimpleNamespace()))
    text_dismiss, _ = query_dismiss.edited[0]
    assert "Cancellation requested" not in text_dismiss
    assert students["1"].get("pending_cancel") is not None

    # Withdraw should clear pending_cancel and re-render without banner
    query_withdraw = DummyQuery("cancel_withdraw")
    asyncio.run(ctb.student_button_handler(types.SimpleNamespace(callback_query=query_withdraw), types.SimpleNamespace()))
    text_withdraw, _ = query_withdraw.edited[0]
    assert "Cancellation requested" not in text_withdraw
    assert students["1"].get("pending_cancel") is None


def test_cancel_replaces_pending(monkeypatch):
    dt1 = datetime(2030, 1, 5, 10, 0, tzinfo=ctb.BKK_TZ)
    dt2 = datetime(2030, 1, 6, 11, 0, tzinfo=ctb.BKK_TZ)
    student = {
        "name": "A",
        "class_dates": [dt1.isoformat(), dt2.isoformat()],
        "pending_cancel": {
            "class_time": dt1.isoformat(),
            "requested_at": dt1.isoformat(),
            "type": "early",
        },
    }
    students = {"1": student}
    monkeypatch.setattr(ctb, "load_students", lambda: students)
    monkeypatch.setattr(ctb, "save_students", lambda s: students.update(s))

    class DummyQuery:
        def __init__(self, data):
            self.data = data
            self.edited = []
            self.from_user = types.SimpleNamespace(id=1, username="u")

        async def answer(self):
            pass

        async def edit_message_text(self, text, reply_markup=None):
            self.edited.append((text, reply_markup))

    query = DummyQuery("cancel_selected:1")
    asyncio.run(ctb.handle_cancel_selection(types.SimpleNamespace(callback_query=query), types.SimpleNamespace(bot=None)))
    assert students["1"]["pending_cancel"]["class_time"] == dt2.isoformat()
    assert query.edited
    text, _ = query.edited[0]
    assert "previous cancellation request has been replaced" in text


def test_confirmcancel_clears_pending_and_refreshes(monkeypatch):
    future_dt = datetime(2030, 2, 1, 9, 0, tzinfo=ctb.BKK_TZ)
    student = {
        "name": "A",
        "telegram_id": 10,
        "class_dates": [future_dt.isoformat()],
        "classes_remaining": 2,
        "pending_cancel": {
            "class_time": future_dt.isoformat(),
            "requested_at": future_dt.isoformat(),
            "type": "late",
        },
    }
    students = {"1": student}
    monkeypatch.setattr(ctb, "load_students", lambda: students)
    monkeypatch.setattr(ctb, "save_students", lambda s: students.update(s))
    monkeypatch.setattr(ctb, "schedule_student_reminders", lambda app, key, s: None)

    admin_replies = []
    class DummyMessage:
        async def reply_text(self, text):
            admin_replies.append(text)

    sent = []
    class DummyBot:
        async def send_message(self, chat_id, text, reply_markup=None):
            sent.append((chat_id, text, reply_markup))

    context = types.SimpleNamespace(
        args=["1"],
        bot=DummyBot(),
        application=types.SimpleNamespace(job_queue=types.SimpleNamespace(jobs=lambda: [])),
    )
    update = types.SimpleNamespace(
        message=DummyMessage(),
        effective_user=types.SimpleNamespace(id=list(ctb.ADMIN_IDS)[0]),
    )

    asyncio.run(ctb.confirm_cancel_command(update, context))
    assert students["1"].get("pending_cancel") is None
    assert admin_replies
    # Ensure My Classes view sent without banner
    assert any(
        any(b.callback_data == "back_to_start" for b in row)
        for _, _, markup in sent
        if markup
        for row in markup.inline_keyboard
    )
    assert all("Cancellation requested" not in text for _, text, _ in sent)
