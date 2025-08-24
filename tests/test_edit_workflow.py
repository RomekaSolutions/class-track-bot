import os
import os
import sys
import types
import json
import asyncio
from datetime import datetime, timedelta, date, tzinfo

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Patch telegram modules
telegram_module = types.ModuleType("telegram")

class InlineKeyboardButton:
    def __init__(self, text, callback_data=None):
        self.text = text
        self.callback_data = callback_data

class InlineKeyboardMarkup:
    def __init__(self, keyboard):
        self.inline_keyboard = keyboard

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

# Patch pytz
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

import class_track_bot as ctb


def test_parse_day_time():
    assert ctb.parse_day_time("Monday 09:00") == "Monday 09:00"
    assert ctb.parse_day_time(" tuesday 7:05 ") == "Tuesday 07:05"
    assert ctb.parse_day_time("Funday 10:00") is None
    assert ctb.parse_day_time("Monday 24:00") is None


def test_edit_add_delete_weekly_slots(monkeypatch):
    tz = ctb.BASE_TZ
    now = tz.localize(datetime(2025, 1, 15, 9, 0))

    # --- edit slot ---
    pattern = "Monday 10:00, Wednesday 10:00"
    student = {
        "schedule_pattern": pattern,
        "class_dates": ctb.parse_schedule(pattern, start_date=date(2025, 1, 1), cycle_weeks=6),
        "cycle_weeks": 6,
    }
    ctb.edit_weekly_slot("1", student, 0, "Tuesday 10:00", now=now)
    assert student["schedule_pattern"] == "Tuesday 10:00, Wednesday 10:00"
    past = [
        datetime.fromisoformat(d)
        for d in student["class_dates"]
        if datetime.fromisoformat(d) <= now
    ]
    future = [
        datetime.fromisoformat(d)
        for d in student["class_dates"]
        if datetime.fromisoformat(d) > now
    ]
    assert any(dt.strftime("%A %H:%M") == "Monday 10:00" for dt in past)
    assert all(dt.strftime("%A %H:%M") != "Monday 10:00" for dt in future)
    assert any(dt.strftime("%A %H:%M") == "Tuesday 10:00" for dt in future)

    # --- add slot ---
    pattern = "Monday 10:00"
    student = {
        "schedule_pattern": pattern,
        "class_dates": ctb.parse_schedule(pattern, start_date=date(2025, 1, 1), cycle_weeks=6),
        "cycle_weeks": 6,
    }
    ctb.add_weekly_slot("1", student, "Wednesday 12:00", now=now)
    assert "Wednesday 12:00" in student["schedule_pattern"]
    past = [
        datetime.fromisoformat(d)
        for d in student["class_dates"]
        if datetime.fromisoformat(d) <= now
    ]
    future = [
        datetime.fromisoformat(d)
        for d in student["class_dates"]
        if datetime.fromisoformat(d) > now
    ]
    assert all(dt.strftime("%A %H:%M") != "Wednesday 12:00" for dt in past)
    assert any(dt.strftime("%A %H:%M") == "Wednesday 12:00" for dt in future)

    # --- delete slot ---
    pattern = "Monday 10:00, Wednesday 12:00"
    student = {
        "schedule_pattern": pattern,
        "class_dates": ctb.parse_schedule(pattern, start_date=date(2025, 1, 1), cycle_weeks=6),
        "cycle_weeks": 6,
    }
    ctb.delete_weekly_slot("1", student, 0, now=now)
    assert student["schedule_pattern"] == "Wednesday 12:00"
    past = [
        datetime.fromisoformat(d)
        for d in student["class_dates"]
        if datetime.fromisoformat(d) <= now
    ]
    future = [
        datetime.fromisoformat(d)
        for d in student["class_dates"]
        if datetime.fromisoformat(d) > now
    ]
    assert any(dt.strftime("%A %H:%M") == "Monday 10:00" for dt in past)
    assert all(dt.strftime("%A %H:%M") != "Monday 10:00" for dt in future)


def test_reschedule_and_cancel(monkeypatch, tmp_path):
    tz = ctb.BASE_TZ
    now = tz.localize(datetime(2025, 1, 1, 9, 0))
    old_dt = tz.localize(datetime(2025, 1, 5, 10, 0))
    new_dt = tz.localize(datetime(2025, 1, 6, 11, 0))

    logs_file = tmp_path / "logs.json"
    logs_file.write_text("[]")
    monkeypatch.setattr(ctb, "LOGS_FILE", str(logs_file))

    called = {}

    def fake_sched(app, key, student):
        called["called"] = True

    monkeypatch.setattr(ctb, "schedule_student_reminders", fake_sched)

    student = {
        "class_dates": [old_dt.isoformat()],
        "cancelled_dates": [new_dt.isoformat()],
        "renewal_date": "2025-12-31",
    }
    ctb.reschedule_single_class(
        "1", student, old_dt.isoformat(), new_dt.isoformat(), now=now, application=object()
    )
    assert new_dt.isoformat() in student["class_dates"]
    assert old_dt.isoformat() not in student["class_dates"]
    assert new_dt.isoformat() not in student["cancelled_dates"]
    assert called.get("called")
    logs = json.loads(logs_file.read_text())
    assert logs and logs[0]["status"] == "rescheduled"

    # --- cancel ---
    called.clear()
    student = {
        "class_dates": [new_dt.isoformat()],
        "cancelled_dates": [],
        "reschedule_credit": 0,
    }
    ctb.cancel_single_class(
        "1", student, new_dt.isoformat(), grant_credit=True, application=object()
    )
    assert new_dt.isoformat() in student["cancelled_dates"]
    assert student["reschedule_credit"] == 1
    assert called.get("called")
    logs = json.loads(logs_file.read_text())
    assert any(entry["status"] == "cancelled (admin)" for entry in logs)


def test_bulk_shift(monkeypatch):
    tz = ctb.BASE_TZ
    now = tz.localize(datetime(2025, 1, 15, 9, 0))
    pattern = "Monday 10:00"
    student = {
        "schedule_pattern": pattern,
        "class_dates": ctb.parse_schedule(pattern, start_date=date(2025, 1, 1), cycle_weeks=8),
        "cycle_weeks": 8,
    }
    ctb.bulk_shift_slot("1", student, 0, new_entry="Tuesday 11:00", now=now)
    assert student["schedule_pattern"] == "Tuesday 11:00"
    past = [
        datetime.fromisoformat(d)
        for d in student["class_dates"]
        if datetime.fromisoformat(d) <= now
    ]
    future = [
        datetime.fromisoformat(d)
        for d in student["class_dates"]
        if datetime.fromisoformat(d) > now
    ]
    assert any(dt.strftime("%A %H:%M") == "Monday 10:00" for dt in past)
    assert all(dt.strftime("%A %H:%M") == "Tuesday 11:00" for dt in future)
    assert len(student["class_dates"]) == len(set(student["class_dates"]))

    # offset variant
    student = {
        "schedule_pattern": pattern,
        "class_dates": ctb.parse_schedule(pattern, start_date=date(2025, 1, 1), cycle_weeks=8),
        "cycle_weeks": 8,
    }
    ctb.bulk_shift_slot("1", student, 0, offset_minutes=30, now=now)
    future = [
        datetime.fromisoformat(d)
        for d in student["class_dates"]
        if datetime.fromisoformat(d) > now
    ]
    assert all(dt.strftime("%A %H:%M") == "Monday 10:30" for dt in future)


def make_update(text, user_id=999):
    replies = []

    async def reply(msg, reply_markup=None):
        replies.append(msg)

    message = types.SimpleNamespace(text=text, reply_text=reply)
    update = types.SimpleNamespace(
        effective_user=types.SimpleNamespace(id=user_id), message=message
    )
    return update, replies


async def run_handle(state, text, student, monkeypatch, extra_user_data=None):
    student_key = "1"
    students = {student_key: student}
    logs = []

    monkeypatch.setattr(ctb, "ADMIN_IDS", {999})
    monkeypatch.setattr(ctb, "load_students", lambda: students)
    monkeypatch.setattr(ctb, "save_students", lambda s: students.update(s))
    monkeypatch.setattr(ctb, "load_logs", lambda: logs)
    monkeypatch.setattr(ctb, "save_logs", lambda l: logs.extend(l[len(logs):]))
    monkeypatch.setattr(ctb, "schedule_student_reminders", lambda app, key, s: None)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            base = datetime(2025, 1, 1, 9, 0)
            return tz.localize(base) if tz else base

    monkeypatch.setattr(ctb, "datetime", FixedDateTime)

    user_data = {"edit_state": state, "edit_student_key": student_key}
    if extra_user_data:
        user_data.update(extra_user_data)
    context = types.SimpleNamespace(user_data=user_data, application=object())
    update, replies = make_update(text)
    await ctb.handle_message(update, context)
    return students[student_key], logs, replies, context.user_data


def test_handle_message_edit_states(monkeypatch):
    tz = ctb.BASE_TZ
    # Change time (all future)
    pattern = "Monday 18:00"
    student = {
        "name": "A",
        "schedule_pattern": pattern,
        "class_dates": ctb.parse_schedule(pattern, start_date=date(2025, 1, 1), cycle_weeks=4),
        "cycle_weeks": 4,
    }
    student, logs, replies, user_data = asyncio.run(
        run_handle(
            "await_time_all",
            "Tuesday 19:00",
            student,
            monkeypatch,
            extra_user_data={"edit_slot_index": 0, "edit_old_entry": "Monday 18:00"},
        )
    )
    assert student["schedule_pattern"] == "Tuesday 19:00"
    assert not user_data.get("edit_state")
    assert any("Updated slot 0" in r for r in replies)
    assert logs and logs[0]["status"] == "pattern_updated"

    # Reschedule once (time only)
    old_dt = tz.localize(datetime(2025, 1, 5, 10, 0))
    student = {
        "class_dates": [old_dt.isoformat()],
        "cancelled_dates": [],
        "renewal_date": "2025-12-31",
    }
    student, logs, replies, user_data = asyncio.run(
        run_handle(
            "await_time_once",
            "12:00",
            student,
            monkeypatch,
            extra_user_data={"edit_once_old_dt": old_dt.isoformat()},
        )
    )
    assert any("Rescheduled class" in r for r in replies)
    assert logs and logs[-1]["status"] == "rescheduled"
    new_dt = datetime.fromisoformat(student["class_dates"][0])

    # Cancel
    student = {"class_dates": [new_dt.isoformat()], "cancelled_dates": [], "reschedule_credit": 0}
    student, logs, replies, user_data = asyncio.run(
        run_handle(
            "await_cancel",
            new_dt.isoformat(),
            student,
            monkeypatch,
        )
    )
    assert new_dt.isoformat() in student["cancelled_dates"]
    assert student["reschedule_credit"] == 1


def test_edit_scope_once_shows_buttons(monkeypatch):
    tz = ctb.BASE_TZ

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            base = datetime(2025, 1, 1, 9, 0)
            return tz.localize(base) if tz else base

    monkeypatch.setattr(ctb, "datetime", FixedDateTime)

    pattern = "Monday 10:00"
    student = {
        "name": "A",
        "schedule_pattern": pattern,
        "class_dates": ctb.parse_schedule(pattern, start_date=date(2025, 1, 1), cycle_weeks=10),
        "cycle_weeks": 10,
    }
    students = {"1": student}
    monkeypatch.setattr(ctb, "load_students", lambda: students)
    monkeypatch.setattr(ctb, "ADMIN_IDS", {1})

    class DummyQuery:
        def __init__(self):
            self.data = "edit:time:scope:once:1:0"
            self.edited = None

        async def answer(self):
            pass

        async def edit_message_text(self, text, reply_markup=None):
            self.edited = (text, reply_markup)

    query = DummyQuery()
    update = types.SimpleNamespace(
        callback_query=query, effective_user=types.SimpleNamespace(id=1)
    )
    asyncio.run(ctb.edit_time_scope_callback(update, types.SimpleNamespace()))

    assert query.edited is not None
    text, markup = query.edited
    buttons = markup.inline_keyboard
    assert text == "Select occurrence to reschedule:"
    assert len(buttons) == 7  # 6 upcoming dates + Back button
    for i in range(6):
        btn = buttons[i][0]
        expected = student["class_dates"][i]
        assert btn.text == expected
        assert btn.callback_data == f"edit:time:oncepick:1:{expected}"
