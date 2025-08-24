import os
import sys
import types
import json
from datetime import datetime, timedelta, date, tzinfo

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Patch telegram modules
telegram_module = types.ModuleType("telegram")
for name in [
    "Update",
    "InlineKeyboardButton",
    "InlineKeyboardMarkup",
    "ReplyKeyboardMarkup",
    "ReplyKeyboardRemove",
    "KeyboardButton",
]:
    setattr(telegram_module, name, object)
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
