import os
import sys
import types
from datetime import datetime, date, timedelta, tzinfo

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
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


def test_no_dates_past_renewal(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return tz.localize(cls(2023, 1, 1, 0, 0)) if tz else cls(2023, 1, 1, 0, 0)

    monkeypatch.setattr(ctb, "datetime", FixedDatetime)

    student = {
        "schedule_pattern": "Monday 10:00",
        "cycle_weeks": 4,
        "class_dates": [],
        "renewal_date": "2023-01-15",
    }
    changed = ctb.ensure_future_class_dates(student)
    assert changed is True
    renewal_date = date.fromisoformat("2023-01-15")
    assert student["class_dates"], "class dates should not be empty"
    for item in student["class_dates"]:
        dt = datetime.fromisoformat(item)
        assert dt.date() <= renewal_date


def test_get_upcoming_includes_renewal_date(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return tz.localize(cls(2023, 1, 1, 0, 0)) if tz else cls(2023, 1, 1, 0, 0)

    monkeypatch.setattr(ctb, "datetime", FixedDatetime)

    student = {
        "class_dates": [
            "2023-01-10T10:00",  # naive
            "2023-01-15T10:00+07:00",  # aware
            "2023-01-20T10:00",  # naive
        ],
        "renewal_date": "2023-01-15",
        "cancelled_dates": [],
    }
    upcoming = ctb.get_upcoming_classes(student, count=10)
    dates = [dt.date() for dt in upcoming]
    assert dates == [date(2023, 1, 10), date(2023, 1, 15)]


def test_build_student_classes_text_includes_renewal_date(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return tz.localize(cls(2023, 1, 1, 0, 0)) if tz else cls(2023, 1, 1, 0, 0)

    monkeypatch.setattr(ctb, "datetime", FixedDatetime)

    dt_str = "2023-01-15T10:00"
    student = {
        "name": "Alice",
        "class_dates": [dt_str],
        "renewal_date": "2023-01-15",
        "classes_remaining": 1,
    }
    text = ctb.build_student_classes_text(student, limit=5)
    assert ctb.fmt_bkk(dt_str) in text
    assert text.count("All times shown in Thai time (ICT).") == 1


def test_mixed_log_timestamps_sort(monkeypatch):
    student = {
        "name": "Alice",
        "class_dates": [],
        "renewal_date": "2024-01-10",
        "classes_remaining": 0,
    }
    logs = [
        {"student": "alice", "date": "2024-01-02T10:00+07:00", "status": "completed"},
        {"student": "alice", "date": "2024-01-03T09:00", "status": "missed"},
    ]
    monkeypatch.setattr(ctb, "load_logs", lambda: logs)
    text = ctb.build_student_classes_text(student, limit=5, student_key="alice")
    lines = text.splitlines()
    recent_idx = lines.index("Recent classes:")
    first_log = lines[recent_idx + 1]
    assert "Wed 03 Jan 09:00" in first_log
