import os
import sys
import types
from datetime import timedelta, tzinfo

# Ensure repository root on path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Stub telegram modules
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

# Stub pytz
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

import json
import class_track_bot as ctb


def test_resolve_student_prefers_numeric_id():
    students = {
        "123": {"telegram_id": 123, "telegram_handle": "foo", "name": "Foo"}
    }
    key, student = ctb.resolve_student(students, "@Foo")
    assert key == "123"
    assert student is students["123"]


def test_load_logs_normalizes_student_keys(tmp_path, monkeypatch):
    students = {
        "123": {"name": "Foo", "telegram_id": 123, "telegram_handle": "Foo"},
        "bar": {"name": "Bar", "telegram_handle": "Bar"},
    }
    logs = [
        {"student": "@Foo", "date": "2024-01-01", "status": "completed"},
        {"student": "BAR", "date": "2024-01-02", "status": "missed"},
    ]
    students_file = tmp_path / "students.json"
    logs_file = tmp_path / "logs.json"
    students_file.write_text(json.dumps(students))
    logs_file.write_text(json.dumps(logs))
    monkeypatch.setattr(ctb, "STUDENTS_FILE", str(students_file))
    monkeypatch.setattr(ctb, "LOGS_FILE", str(logs_file))

    loaded_logs = ctb.load_logs()
    assert loaded_logs[0]["student"] == "123"
    assert loaded_logs[1]["student"] == "bar"

    # ensure logs were written back normalized
    saved = json.loads(logs_file.read_text())
    assert saved == loaded_logs
