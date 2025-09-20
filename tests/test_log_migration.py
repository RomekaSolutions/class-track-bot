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

class DummyConversationHandler:
    END = object()

for name, value in [
    ("Application", object),
    ("ApplicationBuilder", object),
    ("CommandHandler", object),
    ("ContextTypes", context_types),
    ("ConversationHandler", DummyConversationHandler),
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
import asyncio
import class_track_bot as ctb
import data_store as ds


def test_resolve_student_prefers_numeric_id():
    students = {
        "123": {"telegram_id": 123, "telegram_handle": "foo", "name": "Foo"}
    }
    key, student = ctb.resolve_student(students, "@Foo")
    assert key == "123"
    assert student is students["123"]


def test_load_logs_normalizes_student_keys(tmp_path, monkeypatch):
    students = {
        "123": {"name": "Foo", "telegram_id": 123, "telegram_handle": "Foo"}
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
    monkeypatch.setattr(ds, "STUDENTS_FILE", str(students_file))
    monkeypatch.setattr(ds, "LOGS_FILE", str(logs_file))

    loaded_logs = ds.load_logs()
    assert loaded_logs == [
        {"student": "123", "date": "2024-01-01", "status": "completed"}
    ]

    saved = json.loads(logs_file.read_text())
    assert saved == loaded_logs


def test_migrate_student_records(tmp_path, monkeypatch, capsys):
    students = {
        "alice": {"name": "Alice", "telegram_id": 1, "telegram_handle": "alice"}
    }
    logs = [
        {"student": "@alice", "date": "2024-01-01", "status": "completed"},
        {"student": "unknown", "date": "2024-01-02", "status": "missed"},
    ]
    students_file = tmp_path / "students.json"
    logs_file = tmp_path / "logs.json"
    students_file.write_text(json.dumps(students))
    logs_file.write_text(json.dumps(logs))
    for module in (ctb, ds):
        monkeypatch.setattr(module, "STUDENTS_FILE", str(students_file))
        monkeypatch.setattr(module, "LOGS_FILE", str(logs_file))

    ds.migrate_student_records()
    out = capsys.readouterr().out
    assert "Handles rekeyed" in out
    assert ds.load_students().keys() == {"1"}
    assert ds.load_logs()[0]["student"] == "1"


def test_add_student_handle_resolves(tmp_path, monkeypatch):
    students_file = tmp_path / "students.json"
    logs_file = tmp_path / "logs.json"
    students_file.write_text("{}")
    logs_file.write_text("[]")
    for module in (ctb, ds):
        monkeypatch.setattr(module, "STUDENTS_FILE", str(students_file))
        monkeypatch.setattr(module, "LOGS_FILE", str(logs_file))
    calls = []
    monkeypatch.setattr(ctb, "schedule_student_reminders", lambda app, key, s: calls.append(key))
    monkeypatch.setattr(ctb, "ConversationHandler", types.SimpleNamespace(END=None))

    async def fake_get_chat(username):
        return types.SimpleNamespace(id=555, username="foo")

    context = types.SimpleNamespace(
        user_data={
            "name": "Test",
            "plan_price": 1000,
            "classes_remaining": 4,
            "schedule_pattern": "",
            "cutoff_hours": 24,
            "cycle_weeks": 4,
            "class_duration_hours": 1.0,
        },
        application=types.SimpleNamespace(bot=types.SimpleNamespace(get_chat=fake_get_chat)),
    )

    class DummyMessage:
        def __init__(self, text):
            self.text = text
            self.reply_calls = []

        async def reply_text(self, text, reply_markup=None):
            self.reply_calls.append(text)

    # add_handle resolves handle -> id
    update = types.SimpleNamespace(message=DummyMessage("@foo"))
    asyncio.run(ctb.add_handle(update, context))
    assert context.user_data["telegram_id"] == 555
    assert context.user_data["telegram_handle"] == "foo"

    # final step add_renewal
    update2 = types.SimpleNamespace(message=DummyMessage("2024-12-31"))
    asyncio.run(ctb.add_renewal(update2, context))
    data = json.loads(students_file.read_text())
    assert "555" in data
    assert data["555"]["telegram_handle"] == "foo"
    assert calls == ["555"]


def test_add_student_numeric_id(tmp_path, monkeypatch):
    students_file = tmp_path / "students.json"
    logs_file = tmp_path / "logs.json"
    students_file.write_text("{}")
    logs_file.write_text("[]")
    for module in (ctb, ds):
        monkeypatch.setattr(module, "STUDENTS_FILE", str(students_file))
        monkeypatch.setattr(module, "LOGS_FILE", str(logs_file))
    calls = []
    monkeypatch.setattr(ctb, "schedule_student_reminders", lambda app, key, s: calls.append(key))
    monkeypatch.setattr(ctb, "ConversationHandler", types.SimpleNamespace(END=None))

    context = types.SimpleNamespace(
        user_data={
            "name": "Num",
            "plan_price": 1000,
            "classes_remaining": 4,
            "schedule_pattern": "",
            "cutoff_hours": 24,
            "cycle_weeks": 4,
            "class_duration_hours": 1.0,
        },
        application=types.SimpleNamespace(bot=types.SimpleNamespace(get_chat=lambda u: types.SimpleNamespace(id=777))),
    )

    class DummyMessage:
        def __init__(self, text):
            self.text = text
            self.reply_calls = []

        async def reply_text(self, text, reply_markup=None):
            self.reply_calls.append(text)

    update = types.SimpleNamespace(message=DummyMessage("123"))
    asyncio.run(ctb.add_handle(update, context))
    assert context.user_data["telegram_id"] == 123
    assert context.user_data["telegram_handle"] is None

    update2 = types.SimpleNamespace(message=DummyMessage("2024-12-31"))
    asyncio.run(ctb.add_renewal(update2, context))
    data = json.loads(students_file.read_text())
    assert "123" in data
    assert calls == ["123"]


def test_add_student_handle_unresolved(tmp_path, monkeypatch):
    students_file = tmp_path / "students.json"
    logs_file = tmp_path / "logs.json"
    students_file.write_text("{}")
    logs_file.write_text("[]")
    for module in (ctb, ds):
        monkeypatch.setattr(module, "STUDENTS_FILE", str(students_file))
        monkeypatch.setattr(module, "LOGS_FILE", str(logs_file))
    calls = []
    monkeypatch.setattr(ctb, "schedule_student_reminders", lambda app, key, s: calls.append(key))
    monkeypatch.setattr(ctb, "ConversationHandler", types.SimpleNamespace(END=None))

    async def fake_get_chat(username):
        raise Exception("not found")

    context = types.SimpleNamespace(
        user_data={
            "name": "Unk",
            "plan_price": 1000,
            "classes_remaining": 4,
            "schedule_pattern": "",
            "cutoff_hours": 24,
            "cycle_weeks": 4,
            "class_duration_hours": 1.0,
        },
        application=types.SimpleNamespace(bot=types.SimpleNamespace(get_chat=fake_get_chat)),
    )

    class DummyMessage:
        def __init__(self, text):
            self.text = text
            self.reply_calls = []

        async def reply_text(self, text, reply_markup=None):
            self.reply_calls.append(text)

    update = types.SimpleNamespace(message=DummyMessage("@unknown"))
    asyncio.run(ctb.add_handle(update, context))
    assert context.user_data["telegram_id"] is None
    assert context.user_data["telegram_handle"] == "unknown"
    assert context.user_data.get("needs_id")

    update2 = types.SimpleNamespace(message=DummyMessage("2024-12-31"))
    asyncio.run(ctb.add_renewal(update2, context))
    data = json.loads(students_file.read_text())
    assert "unknown" in data
    assert data["unknown"].get("needs_id")
    assert calls == []
    # admin actions resolve student by handle
    students = ctb.load_students()
    key, student = ctb.resolve_student(students, "unknown")
    assert key == "unknown" and student is not None
