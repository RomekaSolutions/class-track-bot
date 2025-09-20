import json
import os
import sys
import types
from datetime import datetime

# Ensure project root on path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Minimal stubs for external modules used during import
class InlineKeyboardButton:
    def __init__(self, text, callback_data=None):
        self.text = text
        self.callback_data = callback_data

class InlineKeyboardMarkup:
    def __init__(self, keyboard):
        self.inline_keyboard = keyboard

class CommandHandler:
    def __init__(self, command, callback):
        self.command = command
        self.callback = callback

class CallbackQueryHandler:
    def __init__(self, callback, pattern=None):
        self.callback = callback
        self.pattern = pattern

class Application:
    def __init__(self):
        self.handlers = []
    def add_handler(self, handler, group=0):
        self.handlers.append(handler)

class ApplicationBuilder:
    def token(self, token):
        return self
    def build(self):
        return Application()

class JobQueue: ...
class filters: ...

telegram = types.ModuleType("telegram")
telegram.InlineKeyboardButton = InlineKeyboardButton
telegram.InlineKeyboardMarkup = InlineKeyboardMarkup
telegram.Update = object
telegram.ReplyKeyboardMarkup = object
telegram.ReplyKeyboardRemove = object
telegram.KeyboardButton = object

telegram_ext = types.ModuleType("telegram.ext")
telegram_ext.CommandHandler = CommandHandler
telegram_ext.CallbackQueryHandler = CallbackQueryHandler
telegram_ext.ApplicationBuilder = ApplicationBuilder
telegram_ext.Application = Application
telegram_ext.JobQueue = JobQueue
telegram_ext.filters = filters
telegram_ext.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
telegram_ext.ConversationHandler = object
telegram_ext.MessageHandler = object

sys.modules["telegram"] = telegram
sys.modules["telegram.ext"] = telegram_ext

_pytz = types.ModuleType("pytz")
class _TZ:
    def localize(self, dt, is_dst=None):
        return dt
    def normalize(self, dt):
        return dt
_pytz.timezone = lambda name: _TZ()
_pytz.AmbiguousTimeError = Exception
_pytz.NonExistentTimeError = Exception
sys.modules["pytz"] = _pytz

import class_track_bot as ctb
import data_store


def _setup(monkeypatch, tmp_path, class_dates):
    students = {
        "1": {
            "name": "Test Student",
            "telegram_id": 1,
            "class_dates": class_dates,
            "classes_remaining": 1,
            "cancelled_dates": [],
            "schedule_pattern": "",
            "class_duration_hours": 1.0,
        }
    }
    students_file = tmp_path / "students.json"
    logs_file = tmp_path / "logs.json"
    students_file.write_text(json.dumps(students))
    logs_file.write_text("[]")
    for module in (ctb, data_store):
        monkeypatch.setattr(module, "STUDENTS_FILE", str(students_file))
        monkeypatch.setattr(module, "LOGS_FILE", str(logs_file))
    return students_file, logs_file


def test_dashboard_ignores_logs_without_date(monkeypatch, capsys):
    monkeypatch.setattr(ctb, "load_students", lambda: {})
    today = datetime.now().date().isoformat()
    logs = [
        {"date": today, "status": "completed"},
        {"status": "missed"},
    ]
    monkeypatch.setattr(ctb, "load_logs", lambda: logs)
    summary = ctb.generate_dashboard_summary()
    captured = capsys.readouterr()
    assert "Skipping malformed log entry (no date/at)" in captured.out
    assert "Completed: 1" in summary
    assert "Note: 1 logs were ignored due to missing date/at." in summary


def test_dual_field_logging_and_counting(tmp_path, monkeypatch):
    iso_dt = datetime.now().replace(microsecond=0).isoformat()
    _, logs_file = _setup(monkeypatch, tmp_path, [iso_dt])

    data_store.mark_class_completed("1", iso_dt)

    logs = json.loads(logs_file.read_text())
    last_entry = logs[-1]
    assert "at" in last_entry
    assert "date" in last_entry
    assert last_entry["at"] == last_entry["date"]

    summary = ctb.generate_dashboard_summary()
    assert "Completed: 1" in summary
