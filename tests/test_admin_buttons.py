import os
import sys
import json
import types
import asyncio

# Ensure project root is on the path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# ---- Stub required external modules ----
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

# Minimal pytz stub
_pytz = types.ModuleType("pytz")
from datetime import tzinfo, timedelta


class _TZ(tzinfo):
    def utcoffset(self, dt):
        return timedelta(0)

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def localize(self, dt, is_dst=None):
        return dt.replace(tzinfo=self)

    def normalize(self, dt):
        return dt
_pytz.timezone = lambda name: _TZ()
_pytz.AmbiguousTimeError = Exception
_pytz.NonExistentTimeError = Exception
sys.modules["pytz"] = _pytz
sys.modules["pytz.tzinfo"] = types.ModuleType("pytz.tzinfo")

# ---- Import module under test ----
import data_store
from admin_flows import handle_student_action


class DummyMessage:
    def __init__(self):
        self.sent = []

    async def reply_text(self, text, reply_markup=None):
        self.sent.append(text)


class DummyQuery:
    def __init__(self, data):
        self.data = data
        self.message = DummyMessage()
        self.edited = None
        self.from_user = types.SimpleNamespace(id=123456789)

    async def answer(self):
        pass

    async def edit_message_text(self, text, reply_markup=None):
        self.edited = text


def _setup(monkeypatch, tmp_path):
    students = {
        "1": {
            "name": "Alice",
            "class_dates": [],
            "classes_remaining": 0,
            "cancelled_dates": [],
            "free_class_credit": 0,
            "class_duration_hours": 1.0,
        }
    }
    logs = []

    import importlib
    import class_track_bot as ctb
    import data_store
    from datetime import timezone

    importlib.reload(ctb)

    monkeypatch.setattr(ctb, "load_students", lambda: students)
    monkeypatch.setattr(ctb, "save_students", lambda d: students.update(d))
    monkeypatch.setattr(data_store, "get_student_by_id", lambda sid: students.get(sid))
    monkeypatch.setattr(ctb, "student_timezone", lambda s: timezone.utc)

    def _load_logs():
        return logs

    def _save_logs(l):
        logs.clear()
        logs.extend(l)

    monkeypatch.setattr(ctb, "load_logs", _load_logs)
    monkeypatch.setattr(ctb, "save_logs", _save_logs)

    return students


def _ctx():
    return types.SimpleNamespace(
        user_data={},
        application=types.SimpleNamespace(job_queue=types.SimpleNamespace(jobs=lambda: [])),
    )


def test_length_button(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path)
    query = DummyQuery("stu:LENGTH:1")
    ctx = _ctx()
    asyncio.run(handle_student_action(types.SimpleNamespace(callback_query=query), ctx))
    assert "Enter new length" in query.edited


def test_edit_button(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path)
    query = DummyQuery("stu:EDIT:1")
    ctx = _ctx()
    asyncio.run(handle_student_action(types.SimpleNamespace(callback_query=query), ctx))
    assert "Editing Alice" in query.edited


def test_freecredit_button(monkeypatch, tmp_path):
    students = _setup(monkeypatch, tmp_path)
    query = DummyQuery("stu:FREECREDIT:1")
    ctx = _ctx()
    asyncio.run(handle_student_action(types.SimpleNamespace(callback_query=query), ctx))
    assert students["1"]["free_class_credit"] == 1
    assert "free class credit" in query.edited.lower()


def test_remove_button(monkeypatch, tmp_path):
    students = _setup(monkeypatch, tmp_path)
    query = DummyQuery("stu:REMOVE:1")
    ctx = _ctx()
    asyncio.run(handle_student_action(types.SimpleNamespace(callback_query=query), ctx))
    assert "1" not in students
    assert any("removed" in msg.lower() for msg in query.message.sent)
