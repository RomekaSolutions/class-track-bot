import os
import sys
import types
import json
import asyncio
from datetime import datetime, timedelta, tzinfo
from types import SimpleNamespace
from unittest.mock import AsyncMock

# Patch telegram modules before importing bot
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

telegram_module = types.ModuleType("telegram")

class DummyInlineKeyboardButton:
    def __init__(self, text, callback_data=None):
        self.text = text
        self.callback_data = callback_data

class DummyInlineKeyboardMarkup:
    def __init__(self, inline_keyboard):
        self.inline_keyboard = inline_keyboard

tg_attrs = {
    "Update": object,
    "InlineKeyboardButton": DummyInlineKeyboardButton,
    "InlineKeyboardMarkup": DummyInlineKeyboardMarkup,
    "ReplyKeyboardMarkup": object,
    "ReplyKeyboardRemove": object,
    "KeyboardButton": object,
}
for name, val in tg_attrs.items():
    setattr(telegram_module, name, val)

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

# Ensure fresh import of the bot module with patched telegram and pytz stubs
sys.modules.pop("class_track_bot", None)
import class_track_bot as ctb


def test_dayview_command_keyboard(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return tz.localize(cls(2025, 8, 26, 0, 0)) if tz else cls(2025, 8, 26, 0, 0)

    monkeypatch.setattr(ctb, "datetime", FixedDatetime)

    reply = AsyncMock()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=next(iter(ctb.ADMIN_IDS))),
        message=SimpleNamespace(reply_text=reply),
    )
    context = SimpleNamespace()

    asyncio.run(ctb.dayview_command(update, context))

    reply.assert_awaited()
    markup = reply.call_args.kwargs["reply_markup"]
    assert len(markup.inline_keyboard) == 7
    first_button = markup.inline_keyboard[0][0]
    assert first_button.text == "Tue 26 Aug"
    assert first_button.callback_data == "dayview:2025-08-26"


def test_dayview_callback_filters_classes(tmp_path, monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return tz.localize(cls(2025, 8, 26, 10, 0)) if tz else cls(2025, 8, 26, 10, 0)

    monkeypatch.setattr(ctb, "datetime", FixedDatetime)
    tz = ctb.BASE_TZ
    dt1 = tz.localize(FixedDatetime(2025, 8, 26, 9, 0)).isoformat()
    dt2 = tz.localize(FixedDatetime(2025, 8, 26, 10, 30)).isoformat()
    dt3 = tz.localize(FixedDatetime(2025, 8, 26, 10, 45)).isoformat()
    dt4 = tz.localize(FixedDatetime(2025, 8, 26, 10, 0)).isoformat()
    dt5 = tz.localize(FixedDatetime(2025, 8, 26, 11, 0)).isoformat()

    data = {
        "1": {
            "name": "Alice",
            "paused": False,
            "class_dates": [dt1, dt2, dt3],
            "cancelled_dates": [dt3],
        },
        "2": {
            "name": "Bob",
            "paused": True,
            "class_dates": [dt4],
        },
        "3": {
            "name": "Charlie",
            "paused": False,
            "class_dates": [dt5],
            "renewal_date": "2025-08-25",
        },
    }

    students_file = tmp_path / "students.json"
    students_file.write_text(json.dumps(data))
    monkeypatch.setattr(ctb, "STUDENTS_FILE", str(students_file))

    query = SimpleNamespace(
        data="dayview:2025-08-26",
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=next(iter(ctb.ADMIN_IDS))),
        callback_query=query,
    )
    context = SimpleNamespace()

    asyncio.run(ctb.dayview_callback(update, context))

    query.answer.assert_awaited()
    text = query.edit_message_text.call_args.args[0]
    assert text == "10:30 – Alice"
