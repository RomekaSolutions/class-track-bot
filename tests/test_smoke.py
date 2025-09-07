import os
import sys
import types
import re

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Stub telegram modules
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
        self.pattern = re.compile(pattern) if pattern else None

class HandlerGroup:
    def __init__(self):
        self.handlers = []

class Application:
    def __init__(self):
        self.handlers = []
    def add_handler(self, handler, group=0):
        while len(self.handlers) <= group:
            self.handlers.append(HandlerGroup())
        self.handlers[group].handlers.append(handler)

class ApplicationBuilder:
    def __init__(self):
        self._token = None
    def token(self, token):
        self._token = token
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
sys.modules.pop("class_track_bot", None)
from class_track_bot import build_application


def test_handlers_registered():
    app = build_application()
    cmds = [
        h
        for group in app.handlers
        for h in group.handlers
        if isinstance(h, CommandHandler)
    ]
    assert any(h.command == "admin" for h in cmds)
    cqs = [
        h
        for group in app.handlers
        for h in group.handlers
        if isinstance(h, CallbackQueryHandler)
    ]
    assert any(h.pattern and h.pattern.pattern.startswith("^stu:") for h in cqs)
