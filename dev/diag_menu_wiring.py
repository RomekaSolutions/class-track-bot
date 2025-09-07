"""Diagnostic tool to verify student submenu wiring."""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:  # pragma: no cover - minimal stubs if telegram isn't installed
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import CallbackQueryHandler
except Exception:  # pragma: no cover
    import types
    import re as _re

    class CallbackQueryHandler:  # type: ignore
        def __init__(self, callback, pattern=None):
            self.callback = callback
            self.pattern = _re.compile(pattern) if pattern else None

    class InlineKeyboardButton:  # type: ignore
        def __init__(self, text, callback_data=None):
            self.text = text
            self.callback_data = callback_data

    class InlineKeyboardMarkup:  # type: ignore
        def __init__(self, keyboard):
            self.inline_keyboard = keyboard

    telegram = types.ModuleType("telegram")
    telegram.InlineKeyboardButton = InlineKeyboardButton
    telegram.InlineKeyboardMarkup = InlineKeyboardMarkup
    telegram.Update = object
    telegram.ReplyKeyboardMarkup = object
    telegram.ReplyKeyboardRemove = object
    telegram.KeyboardButton = object
    sys.modules.setdefault("telegram", telegram)

    class CommandHandler:  # type: ignore
        def __init__(self, command, callback):
            self.command = command
            self.callback = callback

    class Application:
        def __init__(self):
            self.handlers = []

        def add_handler(self, handler, group=0):
            while len(self.handlers) <= group:
                self.handlers.append(types.SimpleNamespace(handlers=[]))
            self.handlers[group].handlers.append(handler)

    class ApplicationBuilder:
        def __init__(self):
            self._token = None

        def token(self, token):
            self._token = token
            return self

        def build(self):
            return Application()

    telegram_ext = types.ModuleType("telegram.ext")
    telegram_ext.CallbackQueryHandler = CallbackQueryHandler
    telegram_ext.CommandHandler = CommandHandler
    telegram_ext.Application = Application
    telegram_ext.ApplicationBuilder = ApplicationBuilder
    telegram_ext.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
    telegram_ext.ConversationHandler = object
    telegram_ext.MessageHandler = object
    telegram_ext.JobQueue = object
    telegram_ext.filters = object()
    sys.modules.setdefault("telegram.ext", telegram_ext)

    # Minimal pytz stub
    pytz_module = types.ModuleType("pytz")
    class _TZ:
        def localize(self, dt, is_dst=None):
            return dt
        def normalize(self, dt):
            return dt
    pytz_module.timezone = lambda name: _TZ()
    pytz_module.AmbiguousTimeError = Exception
    pytz_module.NonExistentTimeError = Exception
    sys.modules.setdefault("pytz", pytz_module)

from keyboard_builders import build_student_submenu
from class_track_bot import build_application

EXPECTED = [
    "stu:LOG:{id}",
    "stu:CANCEL:{id}",
    "stu:RESHED:{id}",
    "stu:RENEW:{id}",
    "stu:LENGTH:{id}",
    "stu:EDIT:{id}",
    "stu:FREECREDIT:{id}",
    "stu:PAUSE:{id}",
    "stu:REMOVE:{id}",
    "stu:VIEW:{id}",
    "stu:ADHOC:{id}",
]


def main() -> int:
    app = build_application()
    found = False
    for group in getattr(app, "handlers", []):
        for handler in getattr(group, "handlers", []):
            if isinstance(handler, CallbackQueryHandler):
                pattern = getattr(getattr(handler, "pattern", None), "pattern", "")
                if pattern.startswith("^stu:"):
                    found = True
    if not found:
        print("missing stu handler")
        return 1
    fake_id = "123"
    markup = build_student_submenu(fake_id)
    callbacks = [
        b.callback_data
        for row in markup.inline_keyboard
        for b in row
    ]
    expected = [s.format(id=fake_id) for s in EXPECTED]
    for c in callbacks:
        print(c)
    if callbacks != expected:
        print("callback mismatch")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
