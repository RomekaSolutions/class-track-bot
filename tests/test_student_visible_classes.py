import os
import sys
import types
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.dirname(os.path.dirname(__file__)))


class _Filter:
    def __init__(self, name: str):
        self.name = name

    def __and__(self, other):  # pragma: no cover - simple stub behaviour
        return self

    def __rand__(self, other):  # pragma: no cover - simple stub behaviour
        return self

    def __invert__(self):  # pragma: no cover - simple stub behaviour
        return self


def _ensure_telegram_stubs():
    if "telegram" in sys.modules:
        return

    class InlineKeyboardButton:
        def __init__(self, text, callback_data=None):
            self.text = text
            self.callback_data = callback_data

    class InlineKeyboardMarkup:
        def __init__(self, keyboard):
            self.inline_keyboard = keyboard

    class ReplyKeyboardMarkup:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class ReplyKeyboardRemove:
        pass

    class KeyboardButton:
        def __init__(self, text):
            self.text = text

    class Update:  # pragma: no cover - placeholder
        pass

    telegram = types.ModuleType("telegram")
    telegram.InlineKeyboardButton = InlineKeyboardButton
    telegram.InlineKeyboardMarkup = InlineKeyboardMarkup
    telegram.ReplyKeyboardMarkup = ReplyKeyboardMarkup
    telegram.ReplyKeyboardRemove = ReplyKeyboardRemove
    telegram.KeyboardButton = KeyboardButton
    telegram.Update = Update

    class CommandHandler:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class CallbackQueryHandler:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class MessageHandler:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class ConversationHandler:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class Application:
        def __init__(self):
            self.handlers = []

        def add_handler(self, handler, group=0):  # pragma: no cover - simple stub
            self.handlers.append((group, handler))

    class ApplicationBuilder:
        def __init__(self):
            self._token = None

        def token(self, token):
            self._token = token
            return self

        def build(self):
            return Application()

    class JobQueue:  # pragma: no cover - placeholder
        pass

    filters_module = types.SimpleNamespace(TEXT=_Filter("TEXT"), COMMAND=_Filter("COMMAND"))

    telegram_ext = types.ModuleType("telegram.ext")
    telegram_ext.CommandHandler = CommandHandler
    telegram_ext.CallbackQueryHandler = CallbackQueryHandler
    telegram_ext.MessageHandler = MessageHandler
    telegram_ext.ConversationHandler = ConversationHandler
    telegram_ext.Application = Application
    telegram_ext.ApplicationBuilder = ApplicationBuilder
    telegram_ext.JobQueue = JobQueue
    telegram_ext.filters = filters_module
    telegram_ext.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)

    sys.modules["telegram"] = telegram
    sys.modules["telegram.ext"] = telegram_ext

    if "pytz" not in sys.modules:
        pytz_module = types.ModuleType("pytz")

        class _TZ:
            def localize(self, dt, is_dst=None):  # pragma: no cover - stub behaviour
                return dt

            def normalize(self, dt):  # pragma: no cover - stub behaviour
                return dt

        pytz_module.timezone = lambda name: _TZ()
        pytz_module.AmbiguousTimeError = Exception
        pytz_module.NonExistentTimeError = Exception
        sys.modules["pytz"] = pytz_module


_ensure_telegram_stubs()

import class_track_bot as ctb


def _build_student(classes_remaining: int, *, premium: bool = False):
    base = datetime.now(timezone.utc).replace(microsecond=0)
    class_dates = [
        (base + timedelta(days=offset)).isoformat()
        for offset in range(1, 5)
    ]
    student = {
        "name": "Student",
        "class_dates": class_dates,
        "classes_remaining": classes_remaining,
        "cancelled_dates": [],
    }
    if premium:
        student["premium"] = True
    return student, class_dates


def test_student_visible_classes_respects_remaining():
    student, class_dates = _build_student(classes_remaining=2)
    visible = ctb.get_student_visible_classes(student, count=5)
    assert len(visible) == 2
    expected = [ctb.ensure_bangkok(dt) for dt in class_dates[:2]]
    assert visible == expected


def test_student_visible_classes_zero_remaining():
    student, _ = _build_student(classes_remaining=0)
    visible = ctb.get_student_visible_classes(student, count=5)
    assert visible == []


def test_student_visible_classes_premium_unlimited():
    student, class_dates = _build_student(classes_remaining=1, premium=True)
    visible = ctb.get_student_visible_classes(student, count=3)
    assert len(visible) == 3
    expected = [ctb.ensure_bangkok(dt) for dt in class_dates[:3]]
    assert visible == expected


def test_student_cancellable_classes_respects_remaining():
    student, class_dates = _build_student(classes_remaining=1)
    cancellable = ctb.get_student_cancellable_classes(student)
    assert len(cancellable) == 1
    expected = [ctb.ensure_bangkok(dt) for dt in class_dates[:1]]
    assert cancellable == expected


def test_student_cancellable_classes_premium_all_classes():
    student, class_dates = _build_student(classes_remaining=1, premium=True)
    cancellable = ctb.get_student_cancellable_classes(student)
    assert len(cancellable) == len(class_dates)
    expected = [ctb.ensure_bangkok(dt) for dt in class_dates]
    assert cancellable == expected
