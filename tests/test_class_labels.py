import os
import sys
import types
from datetime import datetime, timedelta, timezone

# Ensure repository root on path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Stub minimal telegram modules before importing admin_flows
class InlineKeyboardButton:
    def __init__(self, text, callback_data=None):
        self.text = text
        self.callback_data = callback_data

class InlineKeyboardMarkup:
    def __init__(self, keyboard):
        self.inline_keyboard = keyboard

telegram_module = types.ModuleType("telegram")
for name, value in [
    ("InlineKeyboardButton", InlineKeyboardButton),
    ("InlineKeyboardMarkup", InlineKeyboardMarkup),
    ("Update", object),
]:
    setattr(telegram_module, name, value)
telegram_ext_module = types.ModuleType("telegram.ext")
telegram_ext_module.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)

sys.modules["telegram"] = telegram_module
sys.modules["telegram.ext"] = telegram_ext_module

import asyncio
import importlib
import admin_flows as af
importlib.reload(af)
admin_flows = af
from helpers import fmt_class_label
import pytest



class DummyQuery:
    def __init__(self):
        self.edited = []

    async def edit_message_text(self, text, reply_markup=None):
        self.edited.append((text, reply_markup))


@pytest.mark.parametrize(
    "func, action",
    [
        (admin_flows.wrap_log_class, "LOG"),
        (admin_flows.wrap_cancel_class, "CANCEL"),
        (admin_flows.wrap_reschedule_class, "RESHED"),
    ],
)
def test_class_keyboard_labels(func, action):
    iso = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    student = {"class_dates": [iso]}
    query = DummyQuery()

    asyncio.run(func(query, None, "1", student))

    # Capture the first button text and callback
    _, markup = query.edited[0]
    button = markup.inline_keyboard[0][0]
    assert button.text == fmt_class_label(iso)
    assert button.callback_data == f"cls:{action}:1:{iso}"


def test_fmt_class_label_example():
    iso = "2025-09-09T17:00:00+00:00"
    assert fmt_class_label(iso) == "Tue 09 Sep — 17:00"
