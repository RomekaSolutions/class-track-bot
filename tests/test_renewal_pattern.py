import types
import sys
import os
from datetime import datetime, timedelta, timezone, tzinfo

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

class InlineKeyboardButton:
    def __init__(self, text, callback_data=None):
        self.text = text
        self.callback_data = callback_data

class InlineKeyboardMarkup:
    def __init__(self, keyboard):
        self.inline_keyboard = keyboard

telegram_module = types.ModuleType("telegram")
telegram_module.InlineKeyboardButton = InlineKeyboardButton
telegram_module.InlineKeyboardMarkup = InlineKeyboardMarkup
telegram_module.Update = object
sys.modules["telegram"] = telegram_module

telegram_ext_module = types.ModuleType("telegram.ext")
telegram_ext_module.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
sys.modules["telegram.ext"] = telegram_ext_module

import asyncio
import admin_flows as af


def _make_history_logs(start_monday, start_thursday, weeks=4):
    logs = []
    for i in range(weeks):
        logs.append(
            {
                "student_id": "1",
                "type": "class_completed",
                "at": (start_monday + timedelta(weeks=i)).isoformat(),
            }
        )
        logs.append(
            {
                "student_id": "1",
                "type": "class_completed",
                "at": (start_thursday + timedelta(weeks=i)).isoformat(),
            }
        )
    return logs


def test_weekly_pattern_reuse(monkeypatch):
    monday = datetime(2025, 1, 6, 18, 0, tzinfo=timezone.utc)
    thursday = datetime(2025, 1, 9, 17, 0, tzinfo=timezone.utc)
    logs = _make_history_logs(monday, thursday)
    monkeypatch.setattr(af.data_store, "load_logs", lambda: logs)
    student = {"name": "A", "classes_remaining": 0, "class_dates": [], "cancelled_dates": []}
    monkeypatch.setattr(af.data_store, "get_student_by_id", lambda sid: student)
    students = {"1": student.copy()}
    monkeypatch.setattr(af.data_store, "load_students", lambda: students)
    saved = {}
    monkeypatch.setattr(af.data_store, "save_students", lambda s: saved.update(s))
    monkeypatch.setattr(af.data_store, "append_log", lambda e: None)
    monkeypatch.setattr(af.keyboard_builders, "build_student_detail_view", lambda sid, stu: ("ok", None))

    class DummyQuery:
        def __init__(self, data):
            self.data = data
            self.edited = None
        async def answer(self):
            pass
        async def edit_message_text(self, text, reply_markup=None):
            self.edited = text

    query = DummyQuery("cfm:RENEW:1:8")
    update = types.SimpleNamespace(callback_query=query)
    asyncio.run(af.renew_confirm(update, types.SimpleNamespace()))
    stu = saved["1"]
    assert len(stu["class_dates"]) == 8
    first = datetime.fromisoformat(stu["class_dates"][0])
    second = datetime.fromisoformat(stu["class_dates"][1])
    assert first.weekday() == 0 and first.hour == 18
    assert second.weekday() == 3 and second.hour == 17
    assert stu["cancelled_dates"] == []


def test_no_pattern_fallback(monkeypatch):
    base = datetime(2025, 1, 6, 18, 0, tzinfo=timezone.utc)
    logs = [
        {
            "student_id": "1",
            "type": "class_completed",
            "at": (base + timedelta(days=i, hours=i)).isoformat(),
        }
        for i in range(8)
    ]
    monkeypatch.setattr(af.data_store, "load_logs", lambda: logs)
    student = {"name": "A", "classes_remaining": 0, "class_dates": [], "cancelled_dates": []}
    monkeypatch.setattr(af.data_store, "get_student_by_id", lambda sid: student)
    monkeypatch.setattr(af.data_store, "load_students", lambda: {"1": student})
    monkeypatch.setattr(af.data_store, "save_students", lambda s: None)
    monkeypatch.setattr(af.data_store, "append_log", lambda e: None)
    monkeypatch.setattr(af.keyboard_builders, "build_student_detail_view", lambda sid, stu: ("ok", None))

    class DummyQuery:
        def __init__(self, data):
            self.data = data
            self.edited = None
        async def answer(self):
            pass
        async def edit_message_text(self, text, reply_markup=None):
            self.edited = text

    query = DummyQuery("cfm:RENEW:1:5")
    update = types.SimpleNamespace(callback_query=query)
    asyncio.run(af.renew_confirm(update, types.SimpleNamespace()))
    assert query.edited.startswith("No prior weekly pattern found")


def test_view_student_incomplete(monkeypatch):
    class DummyQuery:
        def __init__(self):
            self.edited = None
        async def edit_message_text(self, text, reply_markup=None):
            self.edited = text

    query = DummyQuery()
    asyncio.run(af.wrap_view_student(query, None, "1", {"name": "A"}))
    assert "Student record incomplete" in query.edited


def test_renewal_broken_record_not_saved(monkeypatch):
    monday = datetime(2025, 1, 6, 18, 0, tzinfo=timezone.utc)
    thursday = datetime(2025, 1, 9, 17, 0, tzinfo=timezone.utc)
    logs = _make_history_logs(monday, thursday)
    monkeypatch.setattr(af.data_store, "load_logs", lambda: logs)

    student = {
        "name": "A",
        "classes_remaining": 0,
        "class_dates": [],
        "cancelled_dates": "oops",
    }
    monkeypatch.setattr(af.data_store, "get_student_by_id", lambda sid: student)
    students = {"1": student.copy()}
    monkeypatch.setattr(af.data_store, "load_students", lambda: students)
    saved = {}
    monkeypatch.setattr(af.data_store, "save_students", lambda s: saved.update(s))
    monkeypatch.setattr(af.data_store, "append_log", lambda e: None)
    monkeypatch.setattr(
        af.keyboard_builders, "build_student_detail_view", lambda sid, stu: ("ok", None)
    )

    class DummyQuery:
        def __init__(self, data):
            self.data = data
            self.edited = None

        async def answer(self):
            pass

        async def edit_message_text(self, text, reply_markup=None):
            self.edited = text

    query = DummyQuery("cfm:RENEW:1:5")
    update = types.SimpleNamespace(callback_query=query)
    asyncio.run(af.renew_confirm(update, types.SimpleNamespace()))

    assert saved == {}
    assert student["classes_remaining"] == 0
    assert query.edited.startswith("Student record invalid")
