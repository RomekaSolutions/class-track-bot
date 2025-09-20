import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import class_track_bot as bot


def test_initiate_cancel_class_paused(monkeypatch):
    sender = AsyncMock()
    monkeypatch.setattr(bot, "safe_edit_or_send", sender)
    student = {"name": "Alice", "paused": True}

    asyncio.run(bot.initiate_cancel_class(SimpleNamespace(), student))

    sender.assert_awaited_once()
    assert sender.call_args.args[1] == bot.PAUSED_ACTION_MESSAGE


def test_show_notification_settings_paused(monkeypatch):
    sender = AsyncMock()
    monkeypatch.setattr(bot, "safe_edit_or_send", sender)
    student = {"name": "Alice", "paused": True}

    asyncio.run(bot.show_notification_settings(SimpleNamespace(), student))

    sender.assert_awaited_once()
    assert sender.call_args.args[1] == bot.PAUSED_SETTINGS_MESSAGE


def test_update_notification_setting_paused(monkeypatch):
    sender = AsyncMock()
    monkeypatch.setattr(bot, "safe_edit_or_send", sender)
    student = {"name": "Alice", "paused": True, "reminder_offset_minutes": 15}
    students = {"1": student}

    asyncio.run(
        bot.update_notification_setting(
            SimpleNamespace(), "1", student, students, 30, SimpleNamespace()
        )
    )

    sender.assert_awaited_once()
    assert sender.call_args.args[1] == bot.PAUSED_SETTINGS_MESSAGE
    assert student.get("reminder_offset_minutes") == 15


def test_handle_cancel_selection_paused(monkeypatch):
    students = {
        "1": {
            "name": "Alice",
            "telegram_id": 1,
            "class_dates": ["2999-01-01T12:00:00+07:00"],
            "paused": True,
        }
    }
    monkeypatch.setattr(bot, "load_students", lambda: students)
    save_mock = Mock()
    monkeypatch.setattr(bot, "save_students", save_mock)
    refresh_mock = AsyncMock()
    monkeypatch.setattr(bot, "refresh_student_menu", refresh_mock)
    sender = AsyncMock()
    monkeypatch.setattr(bot, "safe_edit_or_send", sender)

    query = SimpleNamespace(
        data="cancel_selected:0",
        from_user=SimpleNamespace(id=1, username="alice"),
        answer=AsyncMock(),
        message=SimpleNamespace(),
    )
    query.answer.return_value = None
    update = SimpleNamespace(callback_query=query)

    asyncio.run(bot.handle_cancel_selection(update, SimpleNamespace()))

    sender.assert_awaited_once()
    assert sender.call_args.args[1] == bot.PAUSED_ACTION_MESSAGE
    assert "pending_cancel" not in students["1"]
    save_mock.assert_not_called()
    refresh_mock.assert_not_awaited()
