import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import ask_tutor


def _make_session() -> ask_tutor.AskSession:
    return ask_tutor.AskSession(
        student_id="1",
        student_name="Alice",
        student_handle="@alice",
        chat_id=111,
        messages=[],
        message_ids=[],
    )


def test_collect_text_message(monkeypatch):
    session = _make_session()
    context = SimpleNamespace(user_data={"ask_tutor": session})
    reply = AsyncMock()
    message = SimpleNamespace(
        text="Hello",
        photo=None,
        voice=None,
        caption=None,
        message_id=42,
        reply_text=reply,
    )
    update = SimpleNamespace(message=message)

    asyncio.run(ask_tutor.collect_ask_message(update, context))

    assert session.messages == [{"type": "text", "text": "Hello"}]
    assert session.message_ids == [42]
    reply.assert_awaited_once()


def test_finish_requires_message(monkeypatch):
    session = _make_session()
    context = SimpleNamespace(user_data={"ask_tutor": session})
    reply = AsyncMock()
    message = SimpleNamespace(reply_text=reply)
    update = SimpleNamespace(message=message)

    result = asyncio.run(ask_tutor.finish_ask(update, context))

    assert result == ask_tutor.AskStates.COLLECTING
    reply.assert_awaited_once()


def test_confirm_send_persists_and_notifies(monkeypatch, tmp_path):
    asks_path = tmp_path / "asks.json"
    monkeypatch.setattr(ask_tutor, "ASKS_FILE", str(asks_path))
    session = _make_session()
    session.messages.append({"type": "text", "text": "Question"})
    session.message_ids.append(77)
    context = SimpleNamespace(user_data={"ask_tutor": session})
    query = SimpleNamespace(
        data="ask:confirm:1",
        message=SimpleNamespace(chat_id=555),
    )
    monkeypatch.setattr(ask_tutor, "try_ack", AsyncMock())
    safe_edit = AsyncMock()
    monkeypatch.setattr(ask_tutor, "safe_edit_or_send", safe_edit)
    notify = AsyncMock()
    monkeypatch.setattr(ask_tutor, "notify_admins_new_ask", notify)
    update = SimpleNamespace(callback_query=query)

    result = asyncio.run(ask_tutor.confirm_send_ask(update, context))

    assert result == ask_tutor.CONV_END
    safe_edit.assert_awaited_once()
    notify.assert_awaited_once()
    assert "ask_tutor" not in context.user_data

    with open(asks_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    assert len(data) == 1
    record = data[0]
    assert record["status"] == "new"
    assert record["messages"] == [{"type": "text", "text": "Question"}]
    assert record["origin"]["message_ids"] == [77]
