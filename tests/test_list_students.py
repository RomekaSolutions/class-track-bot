import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import class_track_bot as bot


def test_list_students_command(tmp_path, monkeypatch):
    data = {
        "1": {"name": "Alice", "telegram_handle": "alice", "paused": False},
        "2": {"name": "Bob", "telegram_id": 42, "paused": False},
        "3": {"name": "Charlie", "telegram_handle": "charlie", "paused": True},
    }
    students_file = tmp_path / "students.json"
    students_file.write_text(json.dumps(data))
    monkeypatch.setattr(bot, "STUDENTS_FILE", str(students_file))

    reply = AsyncMock()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=next(iter(bot.ADMIN_IDS))),
        message=SimpleNamespace(reply_text=reply),
    )
    context = SimpleNamespace()

    asyncio.run(bot.list_students_command(update, context))

    reply.assert_awaited()
    text = reply.call_args.args[0]
    assert "Alice (@alice)" in text
    assert "Bob (id 42)" in text
    assert "Charlie" not in text
