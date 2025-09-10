import json
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
import asyncio

import class_track_bot as bot


def setup_files(tmp_path):
    students = {
        "1": {"name": "Alice", "telegram_id": 1},
        "test_handle": {"name": "Tester"},
    }
    logs = [{"student": "@test_handle", "type": "note"}]
    sf = tmp_path / "students.json"
    lf = tmp_path / "logs.json"
    sf.write_text(json.dumps(students))
    lf.write_text(json.dumps(logs))
    return sf, lf


def test_resolveids_flow(tmp_path, monkeypatch):
    sf, lf = setup_files(tmp_path)
    monkeypatch.setattr(bot, "STUDENTS_FILE", str(sf))
    monkeypatch.setattr(bot, "LOGS_FILE", str(lf))

    students = bot.load_students()
    assert "test_handle" in students
    assert students["test_handle"].get("needs_id")

    # datacheck should list the flagged student
    reply = AsyncMock()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=next(iter(bot.ADMIN_IDS))),
        message=SimpleNamespace(reply_text=reply),
    )
    asyncio.run(bot.datacheck_command(update, SimpleNamespace()))
    assert "test_handle" in reply.call_args.args[0]

    # resolve ids
    get_chat = AsyncMock(return_value=SimpleNamespace(id=99))
    update2 = SimpleNamespace(
        effective_user=SimpleNamespace(id=next(iter(bot.ADMIN_IDS))),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )
    context2 = SimpleNamespace(application=SimpleNamespace(bot=SimpleNamespace(get_chat=get_chat)))
    asyncio.run(bot.resolveids_command(update2, context2))

    data = json.loads(sf.read_text())
    assert "99" in data and "test_handle" not in data
    assert data["99"].get("telegram_id") == 99
    assert "needs_id" not in data["99"]
    logs = json.loads(lf.read_text())
    assert logs[0]["student"] == "99"
    get_chat.assert_awaited_once_with("@test_handle")
