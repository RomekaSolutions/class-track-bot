import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import class_track_bot as ctb


def make_update(reply):
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=next(iter(ctb.ADMIN_IDS))),
        message=SimpleNamespace(reply_text=reply),
    )


def make_context():
    return SimpleNamespace()


def test_fixlogs_command_normalizes_and_drops(tmp_path, monkeypatch):
    students = {
        "1": {"name": "Alice", "telegram_id": 1, "telegram_handle": "alice"},
        "2": {"name": "Bob", "telegram_id": 2, "telegram_handle": "bob"},
    }
    logs = [
        {"student": "@Alice", "date": "2024-01-01", "status": "completed"},
        {"student": "2", "date": "2024-01-02", "status": "completed"},
        {"student": "unknown", "date": "2024-01-03", "status": "missed"},
    ]
    students_file = tmp_path / "students.json"
    logs_file = tmp_path / "logs.json"
    students_file.write_text(json.dumps(students))
    logs_file.write_text(json.dumps(logs))
    monkeypatch.setattr(ctb, "STUDENTS_FILE", str(students_file))
    monkeypatch.setattr(ctb, "LOGS_FILE", str(logs_file))
    monkeypatch.setattr(ctb, "load_logs", lambda: json.loads(logs_file.read_text()))

    reply = AsyncMock()
    update = make_update(reply)
    context = make_context()

    asyncio.run(ctb.fixlogs_command(update, context))

    reply.assert_awaited()
    text = reply.call_args.args[0]
    assert "Total logs processed: 3" in text
    assert "Rewritten: 1" in text
    assert "Dropped: 1" in text

    saved = json.loads(logs_file.read_text())
    assert len(saved) == 2
    assert saved[0]["student"] == "1"
    assert saved[1]["student"] == "2"


def test_fixlogs_command_all_clean(tmp_path, monkeypatch):
    students = {
        "1": {"name": "Alice", "telegram_id": 1},
        "2": {"name": "Bob", "telegram_id": 2},
    }
    logs = [
        {"student": "1", "date": "2024-01-01", "status": "completed"},
        {"student": "2", "date": "2024-01-02", "status": "completed"},
    ]
    students_file = tmp_path / "students.json"
    logs_file = tmp_path / "logs.json"
    students_file.write_text(json.dumps(students))
    logs_file.write_text(json.dumps(logs))
    monkeypatch.setattr(ctb, "STUDENTS_FILE", str(students_file))
    monkeypatch.setattr(ctb, "LOGS_FILE", str(logs_file))
    monkeypatch.setattr(ctb, "load_logs", lambda: json.loads(logs_file.read_text()))

    reply = AsyncMock()
    update = make_update(reply)
    context = make_context()

    asyncio.run(ctb.fixlogs_command(update, context))

    reply.assert_awaited()
    text = reply.call_args.args[0]
    assert "All logs already clean ✅" in text
    saved = json.loads(logs_file.read_text())
    assert saved == logs
