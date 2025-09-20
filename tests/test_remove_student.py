import asyncio
import json
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import class_track_bot as bot


def setup_files(tmp_path, data):
    students_file = tmp_path / "students.json"
    students_file.write_text(json.dumps(data))
    logs_file = tmp_path / "logs.json"
    logs_file.write_text("[]")
    return students_file, logs_file


def make_update(reply):
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=next(iter(bot.ADMIN_IDS))),
        message=SimpleNamespace(reply_text=reply),
    )


def make_context(args):
    return SimpleNamespace(
        args=args,
        application=SimpleNamespace(job_queue=SimpleNamespace(jobs=lambda: [])),
    )


def test_remove_student_keeps_other_students(tmp_path, monkeypatch):
    data = {
        "100": {"name": "A1", "telegram_id": 100, "telegram_handle": "alice", "paused": False},
        "101": {"name": "A3", "telegram_id": 101, "telegram_handle": "alice", "paused": False},
    }
    students_file, logs_file = setup_files(tmp_path, data)
    monkeypatch.setattr(bot, "STUDENTS_FILE", str(students_file))
    monkeypatch.setattr(bot, "LOGS_FILE", str(logs_file))

    reply = AsyncMock()
    update = make_update(reply)
    context = make_context(["100", "confirm"])

    asyncio.run(bot.remove_student_command(update, context))

    students = json.loads(students_file.read_text())
    assert "100" not in students
    assert "101" in students


def test_remove_student_purge(tmp_path, monkeypatch):
    data = {
        "100": {"name": "A1", "telegram_id": 100, "telegram_handle": "alice", "paused": False},
        "101": {"name": "A3", "telegram_id": 101, "telegram_handle": "alice", "paused": False},
    }
    students_file, logs_file = setup_files(tmp_path, data)
    monkeypatch.setattr(bot, "STUDENTS_FILE", str(students_file))
    monkeypatch.setattr(bot, "LOGS_FILE", str(logs_file))

    reply = AsyncMock()
    update = make_update(reply)
    context = make_context(["100", "confirm", "purge"])

    asyncio.run(bot.remove_student_command(update, context))

    students = json.loads(students_file.read_text())
    assert students == {}
