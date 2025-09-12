import os
import sys
import json
from datetime import datetime, timedelta, timezone

# Ensure repository root on path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import data_store


def _setup(monkeypatch, tmp_path, class_dates):
    students = {
        "1": {
            "name": "Test",
            "class_dates": class_dates,
            "classes_remaining": 3,
            "cancelled_dates": [],
        }
    }
    students_file = tmp_path / "students.json"
    logs_file = tmp_path / "logs.json"
    students_file.write_text(json.dumps(students))
    logs_file.write_text("[]")
    monkeypatch.setattr(data_store, "STUDENTS_FILE", str(students_file))
    monkeypatch.setattr(data_store, "LOGS_FILE", str(logs_file))
    return students_file, logs_file


def test_mark_class_completed(tmp_path, monkeypatch):
    now = datetime.utcnow()
    d1 = (now + timedelta(days=1)).isoformat()
    d2 = (now + timedelta(days=2)).isoformat()
    students_file, logs_file = _setup(monkeypatch, tmp_path, [d1, d2])
    data_store.mark_class_completed("1", d1)
    data = json.loads(students_file.read_text())
    stu = data["1"]
    assert d1 not in stu["class_dates"]
    assert stu["classes_remaining"] == 2
    logs = json.loads(logs_file.read_text())
    assert logs[-1]["type"] == "class_completed"
    assert logs[-1]["at"] == d1


def test_cancel_single_class_early(tmp_path, monkeypatch):
    now = datetime.utcnow()
    d1 = (now + timedelta(hours=48)).isoformat()
    students_file, logs_file = _setup(monkeypatch, tmp_path, [d1])
    data_store.cancel_single_class("1", d1, cutoff_hours=24)
    data = json.loads(students_file.read_text())
    stu = data["1"]
    assert d1 in stu["class_dates"]
    assert d1 in stu["cancelled_dates"]
    assert stu["classes_remaining"] == 3
    logs = json.loads(logs_file.read_text())
    assert logs[-1]["type"] == "class_cancelled"
    assert logs[-1]["is_late"] is False


def test_cancel_single_class_late(tmp_path, monkeypatch):
    now = datetime.utcnow()
    d1 = (now + timedelta(hours=1)).isoformat()
    students_file, logs_file = _setup(monkeypatch, tmp_path, [d1])
    data_store.cancel_single_class("1", d1, cutoff_hours=2)
    data = json.loads(students_file.read_text())
    stu = data["1"]
    assert d1 in stu["class_dates"]
    assert d1 in stu["cancelled_dates"]
    assert stu["classes_remaining"] == 2
    logs = json.loads(logs_file.read_text())
    assert logs[-1]["type"] == "class_cancelled"
    assert logs[-1]["is_late"] is True


def test_reschedule_single_class(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    old = (now + timedelta(days=1)).isoformat()
    new = (now + timedelta(days=3)).isoformat()
    students_file, logs_file = _setup(monkeypatch, tmp_path, [old])
    data_store.reschedule_single_class("1", old, new)
    data = json.loads(students_file.read_text())
    stu = data["1"]
    assert old not in stu["class_dates"]
    assert new in stu["class_dates"]
    logs = json.loads(logs_file.read_text())
    assert logs[-1]["type"] == "class_rescheduled"
    assert logs[-1]["from"] == old
    assert logs[-1]["to"] == new


def test_replace_class_date(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    old = (now + timedelta(days=1)).isoformat()
    new = (now + timedelta(days=2)).isoformat()
    students_file, _ = _setup(monkeypatch, tmp_path, [old])
    assert data_store.replace_class_date("1", old, new) is True
    data = json.loads(students_file.read_text())
    stu = data["1"]
    assert old not in stu["class_dates"]
    assert stu["class_dates"].count(new) == 1


def test_log_and_unlog_class(tmp_path, monkeypatch):
    logs_file = tmp_path / "logs.json"
    logs_file.write_text("[]")
    monkeypatch.setattr(data_store, "LOGS_FILE", str(logs_file))
    dt = "2025-01-01T10:00:00+00:00"
    data_store.log_class_status("1", dt, "completed")
    assert data_store.is_class_logged("1", dt)
    logs = json.loads(logs_file.read_text())
    assert logs[-1]["status"] == "completed"
    removed = data_store.remove_class_log("1", dt)
    assert removed is True
    assert not data_store.is_class_logged("1", dt)
