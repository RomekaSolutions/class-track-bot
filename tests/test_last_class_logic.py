import json
from datetime import datetime, timedelta, timezone

import data_store
import class_track_bot as ctb


def _setup(tmp_path, monkeypatch):
    students = {
        "1": {
            "name": "Stu",
            "class_dates": [
                (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
                (datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
            ],
            "classes_remaining": 2,
            "cancelled_dates": [],
        }
    }
    students_file = tmp_path / "students.json"
    students_file.write_text(json.dumps(students))
    logs_file = tmp_path / "logs.json"
    logs_file.write_text("[]")
    monkeypatch.setattr(data_store, "STUDENTS_FILE", str(students_file))
    monkeypatch.setattr(ctb, "STUDENTS_FILE", str(students_file))
    monkeypatch.setattr(data_store, "LOGS_FILE", str(logs_file))
    monkeypatch.setattr(ctb, "LOGS_FILE", str(logs_file))
    return students


def test_get_last_class_updates(monkeypatch, tmp_path):
    students = _setup(tmp_path, monkeypatch)
    stu = students["1"]
    last = ctb.get_last_class(stu)
    assert last is not None
    first_dt = stu["class_dates"][0]
    # cancel early -> remaining unchanged
    class FakeNow(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromisoformat(first_dt) - timedelta(days=1)
    monkeypatch.setattr(data_store, "datetime", FakeNow)
    data_store.cancel_single_class("1", first_dt, cutoff_hours=24)
    stu = data_store.get_student_by_id("1")
    assert stu["classes_remaining"] == 2
    # complete class
    data_store.mark_class_completed("1", stu["class_dates"][0])
    stu = data_store.get_student_by_id("1")
    assert stu["classes_remaining"] == 1
    # last class shifts accordingly
    last2 = ctb.get_last_class(stu)
    assert last2.isoformat() == stu["class_dates"][-1]
