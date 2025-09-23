import asyncio
import json
import types

import data_store
from admin_flows import handle_student_action, STUDENT_NOT_FOUND_MSG


def test_invalid_student_callback(monkeypatch, tmp_path):
    students = {"1": {"name": "Alice"}}
    students_file = tmp_path / "students.json"
    students_file.write_text(json.dumps(students))
    monkeypatch.setattr(data_store, "STUDENTS_FILE", str(students_file))

    class DummyQuery:
        def __init__(self):
            self.data = "stu:VIEW:999"
            self.edited = None

        async def answer(self):
            pass

        async def edit_message_text(self, text, reply_markup=None):
            self.edited = text

    query = DummyQuery()
    update = types.SimpleNamespace(callback_query=query)
    asyncio.run(handle_student_action(update, types.SimpleNamespace()))
    assert query.edited == STUDENT_NOT_FOUND_MSG
