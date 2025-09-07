import json
import os
from typing import Optional, Dict, Any

STUDENTS_FILE = "students.json"


def resolve_student(student_id: str) -> Optional[Dict[str, Any]]:
    """Return the student record for ``student_id`` if available."""
    if not os.path.exists(STUDENTS_FILE):
        return None
    try:
        with open(STUDENTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None
    return data.get(str(student_id))
