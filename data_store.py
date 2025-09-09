import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

STUDENTS_FILE = "students.json"
LOGS_FILE = "logs.json"


def load_students() -> Dict[str, Any]:
    """Return the full students mapping from disk."""
    if not os.path.exists(STUDENTS_FILE):
        return {}
    try:
        with open(STUDENTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_students(data: Dict[str, Any]) -> None:
    """Persist ``data`` to ``STUDENTS_FILE``."""
    with open(STUDENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def get_student_by_id(student_id: str, safe: bool = True) -> Optional[Dict[str, Any]]:
    """Return student dict for ``student_id``.

    If ``safe`` is ``True`` (default) then ``None`` is returned when the ID is
    missing.  Otherwise a ``KeyError`` is raised.
    """

    data = load_students()
    try:
        return data[str(student_id)]
    except KeyError:
        if safe:
            return None
        raise


def load_logs() -> List[Dict[str, Any]]:
    """Return the list of log records from disk."""
    if not os.path.exists(LOGS_FILE):
        return []
    try:
        with open(LOGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def append_log(event: Dict[str, Any]) -> None:
    """Append an event to ``LOGS_FILE``."""
    logs: List[Dict[str, Any]] = []
    if os.path.exists(LOGS_FILE):
        try:
            with open(LOGS_FILE, "r", encoding="utf-8") as f:
                logs = json.load(f)
        except Exception:
            logs = []
    logs.append(event)
    with open(LOGS_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, sort_keys=True)


def _parse_iso(dt_str: str) -> datetime:
    """Return timezone-aware datetime from ``dt_str``."""
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_class_logged(
    student_id: str, iso_dt: str, logs: Optional[List[Dict[str, Any]]] = None
) -> bool:
    """Return True if a class log exists for ``student_id`` at ``iso_dt``.

    Only class-related statuses are considered. ``logs`` may be provided to avoid
    reloading the file repeatedly.
    """

    target = _parse_iso(iso_dt)
    sid = str(student_id)
    if logs is None:
        logs = load_logs()
    for entry in logs:
        entry_sid = str(entry.get("student") or entry.get("student_id") or "")
        if entry_sid != sid:
            continue
        status = (entry.get("status") or entry.get("type") or "").lower()
        if status.startswith("class_"):
            status = status[6:]
        if status not in {
            "completed",
            "cancelled_early",
            "cancelled_late",
            "rescheduled",
            "removed",
        }:
            continue
        dt_val = entry.get("date") or entry.get("at")
        if not dt_val:
            continue
        try:
            if _parse_iso(dt_val) == target:
                return True
        except Exception:
            continue
    return False


def log_class_status(student_id: str, iso_dt: str, status: str) -> None:
    """Append a class status entry to ``logs.json``."""
    aware = _parse_iso(iso_dt).isoformat()
    append_log(
        {
            "student_id": student_id,
            "date": aware,
            "status": status,
            "ts": datetime.utcnow().isoformat(),
        }
    )


def remove_class_log(student_id: str, iso_dt: str) -> bool:
    """Remove log matching ``student_id`` and ``iso_dt``. Return True if removed."""
    sid = str(student_id)
    target = _parse_iso(iso_dt)
    logs = load_logs()
    new_logs: List[Dict[str, Any]] = []
    removed = False
    for entry in logs:
        entry_sid = str(entry.get("student") or entry.get("student_id") or "")
        dt_val = entry.get("date") or entry.get("at")
        if entry_sid == sid and dt_val:
            try:
                if _parse_iso(dt_val) == target:
                    removed = True
                    continue
            except Exception:
                pass
        new_logs.append(entry)
    if removed:
        with open(LOGS_FILE, "w", encoding="utf-8") as f:
            json.dump(new_logs, f, indent=2, sort_keys=True)
    return removed

def resolve_student(student_id: str) -> Optional[Dict[str, Any]]:
    """Backward-compatible wrapper for ``get_student_by_id``."""

    return get_student_by_id(student_id, safe=True)


def replace_class_date(student_id: str, old_iso: str, new_iso: str) -> bool:
    """Replace ``old_iso`` with ``new_iso`` in a student's schedule.

    Datetime strings must match exactly, including timezone information. The
    operation is atomic – the old datetime is removed only if the new one is
    inserted. Returns ``True`` on success and ``False`` otherwise.
    """

    data = load_students()
    stu = data.get(str(student_id))
    if not stu:
        return False

    try:
        old_dt = _parse_iso(old_iso)
        new_dt = _parse_iso(new_iso)
    except Exception:
        return False

    dates = [_parse_iso(d) for d in stu.get("class_dates", [])]
    try:
        dates.remove(old_dt)
    except ValueError:
        return False

    if new_dt not in dates:
        dates.append(new_dt)
    dates.sort()

    stu["class_dates"] = [d.isoformat() for d in dates]
    data[str(student_id)] = stu
    save_students(data)
    return True


def mark_class_completed(student_id: str, iso_dt: str) -> bool:
    """Mark a class as completed for ``student_id``."""
    data = load_students()
    stu = data.get(str(student_id))
    if not stu:
        return False
    dates = stu.get("class_dates", [])
    if iso_dt in dates:
        dates.remove(iso_dt)
    stu["class_dates"] = dates
    remaining = max(0, stu.get("classes_remaining", 0) - 1)
    stu["classes_remaining"] = remaining
    data[str(student_id)] = stu
    save_students(data)
    append_log(
        {
            "type": "class_completed",
            "student_id": student_id,
            "at": iso_dt,
            "ts": datetime.utcnow().isoformat(),
        }
    )
    return True


def cancel_single_class(student_id: str, iso_dt: str, cutoff_hours: int) -> bool:
    """Cancel a single class, applying late deduction logic."""
    data = load_students()
    stu = data.get(str(student_id))
    if not stu:
        return False
    now = datetime.utcnow()
    class_time = datetime.fromisoformat(iso_dt)
    is_late = now > class_time - timedelta(hours=cutoff_hours)
    dates = stu.get("class_dates", [])
    if iso_dt in dates:
        dates.remove(iso_dt)
    stu["class_dates"] = dates
    cancelled = stu.get("cancelled_dates", [])
    cancelled.append(iso_dt)
    stu["cancelled_dates"] = cancelled
    if is_late:
        stu["classes_remaining"] = max(0, stu.get("classes_remaining", 0) - 1)
    data[str(student_id)] = stu
    save_students(data)
    append_log(
        {
            "type": "class_cancelled",
            "student_id": student_id,
            "at": iso_dt,
            "is_late": is_late,
            "ts": datetime.utcnow().isoformat(),
        }
    )
    return True


def reschedule_single_class(student_id: str, old_iso: str, new_iso: str) -> bool:
    """Reschedule one class from ``old_iso`` to ``new_iso``."""
    if not replace_class_date(student_id, old_iso, new_iso):
        return False
    append_log(
        {
            "type": "class_rescheduled",
            "student_id": student_id,
            "from": old_iso,
            "to": new_iso,
            "ts": datetime.utcnow().isoformat(),
        }
    )
    return True
