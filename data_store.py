import json
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple

from helpers import extract_weekly_pattern, generate_from_pattern

STUDENTS_FILE = "students.json"
LOGS_FILE = "logs.json"


def _normalise_handle(handle: Optional[str]) -> Optional[str]:
    """Return ``handle`` lowercased without a leading ``@``."""

    if handle is None:
        return None
    return str(handle).lstrip("@").lower()


def _normalise_student_record(key: str, student: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Return canonical ``(key, student)`` preserving unresolved handles."""

    if not isinstance(student, dict):
        return None

    tid = student.get("telegram_id")
    sid: Optional[str] = None
    if isinstance(tid, int) or (isinstance(tid, str) and str(tid).isdigit()):
        sid = str(int(tid))
    elif str(key).isdigit():
        sid = str(int(key))
        student["telegram_id"] = int(sid)

    if sid is None:
        # Preserve handle keyed students, flagging for resolution later
        handle = _normalise_handle(student.get("telegram_handle") or key)
        student["telegram_handle"] = handle
        student["needs_id"] = True
        return handle, student

    student["telegram_handle"] = _normalise_handle(student.get("telegram_handle"))
    return sid, student


def load_students() -> Dict[str, Any]:
    """Return the full students mapping from disk ensuring numeric keys."""
    if not os.path.exists(STUDENTS_FILE):
        return {}
    try:
        with open(STUDENTS_FILE, "r", encoding="utf-8-sig") as f:
            raw = json.load(f)
    except Exception:
        return {}

    cleaned: Dict[str, Any] = {}
    changed = False
    for key, student in (raw or {}).items():
        result = _normalise_student_record(str(key), student)
        if result is None:
            changed = True
            continue
        new_key, norm_student = result
        cleaned[new_key] = norm_student
        if new_key != str(key):
            changed = True
    if changed and cleaned:
        # Only persist migrations if there is non-empty data
        save_students(cleaned)
    return cleaned


def save_students(data: Dict[str, Any]) -> None:
    """Persist ``data`` to ``STUDENTS_FILE`` ensuring numeric keys."""

    if not data:
        logging.warning("Refusing to overwrite students.json with empty data")
        return

    cleaned: Dict[str, Any] = {}
    for key, student in list(data.items()):
        result = _normalise_student_record(str(key), student)
        if result is None:
            continue
        new_key, norm_student = result
        cleaned[new_key] = norm_student
    if not cleaned:
        logging.warning("Refusing to overwrite students.json with empty data")
        return
    tmp_path = f"{STUDENTS_FILE}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=2, sort_keys=True)
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                # fsync may not be available on some platforms; ignore if so
                pass
        os.replace(tmp_path, STUDENTS_FILE)
    except Exception as e:
        logging.error(
            "Failed to save students.json atomically; original file left unchanged: %s",
            e,
        )
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


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


def _resolve_student_id(value: Any, students: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Return numeric student id string for ``value`` if possible."""

    if value is None:
        return None
    sid = str(value)
    if sid.isdigit():
        return sid
    handle = _normalise_handle(sid)
    if students is None:
        students = load_students()
    for key, stu in students.items():
        if _normalise_handle(stu.get("telegram_handle")) == handle:
            return key
    return None


def load_logs() -> List[Dict[str, Any]]:
    """Return the list of log records from disk ensuring numeric IDs."""
    if not os.path.exists(LOGS_FILE):
        return []
    try:
        with open(LOGS_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return []

    students = load_students()
    cleaned: List[Dict[str, Any]] = []
    changed = False
    for entry in raw:
        sid = entry.get("student") or entry.get("student_id")
        resolved = _resolve_student_id(sid, students)
        if resolved is None:
            changed = True
            continue
        if sid != resolved:
            changed = True
        entry["student"] = resolved
        entry.pop("student_id", None)
        cleaned.append(entry)
    if changed:
        save_logs(cleaned)
    return cleaned


def save_logs(logs: List[Dict[str, Any]]) -> None:
    """Persist ``logs`` ensuring ``student`` fields are numeric."""

    students = load_students()
    cleaned: List[Dict[str, Any]] = []
    for entry in logs:
        sid = entry.get("student") or entry.get("student_id")
        resolved = _resolve_student_id(sid, students)
        if resolved is None:
            continue
        entry["student"] = resolved
        entry.pop("student_id", None)
        cleaned.append(entry)
    with open(LOGS_FILE, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, sort_keys=True)


def append_log(event: Dict[str, Any]) -> None:
    """Append an event to ``LOGS_FILE`` ensuring data integrity."""

    logs = load_logs()
    logs.append(event)
    save_logs(logs)


def migrate_log_schemas() -> int:
    """Add ``date`` field to logs that only have ``at``. Return migrated count."""

    logs = load_logs()
    migrated = 0

    for entry in logs:
        if "date" in entry:
            continue

        at_value = entry.get("at")
        if not at_value:
            continue

        try:
            dt = _parse_iso(str(at_value))
        except Exception:
            logging.warning("Failed to migrate log entry: %s", entry)
            continue

        entry["date"] = dt.isoformat()
        migrated += 1

    if migrated:
        save_logs(logs)

    return migrated


def migrate_student_records() -> None:
    """Rekey students and logs to use numeric Telegram IDs."""

    # --- migrate students ---
    raw_students: Dict[str, Any] = {}
    if os.path.exists(STUDENTS_FILE):
        try:
            with open(STUDENTS_FILE, "r", encoding="utf-8") as f:
                raw_students = json.load(f)
        except Exception:
            raw_students = {}

    cleaned_students: Dict[str, Any] = {}
    rekeyed = 0
    for key, student in raw_students.items():
        result = _normalise_student_record(str(key), student)
        if result is None:
            continue
        new_key, norm_student = result
        cleaned_students[new_key] = norm_student
        if new_key != str(key):
            rekeyed += 1

    with open(STUDENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(cleaned_students, f, indent=2, sort_keys=True)

    # --- migrate logs ---
    raw_logs: List[Dict[str, Any]] = []
    if os.path.exists(LOGS_FILE):
        try:
            with open(LOGS_FILE, "r", encoding="utf-8") as f:
                raw_logs = json.load(f)
        except Exception:
            raw_logs = []

    updated = 0
    skipped = 0
    migrated_logs: List[Dict[str, Any]] = []
    for entry in raw_logs:
        sid = entry.get("student") or entry.get("student_id")
        resolved = _resolve_student_id(sid, cleaned_students)
        if resolved is None:
            skipped += 1
            continue
        if sid != resolved:
            updated += 1
        entry["student"] = resolved
        entry.pop("student_id", None)
        migrated_logs.append(entry)

    with open(LOGS_FILE, "w", encoding="utf-8") as f:
        json.dump(migrated_logs, f, indent=2, sort_keys=True)

    print(f"Handles rekeyed: {rekeyed}")
    print(f"Logs updated: {updated}")
    if skipped:
        print(f"Logs skipped: {skipped}")


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
            "date": iso_dt,
            "status": "completed",
            "ts": datetime.utcnow().isoformat(),
        }
    )
    return True


def cancel_single_class(
    student_id: str,
    iso_dt: str,
    cutoff_hours: int,
    *,
    log: bool = True,
    request_time: Optional[datetime] = None,
) -> bool:
    """Cancel a single class, applying late deduction logic."""

    data = load_students()
    stu = data.get(str(student_id))
    if not stu:
        return False

    # Resolve the class time within the student's schedule
    class_dates: List[str] = list(stu.get("class_dates", []))
    target_iso: Optional[str] = None
    if iso_dt in class_dates:
        target_iso = iso_dt
    else:
        try:
            target_dt = datetime.fromisoformat(iso_dt)
        except Exception:
            target_dt = None
        if target_dt is not None:
            for existing in class_dates:
                try:
                    existing_dt = datetime.fromisoformat(existing)
                except Exception:
                    continue
                if existing_dt == target_dt:
                    target_iso = existing
                    break

    cancelled = list(stu.get("cancelled_dates", []))
    if target_iso is None and iso_dt not in cancelled:
        return False

    # Use timezone-aware UTC so comparisons with aware class datetimes are valid
    compare_time = request_time
    if compare_time is not None and compare_time.tzinfo is None:
        compare_time = compare_time.replace(tzinfo=timezone.utc)
    if compare_time is None:
        compare_time = datetime.now(timezone.utc)
    try:
        class_time = datetime.fromisoformat(target_iso or iso_dt)
    except Exception:
        class_time = datetime.fromisoformat(iso_dt)
    if class_time.tzinfo is None:
        class_time = class_time.replace(tzinfo=timezone.utc)
    is_late = compare_time > class_time - timedelta(hours=cutoff_hours)

    if compare_time > class_time:
        logging.warning(
            "Cancellation request time %s for student %s is after class time %s",
            compare_time.isoformat(),
            student_id,
            class_time.isoformat(),
        )

    if not is_late and target_iso is not None:
        class_dates = [d for d in class_dates if d != target_iso]
        if class_dates:
            try:
                pattern = extract_weekly_pattern(class_dates)
            except Exception:  # pragma: no cover - defensive guard
                pattern = []
            if pattern:
                valid_dates: List[datetime] = []
                for iso in class_dates:
                    try:
                        dt_obj = datetime.fromisoformat(iso)
                    except Exception:
                        continue
                    valid_dates.append(dt_obj)
                if valid_dates:
                    last_date = max(valid_dates)
                    try:
                        generated = generate_from_pattern(
                            anchor=last_date, pattern=pattern, count=1
                        )
                    except Exception:
                        logging.warning(
                            "Failed to generate replacement class for student %s at %s",
                            student_id,
                            iso_dt,
                            exc_info=True,
                        )
                    else:
                        for new_dt in generated:
                            new_iso = new_dt.isoformat()
                            if new_iso not in class_dates:
                                class_dates.append(new_iso)
                                break
        class_dates.sort()

    if iso_dt not in cancelled:
        cancelled.append(iso_dt)

    stu["class_dates"] = class_dates
    stu["cancelled_dates"] = cancelled

    if is_late:
        stu["classes_remaining"] = max(0, stu.get("classes_remaining", 0) - 1)

    data[str(student_id)] = stu
    save_students(data)

    if log:
        append_log(
            {
                "type": "class_cancelled",
                "student_id": student_id,
                "at": iso_dt,
                "date": iso_dt,
                "status": "cancelled_late" if is_late else "cancelled_early",
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
            "at": new_iso,
            "date": new_iso,
            "status": "rescheduled",
            "from": old_iso,
            "to": new_iso,
            "ts": datetime.utcnow().isoformat(),
        }
    )
    return True
