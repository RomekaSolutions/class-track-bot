import json
import pathlib
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter

BKK = timezone(timedelta(hours=7))
students = json.loads(pathlib.Path('students.json').read_text(encoding='utf-8'))
logs = json.loads(pathlib.Path('logs.json').read_text(encoding='utf-8'))

def parse_datetime(value):
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = None
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(value, fmt)
                    break
                except ValueError:
                    continue
        if dt is None:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BKK)
    else:
        dt = dt.astimezone(BKK)
    return dt

today = datetime.now(BKK).date()

student_info = {}
schedule_parse_errors = []
schedule_naive_strings = []
for sid, student in students.items():
    info = {
        "name": student.get("name") or "",
        "class_datetimes": [],
        "class_counts_by_date": Counter(),
        "cancelled_counts_by_date": Counter(),
        "class_duplicates": Counter(),
    }
    for raw in student.get("class_dates", []):
        if isinstance(raw, str) and "T" in raw and "+" not in raw and "Z" not in raw:
            schedule_naive_strings.append({"student": sid, "name": student.get("name"), "value": raw, "source": "class_dates"})
        dt = parse_datetime(raw)
        if not dt:
            schedule_parse_errors.append({"student": sid, "name": student.get("name"), "source": "class_dates", "value": raw})
            continue
        date_str = dt.date().isoformat()
        info["class_datetimes"].append(dt)
        info["class_counts_by_date"][date_str] += 1
        info["class_duplicates"][dt.isoformat()] += 1
    for raw in student.get("cancelled_dates", []):
        if isinstance(raw, str) and "T" in raw and "+" not in raw and "Z" not in raw:
            schedule_naive_strings.append({"student": sid, "name": student.get("name"), "value": raw, "source": "cancelled_dates"})
        dt = parse_datetime(raw)
        if not dt:
            schedule_parse_errors.append({"student": sid, "name": student.get("name"), "source": "cancelled_dates", "value": raw})
            continue
        date_str = dt.date().isoformat()
        info["cancelled_counts_by_date"][date_str] += 1
    student_info[sid] = info

global_schedule_dates = set()
all_schedule_dates = set()
for sid, info in student_info.items():
    for date_str in info["class_counts_by_date"]:
        global_schedule_dates.add((sid, date_str))
        all_schedule_dates.add(date_str)
    for date_str in info["cancelled_counts_by_date"]:
        global_schedule_dates.add((sid, date_str))
        all_schedule_dates.add(date_str)

class_event_keywords = ("complete", "cancel", "resched", "remove", "missed", "no show")
logs_by_student_date = defaultdict(list)
class_logs_by_student_date = defaultdict(list)
orphan_student_logs = []
log_parse_errors = []
log_naive_strings = []
logs_missing_required = []

for idx, entry in enumerate(logs):
    missing_keys = [key for key in ("date", "status", "student") if key not in entry]
    if missing_keys:
        logs_missing_required.append({"index": idx, "missing": missing_keys, "entry": entry})
    student = entry.get("student")
    status = entry.get("status")
    raw_date = entry.get("date")
    if not student or student not in students:
        orphan_student_logs.append({"index": idx, "student": student, "status": status, "date": raw_date})
    if isinstance(raw_date, str) and "T" in raw_date and "+" not in raw_date and "Z" not in raw_date:
        log_naive_strings.append({"index": idx, "student": student, "status": status, "date": raw_date})
    dt = parse_datetime(raw_date) if raw_date else None
    if not dt:
        log_parse_errors.append({"index": idx, "student": student, "status": status, "date": raw_date})
        continue
    date_str = dt.date().isoformat()
    logs_by_student_date[(student, date_str)].append({"index": idx, "status": status, "raw_date": raw_date})
    if isinstance(status, str) and any(keyword in status.lower() for keyword in class_event_keywords):
        class_logs_by_student_date[(student, date_str)].append({"index": idx, "status": status, "raw_date": raw_date})

orphan_date_logs = []
orphan_date_logs_global = []
for (student, date_str), entries in class_logs_by_student_date.items():
    if (student, date_str) not in global_schedule_dates:
        for item in entries:
            orphan_date_logs.append({"student": student, "date": date_str, "status": item["status"], "raw_date": item["raw_date"], "index": item["index"]})
    if date_str not in all_schedule_dates:
        for item in entries:
            orphan_date_logs_global.append({"student": student, "date": date_str, "status": item["status"], "raw_date": item["raw_date"], "index": item["index"]})

missing_logs = []
student_completion_stats = []
students_with_past_no_logs = []
for sid, info in student_info.items():
    expected_total = 0
    actual_total = 0
    past_dates = set()
    for date_str, count in info["class_counts_by_date"].items():
        dt_date = datetime.fromisoformat(date_str).date()
        if dt_date > today:
            continue
        expected_total += count
        past_dates.add(date_str)
        logged = len(class_logs_by_student_date.get((sid, date_str), []))
        actual_total += logged
        if logged < count:
            missing_logs.append({"student": sid, "name": info["name"], "date": date_str, "expected": count, "logged": logged})
    completion_rate = (actual_total / expected_total) if expected_total else None
    student_completion_stats.append({
        "student": sid,
        "name": info["name"],
        "expected": expected_total,
        "actual": actual_total,
        "completion_rate": completion_rate,
    })
    if past_dates and all(len(class_logs_by_student_date.get((sid, d), [])) == 0 for d in past_dates):
        students_with_past_no_logs.append({"student": sid, "name": info["name"], "past_dates": sorted(past_dates)})

student_completion_stats.sort(key=lambda item: (item["completion_rate"] if item["completion_rate"] is not None else -1, -item["expected"]), reverse=True)

schedule_timezone_issues = []
for sid, info in student_info.items():
    for dt in info["class_datetimes"]:
        offset = dt.utcoffset()
        if offset != timedelta(hours=7):
            schedule_timezone_issues.append({"student": sid, "name": info["name"], "class": dt.isoformat(), "offset_hours": offset.total_seconds() / 3600 if offset else None})

log_timezone_issues = []
for idx, entry in enumerate(logs):
    dt = None
    raw_date = entry.get("date")
    if raw_date:
        dt = parse_datetime(raw_date)
    if dt:
        offset = dt.utcoffset()
        if offset != timedelta(hours=7):
            log_timezone_issues.append({"index": idx, "student": entry.get("student"), "status": entry.get("status"), "date": raw_date, "offset_hours": offset.total_seconds() / 3600 if offset else None})

invalid_students = []
for sid, student in students.items():
    issues = []
    telegram_id = student.get("telegram_id")
    if telegram_id in (None, ""):
        issues.append("missing telegram_id")
    elif str(telegram_id) != str(sid):
        issues.append(f"telegram_id mismatch ({telegram_id})")
    for field in ("classes_remaining", "free_class_credit", "reschedule_credit", "plan_price"):
        value = student.get(field)
        if isinstance(value, (int, float)) and value < 0:
            issues.append(f"{field} negative ({value})")
    if issues:
        invalid_students.append({"student": sid, "name": student.get("name"), "issues": issues})

student_schema_variants = Counter(frozenset(student.keys()) for student in students.values())
log_schema_variants = Counter(frozenset(entry.keys()) for entry in logs)

business_flags = {
    "class_credit_no_future": [],
    "future_with_zero_credit": [],
    "premium_visibility": [],
}
for sid, student in students.items():
    info = student_info[sid]
    upcoming = [dt for dt in info["class_datetimes"] if dt.date() > today]
    classes_remaining = student.get("classes_remaining")
    if not isinstance(classes_remaining, (int, float)):
        classes_remaining = 0
    if classes_remaining > 0 and len(upcoming) == 0 and not student.get("paused"):
        business_flags["class_credit_no_future"].append({"student": sid, "name": student.get("name"), "classes_remaining": classes_remaining})
    if classes_remaining <= 0 and len(upcoming) > 0 and not student.get("paused"):
        business_flags["future_with_zero_credit"].append({"student": sid, "name": student.get("name"), "upcoming_classes": len(upcoming), "classes_remaining": classes_remaining})
    if student.get("premium"):
        business_flags["premium_visibility"].append({"student": sid, "name": student.get("name"), "upcoming_classes": len(upcoming), "classes_remaining": classes_remaining})

duplicate_classes = []
for sid, info in student_info.items():
    for iso_ts, count in info["class_duplicates"].items():
        if count > 1:
            duplicate_classes.append({"student": sid, "name": info["name"], "datetime": iso_ts, "count": count})

summary = {
    "meta": {"students": len(students), "logs": len(logs), "today": today.isoformat()},
    "orphan_student_logs_count": len(orphan_student_logs),
    "orphan_student_logs_samples": orphan_student_logs[:5],
    "orphan_date_logs_count": len(orphan_date_logs),
    "orphan_date_logs_samples": orphan_date_logs[:5],
    "orphan_date_logs_global_count": len(orphan_date_logs_global),
    "orphan_date_logs_global_samples": orphan_date_logs_global[:5],
    "missing_logs_count": len(missing_logs),
    "missing_logs_samples": missing_logs[:5],
    "students_with_past_no_logs": students_with_past_no_logs,
    "student_completion_stats": student_completion_stats,
    "schedule_parse_errors": schedule_parse_errors[:5],
    "schedule_naive_strings": schedule_naive_strings[:5],
    "log_parse_errors": log_parse_errors[:5],
    "log_naive_strings": log_naive_strings[:5],
    "log_timezone_issues": log_timezone_issues[:5],
    "schedule_timezone_issues": schedule_timezone_issues[:5],
    "logs_missing_required": logs_missing_required,
    "invalid_students": invalid_students,
    "student_schema_variants": [(list(keys), count) for keys, count in student_schema_variants.most_common()],
    "log_schema_variants": [(list(keys), count) for keys, count in log_schema_variants.most_common()],
    "business_flags": business_flags,
    "duplicate_classes": duplicate_classes[:20],
}

pathlib.Path('dev/audit_summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
print('Summary written to dev/audit_summary.json')
