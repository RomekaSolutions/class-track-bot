from datetime import datetime, timedelta, time

from typing import List, Tuple, Optional


def fmt_class_label(iso_str: str) -> str:
    """Turn an ISO timestamp with offset into a label like 'Mon 09 Sep — 17:00'."""
    dt = datetime.fromisoformat(iso_str)
    return dt.strftime("%a %d %b — %H:%M")


Slot = Tuple[int, int, int, object]

WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def extract_weekly_pattern(dates: List[str]) -> List[Slot]:
    """Return sorted unique weekly slots from ``dates``.

    Each item in ``dates`` must be an ISO timestamp string with timezone
    information.  The result is a list of tuples ``(weekday, hour, minute,
    tzinfo)`` sorted by weekday/hour/minute.
    """

    slots: List[Slot] = []
    seen = set()
    for iso in dates:
        try:
            dt = datetime.fromisoformat(iso)
        except Exception:
            continue
        key = (dt.weekday(), dt.hour, dt.minute, dt.tzinfo)
        if key not in seen:
            seen.add(key)
            slots.append(key)
    slots.sort(key=lambda x: (x[0], x[1], x[2]))
    return slots


def next_occurrence_after(anchor: datetime, slot: Slot) -> datetime:
    """First occurrence of ``slot`` strictly after ``anchor``."""

    w, h, m, tz = slot
    anchor_local = anchor.astimezone(tz)
    anchor_date = anchor_local.date()
    delta_days = (w - anchor_local.weekday()) % 7
    candidate_date = anchor_date + timedelta(days=delta_days)
    candidate = datetime.combine(candidate_date, time(hour=h, minute=m, tzinfo=tz))
    if candidate <= anchor_local:
        candidate += timedelta(days=7)
    return candidate


def generate_from_pattern(anchor: datetime, pattern: List[Slot], count: int) -> List[datetime]:
    """Generate ``count`` datetimes following ``pattern`` after ``anchor``."""

    results: List[datetime] = []
    current = anchor
    for _ in range(count):
        candidates = [next_occurrence_after(current, slot) for slot in pattern]
        next_dt = min(candidates)
        results.append(next_dt)
        current = next_dt
    return results


def get_weekly_pattern_from_history(history: List[datetime]) -> Optional[List[Slot]]:
    """Return a stable weekly pattern from recent ``history``.

    Examines the last 6–10 classes and groups entries by weekday and
    start time allowing a ±15 minute tolerance.  Groups occurring fewer
    than twice are ignored.  If no stable groups remain, ``None`` is
    returned.
    """

    recent = history[-10:]
    if len(recent) < 6:
        return None

    groups = []
    for dt in recent:
        weekday = dt.weekday()
        minutes = dt.hour * 60 + dt.minute
        tz = dt.tzinfo
        matched = False
        for g in groups:
            if (
                g["weekday"] == weekday
                and g["tz"] == tz
                and abs(g["minutes"] - minutes) <= 15
            ):
                g["times"].append(minutes)
                matched = True
                break
        if not matched:
            groups.append({
                "weekday": weekday,
                "tz": tz,
                "minutes": minutes,
                "times": [minutes],
            })

    pattern: List[Slot] = []
    for g in groups:
        if len(g["times"]) >= 2:
            avg = sum(g["times"]) / len(g["times"])
            total_minutes = int(round(avg))
            hour = total_minutes // 60
            minute = total_minutes % 60
            pattern.append((g["weekday"], hour, minute, g["tz"]))

    pattern.sort(key=lambda x: (x[0], x[1], x[2]))
    return pattern or None


def slots_to_text(pattern: List[Slot]) -> str:
    """Format ``pattern`` into human-readable string."""

    return ", ".join(
        f"{WEEKDAYS[w]} {h:02d}:{m:02d}" for w, h, m, _ in pattern
    )
