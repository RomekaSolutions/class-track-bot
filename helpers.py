from datetime import datetime, timedelta, time

from typing import List, Tuple


def fmt_class_label(iso_str: str) -> str:
    """Turn an ISO timestamp with offset into a label like 'Mon 09 Sep — 17:00'."""
    dt = datetime.fromisoformat(iso_str)
    return dt.strftime("%a %d %b — %H:%M")


Slot = Tuple[int, int, int, object]


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
