import logging
from datetime import datetime, timedelta, time
from typing import List, Tuple, Optional

try:
    from telegram.error import BadRequest
except Exception:  # pragma: no cover - fallback when telegram is unavailable

    class BadRequest(Exception):
        """Fallback BadRequest exception for optional telegram dependency."""


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

    Examines the last 4–10 classes and groups entries by weekday and
    start time allowing a ±30 minute tolerance.  Groups occurring fewer
    than twice are ignored.  Timezone differences are ignored and
    grouping is based purely on weekday and local time.  If no stable
    groups remain, ``None`` is returned.
    """

    recent = history[-10:]
    if len(recent) < 4:
        return None

    groups = []
    for dt in recent:
        weekday = dt.weekday()
        minutes = dt.hour * 60 + dt.minute
        matched = False
        for g in groups:
            if (
                g["weekday"] == weekday
                and abs(g["minutes"] - minutes) <= 30
            ):
                g["times"].append(minutes)
                matched = True
                break
        if not matched:
            groups.append({
                "weekday": weekday,
                "tz": dt.tzinfo,
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


async def try_ack(query, *, text=None, show_alert=False) -> bool:
    """Attempt callback acknowledgment, returning whether it succeeded."""

    try:
        await query.answer(text=text, show_alert=show_alert)
        return True
    except BadRequest as exc:
        logging.info("Callback ack failed (continuing): %s", exc)
        return False
    except TypeError as exc:
        logging.debug("Ack signature mismatch: %s", exc)
        return False


async def _answer_with_alert(query, text: str) -> None:
    """Safely answer a callback with an alert, falling back to messaging."""

    async def _fallback_send() -> None:
        target = None
        if hasattr(query, "edit_message_text"):
            try:
                await query.edit_message_text(text)
                return
            except Exception as exc:  # pragma: no cover - unexpected edit failure
                logging.warning("Failed to edit callback message: %s", exc)
                target = getattr(query, "message", None)
        if target is None:
            target = getattr(query, "message", None) or query
        if hasattr(target, "reply_text"):
            try:
                await target.reply_text(text)
            except BadRequest as exc:  # pragma: no cover - depends on telegram
                logging.warning("Failed to send callback fallback message: %s", exc)
            except Exception as exc:  # pragma: no cover - unexpected failure
                logging.warning("Failed to reply for callback fallback: %s", exc)
        else:  # pragma: no cover - unexpected object type
            logging.warning("No reply_text available for callback fallback")

    try:
        await query.answer(text, show_alert=True)
    except TypeError:
        try:
            await query.answer(text)
        except TypeError:
            try:
                await query.answer()
            except TypeError as exc:  # pragma: no cover - unexpected signature
                logging.warning("Failed to answer callback (no-arg TypeError): %s", exc)
            except BadRequest as exc:  # pragma: no cover - depends on telegram
                logging.warning("Failed to answer callback (no-arg): %s", exc)
                await _fallback_send()
        except BadRequest as exc:  # pragma: no cover - depends on telegram
            logging.warning("Failed to answer callback (text): %s", exc)
            await _fallback_send()
    except BadRequest as exc:  # pragma: no cover - depends on telegram
        logging.warning("Failed to answer callback with alert: %s", exc)
        await _fallback_send()
