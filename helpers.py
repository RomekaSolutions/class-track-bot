from datetime import datetime


def fmt_class_label(iso_str: str) -> str:
    """Turn an ISO timestamp with offset into a label like 'Mon 09 Sep — 17:00'."""
    dt = datetime.fromisoformat(iso_str)
    return dt.strftime("%a %d %b — %H:%M")
