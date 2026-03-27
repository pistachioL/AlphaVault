from __future__ import annotations

from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))


def format_cst_datetime(value: datetime) -> str:
    dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=CST)
    else:
        dt = dt.astimezone(CST)
    return dt.isoformat(sep=" ", timespec="seconds")


def now_cst_str() -> str:
    return format_cst_datetime(datetime.now(CST))
