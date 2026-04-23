from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from alphavault.constants import DATETIME_FMT
from alphavault.timeutil import CST
from alphavault_reflex.services.homework_constants import (
    TRADE_BOARD_DEFAULT_WINDOW_DAYS,
    TRADE_BOARD_MAX_WINDOW_DAYS,
)

LOCAL_INPUT_DATETIME_FORMAT = "%Y-%m-%dT%H:%M"
DISPLAY_DATETIME_FORMAT = "%Y-%m-%d %H:%M"

HOMEWORK_TIME_SHORTCUT_TODAY = "今天"
HOMEWORK_TIME_SHORTCUT_RECENT_3_DAYS = "最近3天"
HOMEWORK_TIME_SHORTCUT_RECENT_7_DAYS = "最近7天"
HOMEWORK_TIME_SHORTCUT_RECENT_HALF_MONTH = "最近半个月"
HOMEWORK_TIME_SHORTCUT_RECENT_ONE_MONTH = "最近一个月"
HOMEWORK_TIME_SHORTCUT_RECENT_THREE_MONTHS = "最近三个月"

HOMEWORK_TIME_SHORTCUT_DAY_COUNTS = {
    HOMEWORK_TIME_SHORTCUT_RECENT_3_DAYS: 3,
    HOMEWORK_TIME_SHORTCUT_RECENT_7_DAYS: 7,
    HOMEWORK_TIME_SHORTCUT_RECENT_HALF_MONTH: 15,
    HOMEWORK_TIME_SHORTCUT_RECENT_ONE_MONTH: 30,
    HOMEWORK_TIME_SHORTCUT_RECENT_THREE_MONTHS: 90,
}

HOMEWORK_TIME_SHORTCUT_OPTIONS = [
    HOMEWORK_TIME_SHORTCUT_TODAY,
    HOMEWORK_TIME_SHORTCUT_RECENT_3_DAYS,
    HOMEWORK_TIME_SHORTCUT_RECENT_7_DAYS,
    HOMEWORK_TIME_SHORTCUT_RECENT_HALF_MONTH,
    HOMEWORK_TIME_SHORTCUT_RECENT_ONE_MONTH,
    HOMEWORK_TIME_SHORTCUT_RECENT_THREE_MONTHS,
]

HOMEWORK_TIME_RANGE_EMPTY_ERROR = "请选择开始时间和结束时间。"
HOMEWORK_TIME_RANGE_INVALID_ERROR = "时间格式不正确。"
HOMEWORK_TIME_RANGE_ORDER_ERROR = "结束时间需要晚于开始时间。"
HOMEWORK_TIME_RANGE_TOO_WIDE_ERROR = (
    f"时间范围不能超过 {TRADE_BOARD_MAX_WINDOW_DAYS} 天。"
)


@dataclass(frozen=True)
class HomeworkTimeRange:
    start_local: str
    end_local: str
    start_utc: datetime
    end_exclusive_utc: datetime

    @property
    def caption(self) -> str:
        return (
            f"时间范围：{_format_display_datetime(self.start_local)}"
            f" ~ {_format_display_datetime(self.end_local)}"
        )

    @property
    def end_reference_utc(self) -> datetime:
        return self.end_exclusive_utc - timedelta(minutes=1)


def default_homework_time_range(
    *, now_local: datetime | None = None
) -> HomeworkTimeRange:
    end_local_dt = _truncate_to_minute(_coerce_local_datetime(now_local))
    start_local_dt = end_local_dt - timedelta(days=TRADE_BOARD_DEFAULT_WINDOW_DAYS)
    return _build_time_range(start_local_dt=start_local_dt, end_local_dt=end_local_dt)


def resolve_homework_time_shortcut(
    shortcut: str,
    *,
    now_local: datetime | None = None,
) -> HomeworkTimeRange:
    end_local_dt = _truncate_to_minute(_coerce_local_datetime(now_local))
    shortcut_label = str(shortcut or "").strip()
    if shortcut_label == HOMEWORK_TIME_SHORTCUT_TODAY:
        start_local_dt = end_local_dt.replace(hour=0, minute=0)
        return _build_time_range(
            start_local_dt=start_local_dt,
            end_local_dt=end_local_dt,
        )

    day_count = HOMEWORK_TIME_SHORTCUT_DAY_COUNTS.get(shortcut_label)
    if day_count is None:
        return default_homework_time_range(now_local=end_local_dt)
    return _build_time_range(
        start_local_dt=end_local_dt - timedelta(days=day_count),
        end_local_dt=end_local_dt,
    )


def parse_homework_time_range(
    start_local: str,
    end_local: str,
) -> tuple[HomeworkTimeRange | None, str]:
    start_text = str(start_local or "").strip()
    end_text = str(end_local or "").strip()
    if not start_text or not end_text:
        return None, HOMEWORK_TIME_RANGE_EMPTY_ERROR

    start_local_dt = _parse_local_datetime(start_text)
    end_local_dt = _parse_local_datetime(end_text)
    if start_local_dt is None or end_local_dt is None:
        return None, HOMEWORK_TIME_RANGE_INVALID_ERROR
    if end_local_dt < start_local_dt:
        return None, HOMEWORK_TIME_RANGE_ORDER_ERROR
    query_span = (end_local_dt - start_local_dt) + timedelta(minutes=1)
    if query_span > timedelta(days=TRADE_BOARD_MAX_WINDOW_DAYS, minutes=1):
        return None, HOMEWORK_TIME_RANGE_TOO_WIDE_ERROR

    return (
        _build_time_range(
            start_local_dt=start_local_dt,
            end_local_dt=end_local_dt,
        ),
        "",
    )


def format_homework_query_datetime(value: datetime) -> str:
    return _coerce_query_datetime(value).isoformat(
        sep=" ",
        timespec="seconds",
    )


def coerce_homework_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        ts = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                ts = datetime.strptime(text, DATETIME_FMT)
            except ValueError:
                try:
                    ts = datetime.strptime(text, "%Y-%m-%d %H:%M")
                except ValueError:
                    return None
    if ts.tzinfo is not None:
        return ts.astimezone(UTC).replace(tzinfo=None)
    return ts.replace(tzinfo=CST).astimezone(UTC).replace(tzinfo=None)


def _build_time_range(
    *,
    start_local_dt: datetime,
    end_local_dt: datetime,
) -> HomeworkTimeRange:
    normalized_start = _truncate_to_minute(_coerce_local_datetime(start_local_dt))
    normalized_end = _truncate_to_minute(_coerce_local_datetime(end_local_dt))
    return HomeworkTimeRange(
        start_local=normalized_start.strftime(LOCAL_INPUT_DATETIME_FORMAT),
        end_local=normalized_end.strftime(LOCAL_INPUT_DATETIME_FORMAT),
        start_utc=_to_utc_naive(normalized_start),
        end_exclusive_utc=_to_utc_naive(normalized_end + timedelta(minutes=1)),
    )


def _parse_local_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _truncate_to_minute(_coerce_local_datetime(parsed))


def _coerce_local_datetime(value: datetime | None) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=CST)
        return value.astimezone(CST)
    return datetime.now(CST)


def _to_utc_naive(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


def _coerce_query_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _truncate_to_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def _format_display_datetime(value: str) -> str:
    parsed = _parse_local_datetime(value)
    if parsed is None:
        return str(value or "").strip()
    return parsed.strftime(DISPLAY_DATETIME_FORMAT)


__all__ = [
    "coerce_homework_timestamp",
    "HOMEWORK_TIME_SHORTCUT_OPTIONS",
    "HOMEWORK_TIME_SHORTCUT_RECENT_3_DAYS",
    "HOMEWORK_TIME_SHORTCUT_RECENT_7_DAYS",
    "HOMEWORK_TIME_SHORTCUT_RECENT_HALF_MONTH",
    "HOMEWORK_TIME_SHORTCUT_RECENT_ONE_MONTH",
    "HOMEWORK_TIME_SHORTCUT_RECENT_THREE_MONTHS",
    "HOMEWORK_TIME_SHORTCUT_TODAY",
    "HomeworkTimeRange",
    "default_homework_time_range",
    "format_homework_query_datetime",
    "parse_homework_time_range",
    "resolve_homework_time_shortcut",
]
