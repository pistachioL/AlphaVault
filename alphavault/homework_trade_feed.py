from __future__ import annotations

from contextlib import contextmanager
import json
from typing import Any, Iterator

from alphavault.constants import SCHEMA_STANDARD
from alphavault.content_hash import build_content_hash
from alphavault.db.sql.homework_trade_feed import (
    select_homework_trade_feed,
    upsert_homework_trade_feed,
)
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    postgres_connect_autocommit,
)
from alphavault.timeutil import now_cst_str

HOMEWORK_TRADE_FEED_TABLE = f"{SCHEMA_STANDARD}.homework_trade_feed"
HOMEWORK_DEFAULT_VIEW_KEY = "default"

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _now_str() -> str:
    return now_cst_str()


@contextmanager
def _use_conn(
    engine_or_conn: PostgresEngine | PostgresConnection | Any,
) -> Iterator[Any]:
    if isinstance(engine_or_conn, PostgresEngine):
        with postgres_connect_autocommit(engine_or_conn) as conn:
            yield conn
        return
    yield engine_or_conn


def _handle_db_error(
    engine_or_conn: PostgresEngine | PostgresConnection | Any, err: BaseException
) -> None:
    del engine_or_conn
    if isinstance(err, _FATAL_BASE_EXCEPTIONS):
        raise err
    raise err


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clean_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                str(key): _clean_text(raw)
                for key, raw in row.items()
                if str(key or "").strip()
            }
        )
    return out


def _json_load_dict(value: object) -> dict[str, object]:
    text = _clean_text(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_load_rows(value: object) -> list[dict[str, str]]:
    text = _clean_text(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return _clean_rows(parsed)


def _coerce_positive_int(value: object, *, default: int) -> int:
    text = _clean_text(value)
    if not text:
        return max(1, int(default))
    try:
        parsed = int(text)
    except (TypeError, ValueError):
        return max(1, int(default))
    return max(1, parsed)


def _coerce_non_negative_int(value: object, *, default: int) -> int:
    text = _clean_text(value)
    if not text:
        return max(0, int(default))
    try:
        parsed = int(text)
    except (TypeError, ValueError):
        return max(0, int(default))
    return max(0, parsed)


def _select_homework_trade_feed_row(
    conn: PostgresConnection | Any,
    *,
    view_key: str,
) -> dict[str, object]:
    row = (
        conn.execute(
            select_homework_trade_feed(HOMEWORK_TRADE_FEED_TABLE),
            {"view_key": view_key},
        )
        .mappings()
        .fetchone()
    )
    return dict(row) if row else {}


def _homework_trade_feed_content_hash(
    *,
    view_key: str,
    header: dict[str, object],
    rows: list[dict[str, str]],
    counters: dict[str, int],
) -> str:
    return build_content_hash(
        {
            "view_key": _clean_text(view_key),
            "header": dict(header),
            "items": _clean_rows(rows),
            "counters": dict(counters),
        }
    )


def _homework_trade_feed_row_hash(row: dict[str, object]) -> str:
    if not row:
        return ""
    stored_hash = _clean_text(row.get("content_hash"))
    if stored_hash:
        return stored_hash
    return _homework_trade_feed_content_hash(
        view_key=_clean_text(row.get("view_key")),
        header=_json_load_dict(row.get("header_json")),
        rows=_json_load_rows(row.get("items_json")),
        counters={
            "row_count": _coerce_non_negative_int(
                _json_load_dict(row.get("counters_json")).get("row_count"),
                default=len(_json_load_rows(row.get("items_json"))),
            )
        },
    )


def save_homework_trade_feed(
    engine_or_conn: PostgresEngine | PostgresConnection | Any,
    *,
    view_key: str,
    caption: str,
    used_window_days: int,
    rows: list[dict[str, object]] | list[dict[str, str]],
) -> None:
    key = _clean_text(view_key)
    if not key:
        return
    cleaned_rows = _clean_rows(rows)
    header = {
        "caption": _clean_text(caption),
        "used_window_days": _coerce_positive_int(
            used_window_days,
            default=1,
        ),
    }
    counters = {"row_count": len(cleaned_rows)}
    try:
        with _use_conn(engine_or_conn) as conn:
            existing_row = _select_homework_trade_feed_row(conn, view_key=key)
            content_hash = _homework_trade_feed_content_hash(
                view_key=key,
                header=header,
                rows=cleaned_rows,
                counters=counters,
            )
            if (
                existing_row
                and _homework_trade_feed_row_hash(existing_row) == content_hash
            ):
                return
            conn.execute(
                upsert_homework_trade_feed(HOMEWORK_TRADE_FEED_TABLE),
                {
                    "view_key": key,
                    "header_json": json.dumps(header, ensure_ascii=False),
                    "items_json": json.dumps(cleaned_rows, ensure_ascii=False),
                    "counters_json": json.dumps(counters, ensure_ascii=False),
                    "content_hash": content_hash,
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_db_error(engine_or_conn, err)


def load_homework_trade_feed(
    engine_or_conn: PostgresEngine | PostgresConnection | Any,
    *,
    view_key: str,
) -> dict[str, object]:
    key = _clean_text(view_key)
    if not key:
        return {}
    try:
        with _use_conn(engine_or_conn) as conn:
            row = _select_homework_trade_feed_row(conn, view_key=key)
    except BaseException as err:
        _handle_db_error(engine_or_conn, err)
    if not row:
        return {}
    header = _json_load_dict(row.get("header_json"))
    counters = _json_load_dict(row.get("counters_json"))
    rows = _json_load_rows(row.get("items_json"))
    return {
        "view_key": _clean_text(row.get("view_key")),
        "caption": _clean_text(header.get("caption")),
        "used_window_days": _coerce_positive_int(
            header.get("used_window_days"),
            default=1,
        ),
        "rows": rows,
        "row_count": max(
            0,
            _coerce_non_negative_int(
                counters.get("row_count"),
                default=len(rows),
            ),
        ),
        "updated_at": _clean_text(row.get("updated_at")),
    }


__all__ = [
    "HOMEWORK_DEFAULT_VIEW_KEY",
    "HOMEWORK_TRADE_FEED_TABLE",
    "load_homework_trade_feed",
    "save_homework_trade_feed",
]
