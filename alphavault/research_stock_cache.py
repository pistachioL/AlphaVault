from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
import json
from typing import Iterator, TypedDict

from alphavault.content_hash import build_content_hash
from alphavault.domains.common.assertion_entities import extract_stock_entity_keys
from alphavault.domains.common.json_list import parse_json_list
from alphavault.timeutil import CST, format_cst_datetime, now_cst_str
from alphavault.db.sql.research_stock_cache import (
    select_claimable_research_stock_dirty_keys,
    select_entity_page_snapshot,
    select_research_stock_dirty_keys,
    upsert_entity_page_snapshot_extras,
    upsert_entity_page_snapshot_hot,
    upsert_research_stock_dirty_key,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
    turso_savepoint,
)

ENTITY_PAGE_SNAPSHOT_TABLE = "entity_page_snapshot"
PROJECTION_DIRTY_TABLE = "projection_dirty"
PROJECTION_JOB_TYPE_ENTITY_PAGE = "entity_page"

DIRTY_REASON_MASK_OTHER = 1 << 0
DIRTY_REASON_MASK_RSS = 1 << 1
DIRTY_REASON_MASK_AI = 1 << 2
DIRTY_REASON_MASK_AI_DONE = 1 << 3
DIRTY_REASON_MASK_ALIAS_RELATION = 1 << 4
DIRTY_REASON_MASK_RELATION_CANDIDATES_CACHE = 1 << 5
DIRTY_REASON_MASK_BACKFILL_CACHE = 1 << 6
DIRTY_REASON_MASK_QUEUE_BACKFILL = 1 << 7
DIRTY_REASON_MASK_BOOTSTRAP_MISSING_HOT = 1 << 8

_DIRTY_REASON_MASKS = {
    "rss": DIRTY_REASON_MASK_RSS,
    "ai": DIRTY_REASON_MASK_AI,
    "ai_done": DIRTY_REASON_MASK_AI_DONE,
    "alias_relation": DIRTY_REASON_MASK_ALIAS_RELATION,
    "relation_candidates_cache": DIRTY_REASON_MASK_RELATION_CANDIDATES_CACHE,
    "backfill_cache": DIRTY_REASON_MASK_BACKFILL_CACHE,
    "queue_backfill": DIRTY_REASON_MASK_QUEUE_BACKFILL,
    "bootstrap_missing_hot": DIRTY_REASON_MASK_BOOTSTRAP_MISSING_HOT,
}

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


EntityPageDirtyEntry = TypedDict(
    "EntityPageDirtyEntry",
    {
        "stock_key": str,
        "reason_mask": int,
        "dirty_since": str,
        "last_dirty_at": str,
        "claim_until": str,
        "attempt_count": int,
        "updated_at": str,
    },
)


def _now_str() -> str:
    return now_cst_str()


@contextmanager
def _use_conn(
    engine_or_conn: TursoEngine | TursoConnection,
) -> Iterator[TursoConnection]:
    if isinstance(engine_or_conn, TursoConnection):
        yield engine_or_conn
        return
    with turso_connect_autocommit(engine_or_conn) as conn:
        yield conn


def _handle_turso_error(
    engine_or_conn: TursoEngine | TursoConnection, err: BaseException
) -> None:
    if isinstance(err, _FATAL_BASE_EXCEPTIONS):
        raise err
    engine = (
        engine_or_conn._engine
        if isinstance(engine_or_conn, TursoConnection)
        else engine_or_conn
    )
    if engine is not None and (
        is_turso_stream_not_found_error(err) or is_turso_libsql_panic_error(err)
    ):
        engine.dispose()
    raise err


def _clean_json_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                str(key): str(raw or "").strip()
                for key, raw in row.items()
                if str(key).strip()
            }
        )
    return out


def _json_list(value: object) -> list[dict[str, str]]:
    if isinstance(value, list):
        return _clean_json_rows(value)
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return _clean_json_rows(parsed)


def _json_dumps(value: list[dict[str, str]]) -> str:
    return json.dumps(_clean_json_rows(value), ensure_ascii=False)


def _coerce_non_negative_int(value: object, *, default: int) -> int:
    text = str(value or "").strip()
    if not text:
        return max(int(default), 0)
    try:
        parsed = int(text)
    except (TypeError, ValueError):
        return max(int(default), 0)
    return max(parsed, 0)


def dirty_reason_mask_for(reason: str) -> int:
    text = str(reason or "").strip()
    if not text:
        return 0
    return int(_DIRTY_REASON_MASKS.get(text, DIRTY_REASON_MASK_OTHER))


def _select_entity_page_snapshot_row(
    conn: TursoConnection,
    *,
    entity_key: str,
) -> dict[str, object]:
    row = (
        conn.execute(
            select_entity_page_snapshot(ENTITY_PAGE_SNAPSHOT_TABLE),
            {"entity_key": entity_key},
        )
        .mappings()
        .fetchone()
    )
    return dict(row) if row else {}


def _entity_page_snapshot_content_hash(
    *,
    header_title: str,
    signal_total: int,
    signals: list[dict[str, str]],
    related_sectors: list[dict[str, str]],
    related_stocks: list[dict[str, str]],
    backfill_posts: list[dict[str, str]],
) -> str:
    return build_content_hash(
        {
            "header_title": str(header_title or "").strip(),
            "signal_total": max(0, int(signal_total)),
            "signals": _clean_json_rows(signals),
            "related_sectors": _clean_json_rows(related_sectors),
            "related_stocks": _clean_json_rows(related_stocks),
            "backfill_posts": _clean_json_rows(backfill_posts),
        }
    )


def _entity_page_snapshot_row_hash(row: dict[str, object]) -> str:
    if not row:
        return ""
    stored_hash = str(row.get("content_hash") or "").strip()
    if stored_hash:
        return stored_hash
    return _entity_page_snapshot_content_hash(
        header_title=str(row.get("header_title") or "").strip(),
        signal_total=_coerce_non_negative_int(row.get("signal_total"), default=0),
        signals=_json_list(row.get("signals_json")),
        related_sectors=_json_list(row.get("related_sectors_json")),
        related_stocks=_json_list(row.get("related_stocks_json")),
        backfill_posts=_json_list(row.get("backfill_posts_json")),
    )


def save_entity_page_signal_snapshot(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    payload: dict[str, object],
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    signals = _clean_json_rows(payload.get("signals"))
    related_sectors = _clean_json_rows(payload.get("related_sectors"))
    related_stocks = _clean_json_rows(payload.get("related_stocks"))
    entity_key = str(payload.get("entity_key") or key).strip() or key
    header_title = str(payload.get("header_title") or "").strip()
    signal_total = _coerce_non_negative_int(
        payload.get("signal_total"),
        default=len(signals),
    )
    try:
        with _use_conn(engine_or_conn) as conn:
            existing_row = _select_entity_page_snapshot_row(conn, entity_key=entity_key)
            content_hash = _entity_page_snapshot_content_hash(
                header_title=header_title,
                signal_total=signal_total,
                signals=signals,
                related_sectors=related_sectors,
                related_stocks=related_stocks,
                backfill_posts=_json_list(existing_row.get("backfill_posts_json")),
            )
            if (
                existing_row
                and _entity_page_snapshot_row_hash(existing_row) == content_hash
            ):
                return
            conn.execute(
                upsert_entity_page_snapshot_hot(ENTITY_PAGE_SNAPSHOT_TABLE),
                {
                    "entity_key": entity_key,
                    "header_title": header_title,
                    "signal_total": signal_total,
                    "signals_json": _json_dumps(signals),
                    "related_sectors_json": _json_dumps(related_sectors),
                    "related_stocks_json": _json_dumps(related_stocks),
                    "content_hash": content_hash,
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def load_entity_page_signal_snapshot(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
) -> dict[str, object]:
    key = str(stock_key or "").strip()
    if not key:
        return {}
    try:
        with _use_conn(engine_or_conn) as conn:
            row = _select_entity_page_snapshot_row(conn, entity_key=key)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    if not row:
        return {}
    return {
        "entity_key": str(row.get("entity_key") or "").strip(),
        "header_title": str(row.get("header_title") or "").strip(),
        "signal_total": _coerce_non_negative_int(row.get("signal_total"), default=0),
        "signals": _json_list(row.get("signals_json")),
        "related_sectors": _json_list(row.get("related_sectors_json")),
        "related_stocks": _json_list(row.get("related_stocks_json")),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def save_entity_page_backfill_snapshot(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    backfill_posts: list[dict[str, object]] | list[dict[str, str]],
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    backfill_rows = _clean_json_rows(backfill_posts)
    try:
        with _use_conn(engine_or_conn) as conn:
            existing_row = _select_entity_page_snapshot_row(conn, entity_key=key)
            content_hash = _entity_page_snapshot_content_hash(
                header_title=str(existing_row.get("header_title") or "").strip(),
                signal_total=_coerce_non_negative_int(
                    existing_row.get("signal_total"),
                    default=0,
                ),
                signals=_json_list(existing_row.get("signals_json")),
                related_sectors=_json_list(existing_row.get("related_sectors_json")),
                related_stocks=_json_list(existing_row.get("related_stocks_json")),
                backfill_posts=backfill_rows,
            )
            if (
                existing_row
                and _entity_page_snapshot_row_hash(existing_row) == content_hash
            ):
                return
            conn.execute(
                upsert_entity_page_snapshot_extras(ENTITY_PAGE_SNAPSHOT_TABLE),
                {
                    "entity_key": key,
                    "backfill_posts_json": _json_dumps(backfill_rows),
                    "content_hash": content_hash,
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def load_entity_page_backfill_snapshot(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
) -> dict[str, object]:
    key = str(stock_key or "").strip()
    if not key:
        return {}
    try:
        with _use_conn(engine_or_conn) as conn:
            row = _select_entity_page_snapshot_row(conn, entity_key=key)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    if not row:
        return {}
    return {
        "stock_key": str(row.get("entity_key") or "").strip(),
        "backfill_posts": _json_list(row.get("backfill_posts_json")),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def _parse_cst_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=CST)
    return parsed.astimezone(CST)


def _claim_until_str(now_str: str, claim_ttl_seconds: int) -> str:
    base = _parse_cst_datetime(now_str) or datetime.now(CST)
    return format_cst_datetime(base + timedelta(seconds=max(1, int(claim_ttl_seconds))))


def _clean_dirty_keys(stock_keys: list[str]) -> list[str]:
    return [str(item or "").strip() for item in stock_keys if str(item or "").strip()]


def _map_entity_page_dirty_row(
    row: dict[str, object],
    *,
    claim_until_override: str = "",
) -> EntityPageDirtyEntry:
    claim_until = str(row.get("claim_until") or "").strip()
    if claim_until_override:
        claim_until = str(claim_until_override).strip()
    return {
        "stock_key": str(row.get("target_key") or "").strip(),
        "reason_mask": _coerce_non_negative_int(row.get("reason_mask"), default=0),
        "dirty_since": str(row.get("dirty_since") or "").strip(),
        "last_dirty_at": str(row.get("last_dirty_at") or "").strip(),
        "claim_until": claim_until,
        "attempt_count": _coerce_non_negative_int(row.get("attempt_count"), default=0),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def mark_entity_page_dirty(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    reason: str,
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    now_str = _now_str()
    try:
        with _use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_research_stock_dirty_key(PROJECTION_DIRTY_TABLE),
                {
                    "job_type": PROJECTION_JOB_TYPE_ENTITY_PAGE,
                    "target_key": key,
                    "reason_mask": dirty_reason_mask_for(reason),
                    "dirty_since": now_str,
                    "last_dirty_at": now_str,
                    "updated_at": now_str,
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def list_entity_page_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limit: int,
) -> list[str]:
    n = max(0, int(limit))
    if n <= 0:
        return []
    try:
        with _use_conn(engine_or_conn) as conn:
            rows = (
                conn.execute(
                    select_research_stock_dirty_keys(PROJECTION_DIRTY_TABLE),
                    {
                        "job_type": PROJECTION_JOB_TYPE_ENTITY_PAGE,
                        "limit": n,
                    },
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    return [
        str(row.get("target_key") or "").strip()
        for row in rows
        if str(row.get("target_key") or "").strip()
    ]


def list_entity_page_dirty_entries(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limit: int,
) -> list[EntityPageDirtyEntry]:
    n = max(0, int(limit))
    if n <= 0:
        return []
    try:
        with _use_conn(engine_or_conn) as conn:
            rows = (
                conn.execute(
                    select_research_stock_dirty_keys(PROJECTION_DIRTY_TABLE),
                    {
                        "job_type": PROJECTION_JOB_TYPE_ENTITY_PAGE,
                        "limit": n,
                    },
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    out: list[EntityPageDirtyEntry] = []
    for row in rows:
        mapped = _map_entity_page_dirty_row(dict(row))
        if not str(mapped.get("stock_key") or "").strip():
            continue
        out.append(mapped)
    return out


def remove_entity_page_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_keys: list[str],
    claim_until: str = "",
) -> int:
    keys = _clean_dirty_keys(stock_keys)
    if not keys:
        return 0
    claim_token = str(claim_until or "").strip()
    placeholders = ", ".join(["?"] * len(keys))
    sql = f"""
DELETE FROM {PROJECTION_DIRTY_TABLE}
WHERE job_type = ?
  AND target_key IN ({placeholders})
"""
    params: list[object] = [PROJECTION_JOB_TYPE_ENTITY_PAGE, *keys]
    if claim_token:
        sql += "\n  AND claim_until = ?"
        params.append(claim_token)
    try:
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                res = conn.execute(
                    sql,
                    params,
                )
                return int(res.rowcount or 0)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    return 0


def claim_entity_page_dirty_entries(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limit: int,
    claim_ttl_seconds: int,
) -> list[EntityPageDirtyEntry]:
    n = max(0, int(limit))
    if n <= 0:
        return []
    now_str = _now_str()
    claim_until = _claim_until_str(now_str, claim_ttl_seconds)
    try:
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                rows = (
                    conn.execute(
                        select_claimable_research_stock_dirty_keys(
                            PROJECTION_DIRTY_TABLE
                        ),
                        {
                            "job_type": PROJECTION_JOB_TYPE_ENTITY_PAGE,
                            "now": now_str,
                            "limit": n,
                        },
                    )
                    .mappings()
                    .all()
                )
                keys = _clean_dirty_keys(
                    [str(row.get("target_key") or "").strip() for row in rows]
                )
                if not keys:
                    return []
                placeholders = ", ".join(["?"] * len(keys))
                conn.execute(
                    f"""
UPDATE {PROJECTION_DIRTY_TABLE}
SET claim_until = ?,
    updated_at = ?
WHERE job_type = ?
  AND target_key IN ({placeholders})
  AND (claim_until = '' OR claim_until <= ?)
""",
                    [
                        claim_until,
                        now_str,
                        PROJECTION_JOB_TYPE_ENTITY_PAGE,
                        *keys,
                        now_str,
                    ],
                )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    out: list[EntityPageDirtyEntry] = []
    for row in rows:
        mapped = _map_entity_page_dirty_row(dict(row), claim_until_override=claim_until)
        if not str(mapped.get("stock_key") or "").strip():
            continue
        out.append(mapped)
    return out


def release_entity_page_dirty_claims(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_keys: list[str],
    claim_until: str,
) -> int:
    keys = _clean_dirty_keys(stock_keys)
    claim_token = str(claim_until or "").strip()
    if (not keys) or (not claim_token):
        return 0
    placeholders = ", ".join(["?"] * len(keys))
    now_str = _now_str()
    sql = f"""
UPDATE {PROJECTION_DIRTY_TABLE}
SET claim_until = '',
    updated_at = ?
WHERE job_type = ?
  AND target_key IN ({placeholders})
  AND claim_until = ?
"""
    try:
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                res = conn.execute(
                    sql,
                    [
                        now_str,
                        PROJECTION_JOB_TYPE_ENTITY_PAGE,
                        *keys,
                        claim_token,
                    ],
                )
                return int(res.rowcount or 0)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    return 0


def fail_entity_page_dirty_claims(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_keys: list[str],
    claim_until: str,
) -> int:
    keys = _clean_dirty_keys(stock_keys)
    claim_token = str(claim_until or "").strip()
    if (not keys) or (not claim_token):
        return 0
    placeholders = ", ".join(["?"] * len(keys))
    now_str = _now_str()
    sql = f"""
UPDATE {PROJECTION_DIRTY_TABLE}
SET claim_until = '',
    attempt_count = attempt_count + 1,
    updated_at = ?
WHERE job_type = ?
  AND target_key IN ({placeholders})
  AND claim_until = ?
"""
    try:
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                res = conn.execute(
                    sql,
                    [
                        now_str,
                        PROJECTION_JOB_TYPE_ENTITY_PAGE,
                        *keys,
                        claim_token,
                    ],
                )
                return int(res.rowcount or 0)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    return 0


def pop_entity_page_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limit: int,
) -> list[str]:
    keys = list_entity_page_dirty_keys(engine_or_conn, limit=limit)
    if not keys:
        return []
    remove_entity_page_dirty_keys(engine_or_conn, stock_keys=keys)
    return keys


def mark_entity_page_dirty_from_assertions(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    assertions: list[dict[str, object]],
    reason: str,
) -> int:
    keys = set(extract_stock_entity_keys(assertions))
    for row in assertions:
        if not isinstance(row, dict):
            continue
        topic_key = str(row.get("topic_key") or "").strip()
        if topic_key.startswith("cluster:"):
            keys.add(topic_key)
        for sector_key in parse_json_list(row.get("cluster_keys_json")):
            keys.add(f"cluster:{sector_key}")
        for sector_key in parse_json_list(row.get("cluster_keys")):
            keys.add(f"cluster:{sector_key}")
        cluster_key = str(row.get("cluster_key") or "").strip()
        if cluster_key:
            keys.add(f"cluster:{cluster_key}")
    for key in keys:
        mark_entity_page_dirty(engine_or_conn, stock_key=key, reason=reason)
    return len(keys)


__all__ = [
    "DIRTY_REASON_MASK_BACKFILL_CACHE",
    "DIRTY_REASON_MASK_BOOTSTRAP_MISSING_HOT",
    "DIRTY_REASON_MASK_QUEUE_BACKFILL",
    "EntityPageDirtyEntry",
    "ENTITY_PAGE_SNAPSHOT_TABLE",
    "PROJECTION_DIRTY_TABLE",
    "PROJECTION_JOB_TYPE_ENTITY_PAGE",
    "claim_entity_page_dirty_entries",
    "dirty_reason_mask_for",
    "fail_entity_page_dirty_claims",
    "list_entity_page_dirty_entries",
    "list_entity_page_dirty_keys",
    "load_entity_page_backfill_snapshot",
    "load_entity_page_signal_snapshot",
    "mark_entity_page_dirty",
    "mark_entity_page_dirty_from_assertions",
    "pop_entity_page_dirty_keys",
    "release_entity_page_dirty_claims",
    "remove_entity_page_dirty_keys",
    "save_entity_page_backfill_snapshot",
    "save_entity_page_signal_snapshot",
]
