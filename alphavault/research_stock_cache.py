from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
import json
from typing import Iterator, TypedDict

from alphavault.content_hash import build_content_hash
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    qualify_postgres_table,
    require_postgres_schema_name,
    postgres_connect_autocommit,
    run_postgres_transaction,
)
from alphavault.domains.common.assertion_entities import extract_stock_entity_keys
from alphavault.timeutil import CST, format_cst_datetime, now_cst_str
from alphavault.db.sql.research_stock_cache import (
    select_claimable_research_stock_dirty_keys,
    select_entity_page_snapshot,
    select_research_stock_dirty_keys,
    upsert_entity_page_snapshot_hot,
    upsert_research_stock_dirty_key,
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
DIRTY_REASON_MASK_BOOTSTRAP_MISSING_HOT = 1 << 6

_DIRTY_REASON_MASKS = {
    "rss": DIRTY_REASON_MASK_RSS,
    "ai": DIRTY_REASON_MASK_AI,
    "ai_done": DIRTY_REASON_MASK_AI_DONE,
    "alias_relation": DIRTY_REASON_MASK_ALIAS_RELATION,
    "relation_candidates_cache": DIRTY_REASON_MASK_RELATION_CANDIDATES_CACHE,
    "bootstrap_missing_hot": DIRTY_REASON_MASK_BOOTSTRAP_MISSING_HOT,
}

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_TOPIC_CLUSTER_TABLE = "topic_cluster_topics"
_CLUSTER_KEY_PREFIX = "cluster:"
_TOPIC_ENTITY_PREFIXES = (
    "industry:",
    "commodity:",
    "index:",
    "keyword:",
)


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
    engine_or_conn: PostgresEngine | PostgresConnection,
) -> Iterator[PostgresConnection]:
    if isinstance(engine_or_conn, PostgresConnection):
        yield engine_or_conn
        return
    with postgres_connect_autocommit(engine_or_conn) as conn:
        yield conn


def _source_table(engine_or_conn: object, table_name: str) -> str:
    return qualify_postgres_table(
        require_postgres_schema_name(engine_or_conn),
        table_name,
    )


def _entity_page_snapshot_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, ENTITY_PAGE_SNAPSHOT_TABLE)


def _projection_dirty_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, PROJECTION_DIRTY_TABLE)


def _topic_cluster_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _TOPIC_CLUSTER_TABLE)


def _handle_turso_error(
    engine_or_conn: PostgresEngine | PostgresConnection, err: BaseException
) -> None:
    del engine_or_conn
    if isinstance(err, _FATAL_BASE_EXCEPTIONS):
        raise err
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
                str(key): _stringify_json_value(raw)
                for key, raw in row.items()
                if str(key).strip()
            }
        )
    return out


def _stringify_json_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clean_json_object(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): _stringify_json_value(raw)
        for key, raw in value.items()
        if str(key).strip()
    }


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


def _json_object(value: object) -> dict[str, str]:
    if isinstance(value, dict):
        return _clean_json_object(value)
    text = _stringify_json_value(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return _clean_json_object(parsed)


def _json_dumps(value: list[dict[str, str]]) -> str:
    return json.dumps(_clean_json_rows(value), ensure_ascii=False)


def _json_dumps_object(value: dict[str, str]) -> str:
    return json.dumps(_clean_json_object(value), ensure_ascii=False)


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
    conn: PostgresConnection,
    *,
    entity_key: str,
) -> dict[str, object]:
    row = (
        conn.execute(
            select_entity_page_snapshot(_entity_page_snapshot_table(conn)),
            {"entity_key": entity_key},
        )
        .mappings()
        .fetchone()
    )
    return dict(row) if row else {}


def _entity_page_snapshot_content_hash(
    *,
    entity_type: str,
    header: dict[str, str],
    signal_top: list[dict[str, str]],
    related: list[dict[str, str]],
    counters: dict[str, str],
) -> str:
    return build_content_hash(
        {
            "entity_type": _stringify_json_value(entity_type),
            "header": _clean_json_object(header),
            "signal_top": _clean_json_rows(signal_top),
            "related": _clean_json_rows(related),
            "counters": _clean_json_object(counters),
        }
    )


def _entity_page_snapshot_row_hash(row: dict[str, object]) -> str:
    if not row:
        return ""
    stored_hash = str(row.get("content_hash") or "").strip()
    if stored_hash:
        return stored_hash
    return _entity_page_snapshot_content_hash(
        entity_type=str(row.get("entity_type") or "").strip(),
        header=_json_object(row.get("header_json")),
        signal_top=_json_list(row.get("signal_top_json")),
        related=_json_list(row.get("related_json")),
        counters=_json_object(row.get("counters_json")),
    )


def save_entity_page_signal_snapshot(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    stock_key: str,
    payload: dict[str, object],
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    entity_key = str(payload.get("entity_key") or key).strip() or key
    entity_type = str(payload.get("entity_type") or "").strip()
    if not entity_type:
        entity_type = (
            "sector" if entity_key.startswith(_CLUSTER_KEY_PREFIX) else "stock"
        )
    header = _clean_json_object(payload.get("header"))
    signal_top = _clean_json_rows(payload.get("signal_top"))
    related = _clean_json_rows(payload.get("related"))
    counters = _clean_json_object(payload.get("counters"))
    if "signal_total" not in counters:
        counters["signal_total"] = str(len(signal_top))
    signal_total = _coerce_non_negative_int(
        counters.get("signal_total"),
        default=len(signal_top),
    )
    counters["signal_total"] = str(signal_total)
    try:
        with _use_conn(engine_or_conn) as conn:
            existing_row = _select_entity_page_snapshot_row(conn, entity_key=entity_key)
            content_hash = _entity_page_snapshot_content_hash(
                entity_type=entity_type,
                header=header,
                signal_top=signal_top,
                related=related,
                counters=counters,
            )
            if (
                existing_row
                and _entity_page_snapshot_row_hash(existing_row) == content_hash
            ):
                return
            conn.execute(
                upsert_entity_page_snapshot_hot(_entity_page_snapshot_table(conn)),
                {
                    "entity_key": entity_key,
                    "entity_type": entity_type,
                    "header_json": _json_dumps_object(header),
                    "signal_top_json": _json_dumps(signal_top),
                    "related_json": _json_dumps(related),
                    "counters_json": _json_dumps_object(counters),
                    "content_hash": content_hash,
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def load_entity_page_signal_snapshot(
    engine_or_conn: PostgresEngine | PostgresConnection,
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
        "entity_type": str(row.get("entity_type") or "").strip(),
        "header": _json_object(row.get("header_json")),
        "signal_top": _json_list(row.get("signal_top_json")),
        "related": _json_list(row.get("related_json")),
        "counters": _json_object(row.get("counters_json")),
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
    engine_or_conn: PostgresEngine | PostgresConnection,
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
                upsert_research_stock_dirty_key(_projection_dirty_table(conn)),
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
    engine_or_conn: PostgresEngine | PostgresConnection,
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
                    select_research_stock_dirty_keys(_projection_dirty_table(conn)),
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
    engine_or_conn: PostgresEngine | PostgresConnection,
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
                    select_research_stock_dirty_keys(_projection_dirty_table(conn)),
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
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    stock_keys: list[str],
    claim_until: str = "",
) -> int:
    keys = _clean_dirty_keys(stock_keys)
    if not keys:
        return 0
    claim_token = str(claim_until or "").strip()
    placeholders = ", ".join(["?"] * len(keys))
    params: list[object] = [PROJECTION_JOB_TYPE_ENTITY_PAGE, *keys]

    def _delete(conn: PostgresConnection) -> int:
        sql = f"""
DELETE FROM {_projection_dirty_table(conn)}
WHERE job_type = ?
  AND target_key IN ({placeholders})
"""
        delete_params = list(params)
        if claim_token:
            sql += "\n  AND claim_until = ?"
            delete_params.append(claim_token)
        res = conn.execute(sql, delete_params)
        return int(res.rowcount or 0)

    return run_postgres_transaction(engine_or_conn, _delete)


def claim_entity_page_dirty_entries(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    limit: int,
    claim_ttl_seconds: int,
) -> list[EntityPageDirtyEntry]:
    n = max(0, int(limit))
    if n <= 0:
        return []
    now_str = _now_str()
    claim_until = _claim_until_str(now_str, claim_ttl_seconds)

    def _claim(conn: PostgresConnection) -> list[EntityPageDirtyEntry]:
        projection_dirty_table = _projection_dirty_table(conn)
        rows = (
            conn.execute(
                select_claimable_research_stock_dirty_keys(projection_dirty_table),
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
UPDATE {projection_dirty_table}
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
        out: list[EntityPageDirtyEntry] = []
        for row in rows:
            mapped = _map_entity_page_dirty_row(
                dict(row),
                claim_until_override=claim_until,
            )
            if not str(mapped.get("stock_key") or "").strip():
                continue
            out.append(mapped)
        return out

    return run_postgres_transaction(engine_or_conn, _claim)


def release_entity_page_dirty_claims(
    engine_or_conn: PostgresEngine | PostgresConnection,
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

    def _release(conn: PostgresConnection) -> int:
        sql = f"""
UPDATE {_projection_dirty_table(conn)}
SET claim_until = '',
    updated_at = ?
WHERE job_type = ?
  AND target_key IN ({placeholders})
  AND claim_until = ?
"""
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

    return run_postgres_transaction(engine_or_conn, _release)


def fail_entity_page_dirty_claims(
    engine_or_conn: PostgresEngine | PostgresConnection,
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

    def _fail(conn: PostgresConnection) -> int:
        sql = f"""
UPDATE {_projection_dirty_table(conn)}
SET claim_until = '',
    attempt_count = attempt_count + 1,
    updated_at = ?
WHERE job_type = ?
  AND target_key IN ({placeholders})
  AND claim_until = ?
"""
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

    return run_postgres_transaction(engine_or_conn, _fail)


def pop_entity_page_dirty_keys(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    limit: int,
) -> list[str]:
    keys = list_entity_page_dirty_keys(engine_or_conn, limit=limit)
    if not keys:
        return []
    remove_entity_page_dirty_keys(engine_or_conn, stock_keys=keys)
    return keys


def _normalize_assertion_entities(value: object) -> list[dict[str, str]]:
    if isinstance(value, list):
        raw_items = value
    else:
        text = _stringify_json_value(value)
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        raw_items = parsed

    out: list[dict[str, str]] = []
    for row in raw_items:
        if not isinstance(row, dict):
            continue
        entity_key = _stringify_json_value(row.get("entity_key"))
        entity_type = _stringify_json_value(row.get("entity_type"))
        if not entity_key or not entity_type:
            continue
        out.append(
            {
                "entity_key": entity_key,
                "entity_type": entity_type,
            }
        )
    return out


def _extract_topic_and_cluster_entity_keys(
    assertions: list[dict[str, object]],
) -> tuple[list[str], list[str]]:
    topic_keys: set[str] = set()
    cluster_keys: set[str] = set()
    for raw_assertion in assertions:
        if not isinstance(raw_assertion, dict):
            continue
        entities = _normalize_assertion_entities(
            raw_assertion.get("assertion_entities")
        )
        for entity in entities:
            entity_key = _stringify_json_value(entity.get("entity_key"))
            if not entity_key:
                continue
            if entity_key.startswith(_CLUSTER_KEY_PREFIX):
                cluster_keys.add(entity_key)
                continue
            if entity_key.startswith(_TOPIC_ENTITY_PREFIXES):
                topic_keys.add(entity_key)
    return sorted(topic_keys), sorted(cluster_keys)


def _load_cluster_entity_keys_for_topics(
    conn: PostgresConnection,
    *,
    topic_keys: list[str],
) -> list[str]:
    if not topic_keys:
        return []
    placeholders = ", ".join(["?"] * len(topic_keys))
    rows = (
        conn.execute(
            f"""
SELECT DISTINCT cluster_key
FROM {_topic_cluster_table(conn)}
WHERE topic_key IN ({placeholders})
""",
            topic_keys,
        )
        .mappings()
        .all()
    )
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        cluster_key = _stringify_json_value(row.get("cluster_key"))
        if not cluster_key:
            continue
        entity_key = (
            cluster_key
            if cluster_key.startswith(_CLUSTER_KEY_PREFIX)
            else f"{_CLUSTER_KEY_PREFIX}{cluster_key}"
        )
        if entity_key in seen:
            continue
        seen.add(entity_key)
        out.append(entity_key)
    out.sort()
    return out


def mark_entity_page_dirty_from_assertions(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    assertions: list[dict[str, object]],
    reason: str,
) -> int:
    keys = set(extract_stock_entity_keys(assertions))
    try:
        with _use_conn(engine_or_conn) as conn:
            topic_keys, direct_cluster_keys = _extract_topic_and_cluster_entity_keys(
                assertions
            )
            keys.update(direct_cluster_keys)
            keys.update(
                _load_cluster_entity_keys_for_topics(conn, topic_keys=topic_keys)
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    for key in sorted(keys):
        mark_entity_page_dirty(engine_or_conn, stock_key=key, reason=reason)
    return len(keys)


__all__ = [
    "DIRTY_REASON_MASK_BOOTSTRAP_MISSING_HOT",
    "EntityPageDirtyEntry",
    "ENTITY_PAGE_SNAPSHOT_TABLE",
    "PROJECTION_DIRTY_TABLE",
    "PROJECTION_JOB_TYPE_ENTITY_PAGE",
    "claim_entity_page_dirty_entries",
    "dirty_reason_mask_for",
    "fail_entity_page_dirty_claims",
    "list_entity_page_dirty_entries",
    "list_entity_page_dirty_keys",
    "load_entity_page_signal_snapshot",
    "mark_entity_page_dirty",
    "mark_entity_page_dirty_from_assertions",
    "pop_entity_page_dirty_keys",
    "release_entity_page_dirty_claims",
    "remove_entity_page_dirty_keys",
    "save_entity_page_signal_snapshot",
]
