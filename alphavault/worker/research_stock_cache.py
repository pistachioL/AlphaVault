from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from alphavault.db.introspect import table_columns
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql.ui import build_assertions_query
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
)
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.research_backfill_cache import list_stock_backfill_posts
from alphavault.research_stock_cache import (
    ensure_research_stock_cache_schema,
    list_stock_dirty_entries,
    list_stock_dirty_keys,
    load_stock_extras_snapshot,
    remove_stock_dirty_keys,
    save_stock_extras_snapshot,
    save_stock_hot_view,
)
from alphavault.research_workbench import (
    ensure_research_workbench_schema,
    list_pending_candidates_for_left_key,
)
from alphavault.ui.follow_pages_key_match import is_stock_code_value, parse_json_list
from alphavault_reflex.services.stock_objects import (
    build_stock_object_index,
    filter_assertions_for_stock_object,
)
from alphavault_reflex.services.thread_tree import build_post_tree_map
from alphavault.worker.job_state import (
    release_worker_job_lock,
    try_acquire_worker_job_lock,
)

STOCK_HOT_CACHE_LOCK_KEY = "stock_hot_cache.lock"
STOCK_HOT_CACHE_LOCK_LEASE_SECONDS = 600
STOCK_HOT_CACHE_MAX_STOCKS_PER_RUN = 4
STOCK_HOT_CACHE_DIRTY_LIMIT = 16
STOCK_HOT_CACHE_SIGNAL_WINDOW_DAYS = 30
STOCK_HOT_CACHE_SIGNAL_CAP = 500
STOCK_EXTRAS_REFRESH_MIN_SECONDS = 900

_EXTRAS_FORCE_REFRESH_REASONS = {
    "alias_relation",
    "candidate_action",
    "queue_backfill",
    "relation_candidates_cache",
    "backfill_cache",
}

_WANTED_ASSERTION_COLUMNS = [
    "post_uid",
    "topic_key",
    "action",
    "action_strength",
    "summary",
    "author",
    "created_at",
    "stock_codes_json",
    "stock_names_json",
    "cluster_keys_json",
    "cluster_key",
]

_WANTED_POST_COLUMNS = [
    "post_uid",
    "platform_post_id",
    "author",
    "created_at",
    "url",
    "raw_text",
    "display_md",
]

_STOCK_ALIAS_RELATIONS_SQL = """
SELECT relation_type, left_key, right_key, relation_label, source, updated_at
FROM research_relations
WHERE relation_type = 'stock_alias' OR relation_label = 'alias_of'
"""

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _window_cutoff_str(days: int) -> str:
    window_days = max(1, int(days))
    cutoff = datetime.now(tz=UTC) - timedelta(days=window_days)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_stock_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text if text.startswith("stock:") else f"stock:{text}"


def _stock_code_from_key(stock_key: str) -> str:
    key = _normalize_stock_key(stock_key)
    if not key.startswith("stock:"):
        return ""
    code = str(key[len("stock:") :].strip()).upper()
    return code if is_stock_code_value(code) else ""


def _load_stock_alias_relations(conn: TursoConnection) -> pd.DataFrame:
    try:
        return turso_read_sql_df(conn, _STOCK_ALIAS_RELATIONS_SQL)
    except BaseException:
        return pd.DataFrame()


def _alias_keys_for_stock(relations: pd.DataFrame, *, stock_key: str) -> list[str]:
    if relations.empty:
        return []
    left = str(stock_key or "").strip()
    if not left:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for _, row in relations.iterrows():
        relation_type = str(row.get("relation_type") or "").strip()
        relation_label = str(row.get("relation_label") or "").strip()
        if relation_type != "stock_alias" and relation_label != "alias_of":
            continue
        left_key = str(row.get("left_key") or "").strip()
        right_key = str(row.get("right_key") or "").strip()
        if left_key != left or not right_key or right_key in seen:
            continue
        seen.add(right_key)
        out.append(right_key)
    return out


def _load_stock_assertions(
    conn: TursoConnection,
    *,
    stock_key: str,
    stock_relations: pd.DataFrame,
    window_days: int,
    max_rows: int,
) -> pd.DataFrame:
    assertion_cols = table_columns(conn, "assertions")
    selected_cols = [
        col for col in _WANTED_ASSERTION_COLUMNS if col in set(assertion_cols)
    ]
    if not selected_cols:
        return pd.DataFrame()
    query = build_assertions_query(selected_cols)
    params: dict[str, Any] = {
        "stock_key": str(stock_key or "").strip(),
        "cutoff": _window_cutoff_str(window_days),
        "limit": max(1, int(max_rows)),
    }

    keys = [str(stock_key or "").strip()] + _alias_keys_for_stock(
        stock_relations,
        stock_key=stock_key,
    )
    keys = [key for key in keys if key]
    key_clause = "topic_key = :stock_key"
    if len(keys) > 1:
        key_values = keys[1:]
        key_placeholders = make_in_placeholders(prefix="k", count=len(key_values))
        params.update(make_in_params(prefix="k", values=key_values))
        key_clause = f"(topic_key = :stock_key OR topic_key IN ({key_placeholders}))"

    code = _stock_code_from_key(stock_key)
    if code and "stock_codes_json" in assertion_cols:
        params["stock_code_like"] = f"%{code}%"
        key_clause = f"({key_clause} OR stock_codes_json LIKE :stock_code_like)"

    query += "\nWHERE action LIKE 'trade.%'"
    if "created_at" in assertion_cols:
        query += "\n  AND created_at >= :cutoff"
    query += f"\n  AND {key_clause}"
    if "created_at" in assertion_cols:
        query += "\nORDER BY created_at DESC"
    query += "\nLIMIT :limit"

    assertions = turso_read_sql_df(conn, query, params=params)
    if assertions.empty:
        return assertions
    out = assertions.copy()
    for col in ["post_uid", "topic_key", "summary", "author", "action"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    if "stock_codes_json" not in out.columns:
        out["stock_codes_json"] = "[]"
    if "stock_names_json" not in out.columns:
        out["stock_names_json"] = "[]"
    out["stock_codes_json"] = out["stock_codes_json"].fillna("[]").astype(str)
    out["stock_names_json"] = out["stock_names_json"].fillna("[]").astype(str)
    if "cluster_keys_json" in out.columns:
        out["cluster_keys"] = out["cluster_keys_json"].apply(parse_json_list)
    elif "cluster_key" in out.columns:
        out["cluster_keys"] = out["cluster_key"].apply(
            lambda item: [str(item).strip()] if str(item or "").strip() else []
        )
    else:
        out["cluster_keys"] = [[] for _ in range(len(out))]
    return out


def _load_posts_for_assertions(
    conn: TursoConnection,
    *,
    post_uids: list[str],
) -> pd.DataFrame:
    cleaned = [str(uid or "").strip() for uid in post_uids if str(uid or "").strip()]
    if not cleaned:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(cleaned))
    post_cols = table_columns(conn, "posts")
    selected_cols = [col for col in _WANTED_POST_COLUMNS if col in set(post_cols)]
    if "display_md" not in selected_cols:
        selected_cols.append("'' AS display_md")
    post_select_expr = ", ".join(selected_cols)
    sql = f"""
SELECT {post_select_expr}
FROM posts
WHERE processed_at IS NOT NULL
  AND post_uid IN ({placeholders})
"""
    posts = turso_read_sql_df(conn, sql, params=cleaned)
    if posts.empty:
        return posts
    out = posts.copy()
    for col in ["post_uid", "author", "url", "raw_text", "display_md"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str)
    return out


def _merge_post_fields(assertions: pd.DataFrame, posts: pd.DataFrame) -> pd.DataFrame:
    if assertions.empty or posts.empty or "post_uid" not in assertions.columns:
        return assertions
    post_cols = posts[["post_uid", "raw_text", "display_md", "author"]].copy()
    post_cols = post_cols.rename(
        columns={
            "raw_text": "_post_raw_text",
            "display_md": "_post_display_md",
            "author": "_post_author",
        }
    )
    merged = assertions.merge(post_cols, on="post_uid", how="left")
    for col in ["raw_text", "display_md", "author"]:
        if col not in merged.columns:
            merged[col] = ""
        merged[col] = merged[col].fillna("").astype(str)
    merged.loc[merged["raw_text"].eq(""), "raw_text"] = (
        merged.loc[merged["raw_text"].eq(""), "_post_raw_text"].fillna("").astype(str)
    )
    merged.loc[merged["display_md"].eq(""), "display_md"] = (
        merged.loc[merged["display_md"].eq(""), "_post_display_md"]
        .fillna("")
        .astype(str)
    )
    merged.loc[merged["author"].eq(""), "author"] = (
        merged.loc[merged["author"].eq(""), "_post_author"].fillna("").astype(str)
    )
    return merged.drop(
        columns=["_post_raw_text", "_post_display_md", "_post_author"],
        errors="ignore",
    )


def _coerce_timestamp(value: object) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if getattr(ts, "tzinfo", None) is not None:
        return ts.tz_convert(None)
    return pd.Timestamp(ts)


def _format_signal_timestamp(value: object) -> str:
    ts = _coerce_timestamp(value)
    if ts is None:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M")


def _format_signal_created_at_line(value: object) -> str:
    text = _format_signal_timestamp(value)
    if not text:
        return ""
    now = pd.Timestamp.now(tz="UTC").tz_convert(None)
    ts = _coerce_timestamp(value)
    if ts is None:
        return text
    delta_seconds = max((now - ts).total_seconds(), 0.0)
    minutes = int(delta_seconds // 60)
    if minutes < 60:
        age = f"{minutes}分钟前"
    elif minutes < 24 * 60:
        age = f"{minutes // 60}小时前"
    else:
        age = f"{minutes // (24 * 60)}天前"
    return f"{text} · {age}"


def _build_related_sectors(rows: pd.DataFrame) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for item in rows.get("cluster_keys", pd.Series(dtype=object)).tolist():
        if not isinstance(item, list):
            continue
        for raw in item:
            key = str(raw or "").strip()
            if not key:
                continue
            counts[key] = int(counts.get(key, 0)) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {"sector_key": str(sector_key), "mention_count": str(count)}
        for sector_key, count in ranked
    ]


def _build_stock_hot_payload(
    conn: TursoConnection,
    *,
    stock_key: str,
    signal_window_days: int,
    signal_cap: int,
) -> dict[str, object]:
    normalized_key = _normalize_stock_key(stock_key)
    if not normalized_key:
        return {
            "entity_key": "",
            "header_title": "",
            "signals": [],
            "signal_total": 0,
            "related_sectors": [],
        }
    stock_relations = _load_stock_alias_relations(conn)
    assertions = _load_stock_assertions(
        conn,
        stock_key=normalized_key,
        stock_relations=stock_relations,
        window_days=signal_window_days,
        max_rows=max(1, int(signal_cap) * 4),
    )
    if assertions.empty:
        stock_value = normalized_key.removeprefix("stock:")
        return {
            "entity_key": normalized_key,
            "header_title": stock_value,
            "signals": [],
            "signal_total": 0,
            "related_sectors": [],
        }
    stock_index = build_stock_object_index(assertions, stock_relations=stock_relations)
    entity_key = stock_index.resolve(normalized_key) or normalized_key
    rows = filter_assertions_for_stock_object(
        assertions,
        stock_key=entity_key,
        stock_relations=stock_relations,
        stock_index=stock_index,
    )
    if rows.empty:
        return {
            "entity_key": entity_key,
            "header_title": stock_index.header_title(entity_key),
            "signals": [],
            "signal_total": 0,
            "related_sectors": [],
        }
    if "created_at" in rows.columns:
        rows["created_at"] = pd.to_datetime(rows["created_at"], errors="coerce")
        rows = rows.sort_values(by="created_at", ascending=False, na_position="last")
    total = int(len(rows.index))
    rows = rows.head(max(1, int(signal_cap))).copy()
    post_uids = [
        str(uid or "").strip()
        for uid in rows.get("post_uid", pd.Series(dtype=str)).tolist()
        if str(uid or "").strip()
    ]
    posts = _load_posts_for_assertions(conn, post_uids=post_uids)
    rows = _merge_post_fields(rows, posts)
    tree_map = build_post_tree_map(post_uids=post_uids, posts=posts)
    signals: list[dict[str, str]] = []
    for _, row in rows.iterrows():
        post_uid = str(row.get("post_uid") or "").strip()
        tree_label, tree_text = tree_map.get(post_uid, ("", ""))
        signals.append(
            {
                "post_uid": post_uid,
                "summary": str(row.get("summary") or "").strip(),
                "action": str(row.get("action") or "").strip(),
                "action_strength": str(row.get("action_strength") or "").strip(),
                "author": str(row.get("author") or "").strip(),
                "created_at": _format_signal_timestamp(row.get("created_at")),
                "created_at_line": _format_signal_created_at_line(
                    row.get("created_at")
                ),
                "raw_text": str(row.get("raw_text") or "").strip(),
                "display_md": str(row.get("display_md") or "").strip(),
                "tree_label": str(tree_label or "").strip(),
                "tree_text": str(tree_text or "").strip(),
            }
        )
    return {
        "entity_key": entity_key,
        "header_title": stock_index.header_title(entity_key),
        "signals": signals,
        "signal_total": total,
        "related_sectors": _build_related_sectors(rows),
    }


def _parse_naive_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _extras_force_refresh(reason: str) -> bool:
    return str(reason or "").strip() in _EXTRAS_FORCE_REFRESH_REASONS


def _is_extras_snapshot_stale(
    conn: TursoConnection,
    *,
    stock_key: str,
    min_refresh_seconds: int,
) -> bool:
    snapshot = load_stock_extras_snapshot(conn, stock_key=stock_key)
    updated_at = _parse_naive_datetime(snapshot.get("updated_at"))
    if updated_at is None:
        return True
    delta = datetime.now() - updated_at
    return float(delta.total_seconds()) >= max(0, int(min_refresh_seconds))


def refresh_stock_hot_for_key(
    conn: TursoConnection,
    *,
    stock_key: str,
    signal_window_days: int,
    signal_cap: int,
) -> str:
    payload = _build_stock_hot_payload(
        conn,
        stock_key=stock_key,
        signal_window_days=int(signal_window_days),
        signal_cap=int(signal_cap),
    )
    entity_key = (
        str(payload.get("entity_key") or stock_key).strip() or str(stock_key).strip()
    )
    save_stock_hot_view(conn, stock_key=entity_key, payload=payload)
    return entity_key


def refresh_stock_extras_snapshot_for_key(
    conn: TursoConnection,
    *,
    stock_key: str,
    min_refresh_seconds: int,
    force: bool,
) -> bool:
    entity_key = _normalize_stock_key(stock_key)
    if not entity_key:
        return False
    if (not force) and (
        not _is_extras_snapshot_stale(
            conn,
            stock_key=entity_key,
            min_refresh_seconds=int(min_refresh_seconds),
        )
    ):
        return False
    pending = list_pending_candidates_for_left_key(
        conn,
        left_key=entity_key,
        limit=12,
    )
    backfill = list_stock_backfill_posts(
        conn,
        stock_key=entity_key,
        limit=12,
    )
    save_stock_extras_snapshot(
        conn,
        stock_key=entity_key,
        pending_candidates=pending,
        backfill_posts=backfill,
    )
    return True


def sync_stock_hot_cache(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    max_stocks_per_run: int = STOCK_HOT_CACHE_MAX_STOCKS_PER_RUN,
    dirty_limit: int = STOCK_HOT_CACHE_DIRTY_LIMIT,
    signal_window_days: int = STOCK_HOT_CACHE_SIGNAL_WINDOW_DAYS,
    signal_cap: int = STOCK_HOT_CACHE_SIGNAL_CAP,
    extras_refresh_min_seconds: int = STOCK_EXTRAS_REFRESH_MIN_SECONDS,
    lock_lease_seconds: int = STOCK_HOT_CACHE_LOCK_LEASE_SECONDS,
) -> dict[str, int | bool]:
    now_epoch = int(datetime.now(tz=UTC).timestamp())
    if not try_acquire_worker_job_lock(
        engine_or_conn,
        lock_key=STOCK_HOT_CACHE_LOCK_KEY,
        now_epoch=now_epoch,
        lease_seconds=int(lock_lease_seconds),
    ):
        return {"processed": 0, "written": 0, "has_more": False, "locked": True}
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        ensure_research_workbench_schema(engine_or_conn)
        with (
            turso_connect_autocommit(engine_or_conn)
            if isinstance(engine_or_conn, TursoEngine)
            else engine_or_conn
        ) as conn:
            dirty_entries = list_stock_dirty_entries(
                conn,
                limit=max(1, max(int(max_stocks_per_run), int(dirty_limit))),
            )
            dirty_entries = dirty_entries[: max(1, int(max_stocks_per_run))]
            if not dirty_entries:
                return {"processed": 0, "written": 0, "has_more": False}

            written = 0
            extras_written = 0
            processed_keys: list[str] = []
            for entry in dirty_entries:
                stock_key = str(entry.get("stock_key") or "").strip()
                if not stock_key:
                    continue
                reason = str(entry.get("reason") or "").strip()
                entity_key = refresh_stock_hot_for_key(
                    conn,
                    stock_key=stock_key,
                    signal_window_days=int(signal_window_days),
                    signal_cap=int(signal_cap),
                )
                did_refresh_extras = refresh_stock_extras_snapshot_for_key(
                    conn,
                    stock_key=entity_key,
                    min_refresh_seconds=int(extras_refresh_min_seconds),
                    force=_extras_force_refresh(reason),
                )
                if did_refresh_extras:
                    extras_written += 1
                remove_stock_dirty_keys(conn, stock_keys=[stock_key])
                if entity_key != stock_key and entity_key.startswith("stock:"):
                    remove_stock_dirty_keys(conn, stock_keys=[entity_key])
                processed_keys.append(stock_key)
                written += 1

            remaining = list_stock_dirty_keys(conn, limit=1)
            has_more = bool(remaining)
            return {
                "processed": int(len(processed_keys)),
                "written": int(written),
                "extras_written": int(extras_written),
                "has_more": bool(has_more),
            }
    finally:
        try:
            release_worker_job_lock(engine_or_conn, lock_key=STOCK_HOT_CACHE_LOCK_KEY)
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise


__all__ = [
    "refresh_stock_extras_snapshot_for_key",
    "refresh_stock_hot_for_key",
    "sync_stock_hot_cache",
]
