"""
Follow pages (no-code configs) stored in Turso.

One page = follow one thing:
- follow_type: "topic" or "cluster"
- follow_key: 主 key（通常是 assertion.match_keys 里的一个 key；或 cluster_key）
- follow_keys_json: 这个页面“认”的 key 列表（用于 1 个关注页覆盖多个别名/简称）
- keywords_text: optional OR keywords
"""

from __future__ import annotations

from datetime import datetime
import json

import pandas as pd

from alphavault.constants import DATETIME_FMT
from alphavault.db.introspect import table_columns
from alphavault.db.sql.follow_pages import (
    FOLLOW_KEYS_JSON_COLUMN,
    add_follow_keys_json_column,
    create_follow_pages_index,
    create_follow_pages_table,
    delete_follow_page as delete_follow_page_sql,
    select_follow_pages,
    upsert_follow_page as upsert_follow_page_sql,
)
from alphavault.db.turso_db import TursoEngine
from alphavault.db.turso_db import turso_connect_autocommit
from alphavault.db.turso_pandas import turso_read_sql_df


FOLLOW_PAGES_TABLE = "follow_pages"

FOLLOW_TYPE_TOPIC = "topic"
FOLLOW_TYPE_CLUSTER = "cluster"
ALLOWED_FOLLOW_TYPES = {FOLLOW_TYPE_TOPIC, FOLLOW_TYPE_CLUSTER}


def _now_str() -> str:
    # Keep the same datetime shape used in other modules (YYYY-MM-DD HH:MM:SS).
    return datetime.now().strftime(DATETIME_FMT)


def normalize_follow_type(value: object) -> str:
    s = str(value or "").strip().lower()
    return s if s in ALLOWED_FOLLOW_TYPES else ""


def make_page_key(*, follow_type: str, follow_key: str) -> str:
    t = normalize_follow_type(follow_type)
    k = str(follow_key or "").strip()
    if not t or not k:
        return ""
    return f"{t}:{k}"


def init_follow_pages_schema(engine: TursoEngine) -> None:
    """
    Create optional follow_pages table.

    Intentionally additive (CREATE TABLE IF NOT EXISTS).
    """
    with turso_connect_autocommit(engine) as conn:
        conn.execute(create_follow_pages_table(FOLLOW_PAGES_TABLE))
        conn.execute(create_follow_pages_index(FOLLOW_PAGES_TABLE))
        cols = table_columns(conn, FOLLOW_PAGES_TABLE)
        if FOLLOW_KEYS_JSON_COLUMN not in cols:
            conn.execute(add_follow_keys_json_column(FOLLOW_PAGES_TABLE))


def ensure_follow_pages_schema(engine: TursoEngine) -> None:
    init_follow_pages_schema(engine)


def try_load_follow_pages(engine: TursoEngine) -> tuple[pd.DataFrame, str]:
    """
    Best-effort load follow_pages.

    Returns: (pages_df, error_message)
    """
    try:
        with turso_connect_autocommit(engine) as conn:
            pages = turso_read_sql_df(conn, select_follow_pages(FOLLOW_PAGES_TABLE))
        return pages, ""
    except Exception as exc:
        return pd.DataFrame(), f"{type(exc).__name__}: {exc}"


def upsert_follow_page(
    engine: TursoEngine,
    *,
    follow_type: str,
    follow_key: str,
    follow_keys: list[str] | None,
    page_name: str,
    keywords_text: str,
) -> str:
    follow_type_norm = normalize_follow_type(follow_type)
    follow_key_norm = str(follow_key or "").strip()
    page_key = make_page_key(follow_type=follow_type_norm, follow_key=follow_key_norm)
    if not page_key:
        raise ValueError("Invalid follow_type/follow_key")

    now = _now_str()
    with turso_connect_autocommit(engine) as conn:
        follow_keys_json = "[]"
        if follow_type_norm == FOLLOW_TYPE_CLUSTER:
            follow_keys_json = "[]"
        elif follow_keys is None:
            # Preserve existing keys list if caller didn't provide a new one.
            try:
                existing = conn.execute(
                    f"SELECT {FOLLOW_KEYS_JSON_COLUMN} FROM {FOLLOW_PAGES_TABLE} WHERE page_key = :page_key",
                    {"page_key": page_key},
                ).scalar()
            except Exception:
                existing = None
            existing_str = str(existing or "").strip()
            follow_keys_json = existing_str if existing_str else "[]"
        else:
            # Explicitly set keys list (may be empty).
            keys: list[str] = []
            seen: set[str] = set()
            for item in follow_keys:
                s = str(item or "").strip()
                if not s or s in seen:
                    continue
                seen.add(s)
                keys.append(s)
            follow_keys_json = json.dumps(keys, ensure_ascii=False)

        conn.execute(
            upsert_follow_page_sql(FOLLOW_PAGES_TABLE),
            {
                "page_key": page_key,
                "follow_type": follow_type_norm,
                "follow_key": follow_key_norm,
                "follow_keys_json": follow_keys_json,
                "page_name": str(page_name or "").strip(),
                "keywords_text": str(keywords_text or ""),
                "now": now,
            },
        )
    return page_key


def delete_follow_page(engine: TursoEngine, *, page_key: str) -> int:
    key = str(page_key or "").strip()
    if not key:
        return 0
    with turso_connect_autocommit(engine) as conn:
        res = conn.execute(
            delete_follow_page_sql(FOLLOW_PAGES_TABLE),
            {"page_key": key},
        )
        return int(res.rowcount or 0)


__all__ = [
    "ALLOWED_FOLLOW_TYPES",
    "FOLLOW_PAGES_TABLE",
    "FOLLOW_TYPE_CLUSTER",
    "FOLLOW_TYPE_TOPIC",
    "delete_follow_page",
    "ensure_follow_pages_schema",
    "init_follow_pages_schema",
    "make_page_key",
    "normalize_follow_type",
    "try_load_follow_pages",
    "upsert_follow_page",
]
