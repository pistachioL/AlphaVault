from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterator

from alphavault.constants import DEFAULT_SPOOL_DIR


ENV_LOCAL_CACHE_DB_PATH = "LOCAL_CACHE_DB_PATH"
DEFAULT_LOCAL_CACHE_DB_PATH = f"{DEFAULT_SPOOL_DIR}/../alphavault-cache.sqlite3"

CACHE_ASSERTIONS_TABLE = "cache_assertions"
CACHE_STOCK_CODES_TABLE = "cache_assertion_stock_codes"
CACHE_STOCK_NAMES_TABLE = "cache_assertion_stock_names"

TRADE_ACTION_PREFIX = "trade."

_SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_.-]+")

SQL_CREATE_ASSERTIONS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CACHE_ASSERTIONS_TABLE} (
    post_uid TEXT NOT NULL,
    idx INTEGER NOT NULL,
    topic_key TEXT NOT NULL,
    action TEXT NOT NULL,
    action_strength INTEGER NOT NULL DEFAULT 0,
    confidence REAL NOT NULL DEFAULT 0,
    stock_codes_json TEXT NOT NULL DEFAULT '[]',
    stock_names_json TEXT NOT NULL DEFAULT '[]',
    industries_json TEXT NOT NULL DEFAULT '[]',
    commodities_json TEXT NOT NULL DEFAULT '[]',
    indices_json TEXT NOT NULL DEFAULT '[]',
    author TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (post_uid, idx)
)
"""

SQL_CREATE_STOCK_CODES_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CACHE_STOCK_CODES_TABLE} (
    post_uid TEXT NOT NULL,
    idx INTEGER NOT NULL,
    stock_code TEXT NOT NULL,
    PRIMARY KEY (post_uid, idx, stock_code)
)
"""

SQL_CREATE_STOCK_NAMES_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CACHE_STOCK_NAMES_TABLE} (
    post_uid TEXT NOT NULL,
    idx INTEGER NOT NULL,
    stock_name TEXT NOT NULL,
    PRIMARY KEY (post_uid, idx, stock_name)
)
"""

SQL_CREATE_ASSERTIONS_TOPIC_KEY_IDX = f"""
CREATE INDEX IF NOT EXISTS idx_{CACHE_ASSERTIONS_TABLE}_topic_key
    ON {CACHE_ASSERTIONS_TABLE}(topic_key)
"""

SQL_CREATE_ASSERTIONS_ACTION_TOPIC_KEY_IDX = f"""
CREATE INDEX IF NOT EXISTS idx_{CACHE_ASSERTIONS_TABLE}_action_topic_key
    ON {CACHE_ASSERTIONS_TABLE}(action, topic_key)
"""

SQL_CREATE_STOCK_CODES_CODE_IDX = f"""
CREATE INDEX IF NOT EXISTS idx_{CACHE_STOCK_CODES_TABLE}_code
    ON {CACHE_STOCK_CODES_TABLE}(stock_code)
"""

SQL_CREATE_STOCK_NAMES_NAME_IDX = f"""
CREATE INDEX IF NOT EXISTS idx_{CACHE_STOCK_NAMES_TABLE}_name
    ON {CACHE_STOCK_NAMES_TABLE}(stock_name)
"""

SQL_DELETE_POST_ASSERTIONS = f"DELETE FROM {CACHE_ASSERTIONS_TABLE} WHERE post_uid = ?"
SQL_DELETE_POST_STOCK_CODES = (
    f"DELETE FROM {CACHE_STOCK_CODES_TABLE} WHERE post_uid = ?"
)
SQL_DELETE_POST_STOCK_NAMES = (
    f"DELETE FROM {CACHE_STOCK_NAMES_TABLE} WHERE post_uid = ?"
)

SQL_INSERT_ASSERTION = f"""
INSERT INTO {CACHE_ASSERTIONS_TABLE} (
    post_uid,
    idx,
    topic_key,
    action,
    action_strength,
    confidence,
    stock_codes_json,
    stock_names_json,
    industries_json,
    commodities_json,
    indices_json,
    author,
    created_at
) VALUES (
    :post_uid,
    :idx,
    :topic_key,
    :action,
    :action_strength,
    :confidence,
    :stock_codes_json,
    :stock_names_json,
    :industries_json,
    :commodities_json,
    :indices_json,
    :author,
    :created_at
)
"""

SQL_INSERT_STOCK_CODE = f"""
INSERT OR IGNORE INTO {CACHE_STOCK_CODES_TABLE}(post_uid, idx, stock_code)
VALUES (:post_uid, :idx, :stock_code)
"""

SQL_INSERT_STOCK_NAME = f"""
INSERT OR IGNORE INTO {CACHE_STOCK_NAMES_TABLE}(post_uid, idx, stock_name)
VALUES (:post_uid, :idx, :stock_name)
"""


def _safe_source_name(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "default"
    cleaned = _SAFE_NAME_RE.sub("_", raw).strip("_")
    return cleaned or "default"


def resolve_local_cache_db_path(*, source_name: str) -> Path:
    raw = str(os.getenv(ENV_LOCAL_CACHE_DB_PATH, "") or "").strip()
    base = raw or str(DEFAULT_LOCAL_CACHE_DB_PATH)
    suffixes = (".sqlite3", ".sqlite", ".db")
    is_file = any(base.lower().endswith(sfx) for sfx in suffixes)
    safe_source = _safe_source_name(source_name)
    if not is_file:
        return Path(base) / f"alphavault_cache.{safe_source}.sqlite3"
    path = Path(base)
    if safe_source == "default":
        return path
    stem = path.stem or "alphavault_cache"
    sfx = path.suffix or ".sqlite3"
    return path.with_name(f"{stem}.{safe_source}{sfx}")


def ensure_local_cache_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(SQL_CREATE_ASSERTIONS_TABLE)
    conn.execute(SQL_CREATE_STOCK_CODES_TABLE)
    conn.execute(SQL_CREATE_STOCK_NAMES_TABLE)
    conn.execute(SQL_CREATE_ASSERTIONS_TOPIC_KEY_IDX)
    conn.execute(SQL_CREATE_ASSERTIONS_ACTION_TOPIC_KEY_IDX)
    conn.execute(SQL_CREATE_STOCK_CODES_CODE_IDX)
    conn.execute(SQL_CREATE_STOCK_NAMES_NAME_IDX)


@contextmanager
def open_local_cache(*, db_path: Path) -> Iterator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        ensure_local_cache_schema(conn)
        yield conn
    finally:
        conn.close()


def _coerce_str_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, tuple):
        return [str(x).strip() for x in value if str(x).strip()]
    return []


def _json_dumps_list(value: list[str]) -> str:
    return json.dumps(list(value or []), ensure_ascii=False)


def apply_outbox_event_payload(
    conn: sqlite3.Connection, *, payload: dict[str, Any]
) -> int:
    """
    Best-effort mirror of AI outputs into local sqlite.

    Rules:
    - Delete existing rows for post_uid first (mirror "overwrite assertions per post").
    - Only keep action starting with "trade." (to keep cache small).
    """
    post_uid = str(payload.get("post_uid") or "").strip()
    if not post_uid:
        return 0
    author = str(payload.get("author") or "").strip()
    created_at = str(payload.get("created_at") or "").strip()
    assertions = payload.get("assertions")
    items = assertions if isinstance(assertions, list) else []

    with conn:
        conn.execute(SQL_DELETE_POST_ASSERTIONS, (post_uid,))
        conn.execute(SQL_DELETE_POST_STOCK_CODES, (post_uid,))
        conn.execute(SQL_DELETE_POST_STOCK_NAMES, (post_uid,))

        inserted = 0
        next_idx = 1
        for raw in items:
            if not isinstance(raw, dict):
                continue
            action = str(raw.get("action") or "").strip()
            if not action.startswith(TRADE_ACTION_PREFIX):
                continue

            topic_key = str(raw.get("topic_key") or "").strip()
            if not topic_key:
                continue

            try:
                action_strength = int(raw.get("action_strength") or 0)
            except Exception:
                action_strength = 0
            try:
                confidence = float(raw.get("confidence") or 0.0)
            except Exception:
                confidence = 0.0

            stock_codes = _coerce_str_list(
                raw.get("stock_codes")
                or raw.get("stock_codes_json")
                or raw.get("stock_codes_list")
            )
            stock_names = _coerce_str_list(
                raw.get("stock_names")
                or raw.get("stock_names_json")
                or raw.get("stock_names_list")
            )
            industries = _coerce_str_list(
                raw.get("industries")
                or raw.get("industries_json")
                or raw.get("industries_list")
            )
            commodities = _coerce_str_list(
                raw.get("commodities")
                or raw.get("commodities_json")
                or raw.get("commodities_list")
            )
            indices = _coerce_str_list(
                raw.get("indices") or raw.get("indices_json") or raw.get("indices_list")
            )

            row = {
                "post_uid": post_uid,
                "idx": int(next_idx),
                "topic_key": topic_key,
                "action": action,
                "action_strength": int(action_strength),
                "confidence": float(confidence),
                "stock_codes_json": _json_dumps_list(stock_codes),
                "stock_names_json": _json_dumps_list(stock_names),
                "industries_json": _json_dumps_list(industries),
                "commodities_json": _json_dumps_list(commodities),
                "indices_json": _json_dumps_list(indices),
                "author": author,
                "created_at": created_at,
            }
            conn.execute(SQL_INSERT_ASSERTION, row)
            for code in stock_codes:
                conn.execute(
                    SQL_INSERT_STOCK_CODE,
                    {"post_uid": post_uid, "idx": int(next_idx), "stock_code": code},
                )
            for name in stock_names:
                conn.execute(
                    SQL_INSERT_STOCK_NAME,
                    {"post_uid": post_uid, "idx": int(next_idx), "stock_name": name},
                )
            inserted += 1
            next_idx += 1
        return int(inserted)


__all__ = [
    "apply_outbox_event_payload",
    "open_local_cache",
    "resolve_local_cache_db_path",
    "CACHE_ASSERTIONS_TABLE",
    "CACHE_STOCK_CODES_TABLE",
    "CACHE_STOCK_NAMES_TABLE",
    "DEFAULT_LOCAL_CACHE_DB_PATH",
    "ENV_LOCAL_CACHE_DB_PATH",
    "TRADE_ACTION_PREFIX",
]
