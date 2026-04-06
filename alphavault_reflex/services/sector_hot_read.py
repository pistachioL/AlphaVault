from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

import pandas as pd

from alphavault.db.turso_db import ensure_turso_engine
from alphavault.db.turso_env import load_configured_turso_sources_from_env
from alphavault.env import load_dotenv_if_present
from alphavault.research_stock_cache import load_entity_page_signal_snapshot
from alphavault.worker.sector_hot_payload_builder import normalize_sector_key

from .turso_read import MISSING_TURSO_SOURCES_ERROR


_SECTOR_SIGNAL_CAP = 500


@lru_cache(maxsize=256)
def _load_sector_hot_payload_cached(
    db_url: str,
    auth_token: str,
    sector_key: str,
) -> dict[str, object]:
    engine = ensure_turso_engine(db_url, auth_token)
    return load_entity_page_signal_snapshot(engine, stock_key=sector_key)


def _dict_rows(value: object) -> list[dict[str, str]]:
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


def _sort_signal_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    if not rows:
        return []
    frame = pd.DataFrame(rows)
    if "created_at" in frame.columns:
        frame["created_at"] = pd.to_datetime(frame["created_at"], errors="coerce")
        frame = frame.sort_values(by="created_at", ascending=False, na_position="last")
    cleaned: list[dict[str, str]] = []
    seen: set[str] = set()
    for _, row in frame.iterrows():
        payload = {
            str(key): str(value or "").strip() for key, value in row.to_dict().items()
        }
        post_uid = str(payload.get("post_uid") or "").strip()
        if post_uid and post_uid in seen:
            continue
        if post_uid:
            seen.add(post_uid)
        cleaned.append(payload)
        if len(cleaned) >= _SECTOR_SIGNAL_CAP:
            break
    return cleaned


def _merge_related_stocks(hot_rows: list[dict[str, object]]) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for payload in hot_rows:
        for row in _dict_rows(payload.get("related_stocks")):
            stock_key = str(row.get("stock_key") or "").strip()
            if not stock_key:
                continue
            try:
                count = int(str(row.get("mention_count") or "0") or 0)
            except ValueError:
                count = 0
            counts[stock_key] = int(counts.get(stock_key, 0)) + max(0, count)
    ranked = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {"stock_key": stock_key, "mention_count": str(count)}
        for stock_key, count in ranked
    ]


def load_sector_cached_view_from_env(sector_key: str) -> dict[str, object]:
    normalized = normalize_sector_key(sector_key)
    if not normalized:
        return {
            "entity_key": "",
            "header_title": "",
            "signals": [],
            "signal_total": 0,
            "related_stocks": [],
            "load_error": "",
            "snapshot_hit": False,
        }
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return {
            "entity_key": normalized,
            "header_title": normalized.removeprefix("cluster:"),
            "signals": [],
            "signal_total": 0,
            "related_stocks": [],
            "load_error": MISSING_TURSO_SOURCES_ERROR,
            "snapshot_hit": False,
        }
    errors: list[str] = []
    hot_rows: list[dict[str, object]] = []
    max_workers = max(1, min(4, int(len(sources))))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _load_sector_hot_payload_cached,
                source.url,
                source.token,
                normalized,
            ): source.name
            for source in sources
        }
        for fut in as_completed(futures):
            source_name = futures.get(fut, "")
            try:
                hot = fut.result()
            except BaseException as err:
                errors.append(f"turso_connect_error:{source_name}:{type(err).__name__}")
                continue
            if hot:
                hot_rows.append(hot)
    all_signals = _sort_signal_rows(
        [row for payload in hot_rows for row in _dict_rows(payload.get("signals"))]
    )
    header_title = ""
    entity_key = normalized
    for payload in hot_rows:
        payload_entity = str(payload.get("entity_key") or "").strip()
        payload_title = str(payload.get("header_title") or "").strip()
        if payload_entity:
            entity_key = payload_entity
        if payload_title:
            header_title = payload_title
            break
    if not header_title:
        header_title = normalized.removeprefix("cluster:")
    load_error = ""
    if not hot_rows and not sources:
        load_error = MISSING_TURSO_SOURCES_ERROR
    elif not hot_rows and errors and len(errors) == len(sources):
        load_error = errors[0]
    return {
        "entity_key": entity_key,
        "header_title": header_title,
        "signals": all_signals,
        "signal_total": len(all_signals),
        "related_stocks": _merge_related_stocks(hot_rows),
        "load_error": load_error,
        "snapshot_hit": bool(hot_rows),
    }


def clear_sector_hot_read_caches() -> None:
    _load_sector_hot_payload_cached.cache_clear()


__all__ = ["clear_sector_hot_read_caches", "load_sector_cached_view_from_env"]
