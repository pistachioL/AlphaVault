from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import json

import pandas as pd

from alphavault.db.turso_db import ensure_turso_engine
from alphavault.db.turso_env import load_configured_turso_sources_from_env
from alphavault.env import load_dotenv_if_present
from alphavault.research_stock_cache import (
    load_stock_extras_snapshot,
    load_stock_hot_view,
)
from alphavault_reflex.services.turso_read import (
    MISSING_TURSO_SOURCES_ERROR,
    load_stock_alias_relations_from_env,
)
from alphavault_reflex.services.research_status_text import (
    BACKGROUND_PROCESSING_TEXT,
)
from alphavault.worker.job_state import (
    load_worker_job_cursor,
    worker_progress_state_key,
    WORKER_PROGRESS_STAGE_CYCLE,
)

_STOCK_SIGNAL_CAP = 500
_EMPTY_EXTRAS = {"pending_candidates": [], "backfill_posts": [], "updated_at": ""}


def _normalize_stock_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text if text.startswith("stock:") else f"stock:{text}"


@lru_cache(maxsize=256)
def _load_stock_hot_payload_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    stock_key: str,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    engine = ensure_turso_engine(db_url, auth_token)
    hot = load_stock_hot_view(engine, stock_key=stock_key)
    extras = load_stock_extras_snapshot(engine, stock_key=stock_key)
    progress: dict[str, object] = {}
    cycle_state_key = worker_progress_state_key(
        source_name=source_name,
        stage=WORKER_PROGRESS_STAGE_CYCLE,
    )
    if cycle_state_key:
        raw = load_worker_job_cursor(engine, state_key=cycle_state_key)
        text = str(raw or "").strip()
        if text:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = {}
            if isinstance(parsed, dict):
                progress = {
                    str(key): value
                    for key, value in parsed.items()
                    if str(key or "").strip()
                }
    return hot, extras, progress


def _resolve_stock_key_candidates(stock_key: str) -> list[str]:
    normalized = _normalize_stock_key(stock_key)
    if not normalized:
        return []
    out = [normalized]
    relations, relation_err = load_stock_alias_relations_from_env()
    if relation_err or relations.empty:
        return out
    seen = {normalized}
    for _, row in relations.iterrows():
        relation_type = str(row.get("relation_type") or "").strip()
        relation_label = str(row.get("relation_label") or "").strip()
        if relation_type != "stock_alias" and relation_label != "alias_of":
            continue
        right_key = str(row.get("right_key") or "").strip()
        left_key = str(row.get("left_key") or "").strip()
        if right_key != normalized or not left_key.startswith("stock:"):
            continue
        if left_key in seen:
            continue
        seen.add(left_key)
        out.append(left_key)
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
        if len(cleaned) >= _STOCK_SIGNAL_CAP:
            break
    return cleaned


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


def _merge_related_sectors(hot_rows: list[dict[str, object]]) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for payload in hot_rows:
        rows = payload.get("related_sectors")
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            sector_key = str(row.get("sector_key") or "").strip()
            if not sector_key:
                continue
            try:
                count = int(str(row.get("mention_count") or "0") or 0)
            except ValueError:
                count = 0
            counts[sector_key] = int(counts.get(sector_key, 0)) + max(0, count)
    ranked = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {"sector_key": sector_key, "mention_count": str(count)}
        for sector_key, count in ranked
    ]


def _merge_extras(extras_rows: list[dict[str, object]]) -> dict[str, object]:
    pending: list[dict[str, str]] = []
    backfill: list[dict[str, str]] = []
    seen_pending: set[str] = set()
    seen_backfill: set[str] = set()
    updated_at = ""
    for payload in extras_rows:
        current_updated = str(payload.get("updated_at") or "").strip()
        if current_updated > updated_at:
            updated_at = current_updated
        for row in _dict_rows(payload.get("pending_candidates")):
            candidate_id = str(row.get("candidate_id") or "").strip()
            key = candidate_id or str(row)
            if key in seen_pending:
                continue
            seen_pending.add(key)
            pending.append(
                {str(k): str(v or "").strip() for k, v in row.items() if str(k).strip()}
            )
        for row in _dict_rows(payload.get("backfill_posts")):
            post_uid = str(row.get("post_uid") or "").strip()
            key = post_uid or str(row)
            if key in seen_backfill:
                continue
            seen_backfill.add(key)
            backfill.append(
                {str(k): str(v or "").strip() for k, v in row.items() if str(k).strip()}
            )
    return {
        "pending_candidates": pending,
        "backfill_posts": backfill,
        "updated_at": updated_at,
    }


def _slice_signals(
    rows: list[dict[str, str]],
    *,
    signal_page: int,
    signal_page_size: int,
) -> tuple[list[dict[str, str]], int, int]:
    total = max(int(len(rows)), 0)
    if total <= 0:
        return [], 0, 1
    page_size = max(int(signal_page_size or 1), 1)
    page = max(int(signal_page or 1), 1)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    start = (page - 1) * page_size
    end = start + page_size
    return rows[start:end], total, page


def _merge_worker_cycle_progress(
    progress_rows: list[dict[str, object]],
) -> dict[str, object]:
    running = False
    next_run_at = ""
    updated_at = ""
    for row in progress_rows:
        status = str(row.get("status") or "").strip().lower()
        if status == "running" or bool(row.get("running")):
            running = True
        candidate_next = str(row.get("next_run_at") or "").strip()
        if candidate_next and (not next_run_at or candidate_next < next_run_at):
            next_run_at = candidate_next
        candidate_updated = str(row.get("updated_at") or "").strip()
        if candidate_updated > updated_at:
            updated_at = candidate_updated
    status_text = ""
    if running:
        status_text = "本轮补数中，正在更新AI和缓存。"
    elif updated_at:
        status_text = "后台空闲，等待下一轮。"
    return {
        "worker_running": bool(running),
        "worker_status_text": status_text,
        "worker_next_run_at": next_run_at,
        "worker_cycle_updated_at": updated_at,
    }


def load_stock_cached_view_from_env(
    stock_key: str,
    *,
    signal_page: int,
    signal_page_size: int,
) -> dict[str, object]:
    normalized = _normalize_stock_key(stock_key)
    if not normalized:
        return {
            "entity_key": "",
            "header_title": "",
            "signals": [],
            "signal_total": 0,
            "signal_page": 1,
            "signal_page_size": max(int(signal_page_size or 1), 1),
            "related_sectors": [],
            "pending_candidates": [],
            "backfill_posts": [],
            "extras_updated_at": "",
            "load_error": "",
            "load_warning": "",
            "worker_status_text": "",
            "worker_next_run_at": "",
            "worker_cycle_updated_at": "",
            "worker_running": False,
        }
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return {
            "entity_key": normalized,
            "header_title": normalized.removeprefix("stock:"),
            "signals": [],
            "signal_total": 0,
            "signal_page": 1,
            "signal_page_size": max(int(signal_page_size or 1), 1),
            "related_sectors": [],
            "pending_candidates": [],
            "backfill_posts": [],
            "extras_updated_at": "",
            "load_error": MISSING_TURSO_SOURCES_ERROR,
            "load_warning": "",
            "worker_status_text": "",
            "worker_next_run_at": "",
            "worker_cycle_updated_at": "",
            "worker_running": False,
        }
    key_candidates = _resolve_stock_key_candidates(normalized)
    errors: list[str] = []
    selected_hot_rows: list[dict[str, object]] = []
    selected_extras_rows: list[dict[str, object]] = []
    selected_progress_rows: list[dict[str, object]] = []
    selected_key = normalized

    for candidate in key_candidates:
        hot_rows: list[dict[str, object]] = []
        extras_rows: list[dict[str, object]] = []
        progress_rows: list[dict[str, object]] = []
        max_workers = max(1, min(4, int(len(sources))))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    _load_stock_hot_payload_cached,
                    source.url,
                    source.token,
                    source.name,
                    candidate,
                ): source.name
                for source in sources
            }
            for fut in as_completed(futures):
                source_name = futures.get(fut, "")
                try:
                    hot, extras, progress = fut.result()
                except BaseException as err:
                    errors.append(
                        f"turso_connect_error:{source_name}:{type(err).__name__}"
                    )
                    continue
                if hot:
                    hot_rows.append(hot)
                if extras:
                    extras_rows.append(extras)
                if progress:
                    progress_rows.append(progress)
        merged_signals = _sort_signal_rows(
            [row for payload in hot_rows for row in _dict_rows(payload.get("signals"))]
        )
        if merged_signals:
            selected_hot_rows = hot_rows
            selected_extras_rows = extras_rows
            selected_progress_rows = progress_rows
            selected_key = candidate
            break
        if not selected_hot_rows and (hot_rows or extras_rows or progress_rows):
            selected_hot_rows = hot_rows
            selected_extras_rows = extras_rows
            selected_progress_rows = progress_rows
            selected_key = candidate

    header_title = ""
    entity_key = selected_key
    all_signals = _sort_signal_rows(
        [
            row
            for payload in selected_hot_rows
            for row in _dict_rows(payload.get("signals"))
        ]
    )
    for payload in selected_hot_rows:
        payload_entity = str(payload.get("entity_key") or "").strip()
        payload_title = str(payload.get("header_title") or "").strip()
        if payload_entity and payload_entity.startswith("stock:"):
            entity_key = payload_entity
        if payload_title:
            header_title = payload_title
            break
    if not header_title:
        header_title = entity_key.removeprefix("stock:")

    signal_slice, signal_total, safe_page = _slice_signals(
        all_signals,
        signal_page=signal_page,
        signal_page_size=signal_page_size,
    )
    related_sectors = _merge_related_sectors(selected_hot_rows)
    merged_extras = (
        _merge_extras(selected_extras_rows)
        if selected_extras_rows
        else dict(_EMPTY_EXTRAS)
    )
    worker_progress = _merge_worker_cycle_progress(selected_progress_rows)
    worker_running = bool(worker_progress.get("worker_running"))
    warning = ""
    if signal_total <= 0 and worker_running:
        warning = BACKGROUND_PROCESSING_TEXT
    if errors:
        err_line = errors[0]
        warning = f"{warning} | {err_line}" if warning else err_line
    return {
        "entity_key": entity_key,
        "header_title": header_title,
        "signals": signal_slice,
        "signal_total": signal_total,
        "signal_page": safe_page,
        "signal_page_size": max(int(signal_page_size or 1), 1),
        "related_sectors": related_sectors,
        "pending_candidates": merged_extras.get("pending_candidates", []),
        "backfill_posts": merged_extras.get("backfill_posts", []),
        "extras_updated_at": str(merged_extras.get("updated_at") or "").strip(),
        "load_error": "",
        "load_warning": warning,
        "worker_status_text": str(
            worker_progress.get("worker_status_text") or ""
        ).strip(),
        "worker_next_run_at": str(
            worker_progress.get("worker_next_run_at") or ""
        ).strip(),
        "worker_cycle_updated_at": str(
            worker_progress.get("worker_cycle_updated_at") or ""
        ).strip(),
        "worker_running": worker_running,
    }


def clear_stock_hot_read_caches() -> None:
    _load_stock_hot_payload_cached.cache_clear()


__all__ = [
    "clear_stock_hot_read_caches",
    "load_stock_cached_view_from_env",
]
