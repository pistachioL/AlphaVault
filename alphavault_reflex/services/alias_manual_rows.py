from __future__ import annotations

import importlib
from functools import cache
from types import ModuleType
from typing import Callable, TypedDict, cast

from alphavault.domains.stock.keys import stock_value


class AliasHistoryHitRow(TypedDict):
    created_at: str
    post_uid: str
    dialogue_text: str


class AliasManualPendingRow(TypedDict, total=False):
    alias_key: str
    attempt_count: str
    status: str
    sample_post_uid: str
    sample_post_url: str
    sample_evidence: str
    sample_raw_text_excerpt: str
    history_hits: list[AliasHistoryHitRow]
    history_hits_count: str
    history_hits_text: str
    updated_at: str
    ai_status: str
    ai_stock_code: str
    ai_official_name: str
    ai_confidence: str
    ai_reason: str
    ai_uncertain: str
    ai_validation_status: str
    ai_status_display: str
    ai_validation_label: str


ModuleLoader = Callable[[], ModuleType]


def _clean_text(value: object) -> str:
    return str(value or "").strip()


@cache
def _load_research_workbench_module() -> ModuleType:
    return importlib.import_module("alphavault.research_workbench")


@cache
def _load_alias_history_context_module() -> ModuleType:
    return importlib.import_module("alphavault.infra.ai.alias_history_context")


@cache
def _load_source_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.source_read")


def _alias_ai_status_label(status: object) -> str:
    text = _clean_text(status)
    if not text:
        return "未跑"
    if text == "skipped":
        return "未判断"
    if text == "error":
        return "失败"
    return text


def _alias_ai_validation_label(status: object) -> str:
    text = _clean_text(status)
    if text == "validated":
        return "A/H 已过表"
    if text == "missing_security_master":
        return "A/H 表里没有这只票"
    if text == "unvalidated_market":
        return "当前市场暂未校验"
    return ""


def _normalize_alias_history_hits(value: object) -> list[AliasHistoryHitRow]:
    if not isinstance(value, list):
        return []
    out: list[AliasHistoryHitRow] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        post_uid = _clean_text(item.get("post_uid"))
        dialogue_text = _clean_text(item.get("dialogue_text"))
        if not post_uid or not dialogue_text:
            continue
        out.append(
            {
                "created_at": _clean_text(item.get("created_at")),
                "post_uid": post_uid,
                "dialogue_text": dialogue_text,
            }
        )
    return out


def format_alias_history_hits_text(value: object) -> str:
    history_hits = _normalize_alias_history_hits(value)
    if not history_hits:
        return ""
    lines: list[str] = []
    for idx, hit in enumerate(history_hits, start=1):
        header_parts = [
            part
            for part in (
                _clean_text(hit.get("created_at")),
                _clean_text(hit.get("post_uid")),
            )
            if part
        ]
        header = " | ".join(header_parts)
        lines.append(f"{idx}. {header}" if header else f"{idx}.")
        dialogue_text = _clean_text(hit.get("dialogue_text"))
        if dialogue_text:
            lines.append(dialogue_text)
    return "\n".join(lines).strip()


def _decorate_alias_task_row(row: AliasManualPendingRow) -> AliasManualPendingRow:
    next_row = cast(AliasManualPendingRow, dict(row))
    next_row.setdefault("sample_post_uid", "")
    next_row.setdefault("sample_post_url", "")
    next_row.setdefault("sample_evidence", "")
    next_row.setdefault("sample_raw_text_excerpt", "")
    next_row.setdefault("history_hits", [])
    next_row.setdefault("history_hits_count", "0")
    next_row.setdefault("history_hits_text", "")
    next_row.setdefault("ai_status", "")
    next_row.setdefault("ai_stock_code", "")
    next_row.setdefault("ai_official_name", "")
    next_row.setdefault("ai_confidence", "")
    next_row.setdefault("ai_reason", "")
    next_row.setdefault("ai_uncertain", "")
    next_row.setdefault("ai_validation_status", "")
    next_row["ai_status_display"] = _alias_ai_status_label(next_row.get("ai_status"))
    next_row["ai_validation_label"] = _alias_ai_validation_label(
        next_row.get("ai_validation_status")
    )
    if not _clean_text(next_row.get("history_hits_text")):
        next_row["history_hits_text"] = format_alias_history_hits_text(
            next_row.get("history_hits")
        )
    return next_row


def _load_sample_post_urls(
    rows: list[dict[str, object]],
    *,
    source_read_loader: ModuleLoader,
) -> dict[str, str]:
    sample_post_uids: list[str] = []
    seen_post_uids: set[str] = set()
    for row in rows:
        sample_post_uid = _clean_text(row.get("sample_post_uid"))
        if not sample_post_uid or sample_post_uid in seen_post_uids:
            continue
        seen_post_uids.add(sample_post_uid)
        sample_post_uids.append(sample_post_uid)
    if not sample_post_uids:
        return {}
    url_map, _ = source_read_loader().load_post_urls_from_env(sample_post_uids)
    return url_map


def _load_alias_history_hits_batch_by_key(
    *,
    history_context: ModuleType,
    requests: list[dict[str, str]],
    request_key_by_alias_key: dict[str, tuple[str, str]],
) -> dict[str, list[AliasHistoryHitRow]]:
    hits_by_request = history_context.load_alias_history_hits_batch(requests=requests)
    out: dict[str, list[AliasHistoryHitRow]] = {}
    for alias_key, request_key in request_key_by_alias_key.items():
        out[alias_key] = _normalize_alias_history_hits(
            hits_by_request.get(request_key, [])
        )
    return out


def _load_alias_history_hits_one_by_one(
    *,
    history_context: ModuleType,
    rows: list[dict[str, object]],
) -> dict[str, list[AliasHistoryHitRow]]:
    cache_by_query: dict[tuple[str, str], list[AliasHistoryHitRow]] = {}
    out: dict[str, list[AliasHistoryHitRow]] = {}
    for row in rows:
        alias_key = _clean_text(row.get("alias_key"))
        sample_post_uid = _clean_text(row.get("sample_post_uid"))
        alias_text = stock_value(alias_key).strip()
        if not alias_key or not sample_post_uid or not alias_text:
            continue
        cache_key = (sample_post_uid, alias_text)
        history_hits = cache_by_query.get(cache_key)
        if history_hits is None:
            try:
                history_hits = _normalize_alias_history_hits(
                    history_context.load_alias_history_hits(
                        keyword_text=alias_text,
                        sample_post_uid=sample_post_uid,
                    )
                )
            except BaseException:
                history_hits = []
            cache_by_query[cache_key] = history_hits
        out[alias_key] = list(history_hits)
    return out


def _load_alias_history_hits_by_key(
    rows: list[dict[str, object]],
    *,
    alias_history_context_loader: ModuleLoader,
) -> dict[str, list[AliasHistoryHitRow]]:
    try:
        history_context = alias_history_context_loader()
    except BaseException:
        return {}
    requests: list[dict[str, str]] = []
    request_key_by_alias_key: dict[str, tuple[str, str]] = {}
    for row in rows:
        alias_key = _clean_text(row.get("alias_key"))
        sample_post_uid = _clean_text(row.get("sample_post_uid"))
        alias_text = stock_value(alias_key).strip()
        if not alias_key or not sample_post_uid or not alias_text:
            continue
        request_key = (sample_post_uid, alias_text)
        request_key_by_alias_key[alias_key] = request_key
        requests.append(
            {
                "sample_post_uid": sample_post_uid,
                "keyword_text": alias_text,
            }
        )
    if not requests:
        return {}
    try:
        batch_loader = getattr(history_context, "load_alias_history_hits_batch", None)
        if callable(batch_loader):
            return _load_alias_history_hits_batch_by_key(
                history_context=history_context,
                requests=requests,
                request_key_by_alias_key=request_key_by_alias_key,
            )
        return _load_alias_history_hits_one_by_one(
            history_context=history_context,
            rows=rows,
        )
    except BaseException:
        return {}


def load_alias_manual_pending_rows(
    *,
    limit: int | None = None,
    research_workbench_loader: ModuleLoader = _load_research_workbench_module,
    source_read_loader: ModuleLoader = _load_source_read_module,
    alias_history_context_loader: ModuleLoader = _load_alias_history_context_module,
) -> tuple[list[AliasManualPendingRow], str]:
    task_rows: list[dict[str, object]] = []
    try:
        research_workbench = research_workbench_loader()
        engine = research_workbench.get_research_workbench_engine_from_env()
        limit_value = None if limit is None else max(1, int(limit))
        while True:
            task_rows = research_workbench.list_pending_alias_resolve_tasks(
                engine,
                limit=limit_value,
            )
            if not task_rows:
                break
            resolved_alias_keys = (
                research_workbench.cleanup_known_pending_alias_resolve_tasks(
                    engine,
                    [_clean_text(row.get("alias_key")) for row in task_rows],
                )
            )
            if not resolved_alias_keys:
                break
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise
        return [], str(exc)
    if not task_rows:
        return [], ""

    sample_post_urls = _load_sample_post_urls(
        task_rows,
        source_read_loader=source_read_loader,
    )
    history_hits_by_key = _load_alias_history_hits_by_key(
        task_rows,
        alias_history_context_loader=alias_history_context_loader,
    )
    out: list[AliasManualPendingRow] = []
    for row in task_rows:
        alias_key = _clean_text(row.get("alias_key"))
        if not alias_key:
            continue
        sample_post_uid = _clean_text(row.get("sample_post_uid"))
        history_hits = history_hits_by_key.get(alias_key, [])
        out.append(
            _decorate_alias_task_row(
                {
                    "alias_key": alias_key,
                    "attempt_count": _clean_text(row.get("attempt_count")) or "0",
                    "status": _clean_text(row.get("status")),
                    "sample_post_uid": sample_post_uid,
                    "sample_post_url": _clean_text(
                        sample_post_urls.get(sample_post_uid)
                    ),
                    "sample_evidence": _clean_text(row.get("sample_evidence")),
                    "sample_raw_text_excerpt": _clean_text(
                        row.get("sample_raw_text_excerpt")
                    ),
                    "history_hits": history_hits,
                    "history_hits_count": str(len(history_hits)),
                    "history_hits_text": format_alias_history_hits_text(history_hits),
                    "updated_at": _clean_text(row.get("updated_at")),
                    "ai_status": _clean_text(row.get("ai_status")),
                    "ai_stock_code": _clean_text(row.get("ai_stock_code")),
                    "ai_official_name": _clean_text(row.get("ai_official_name")),
                    "ai_confidence": _clean_text(row.get("ai_confidence")),
                    "ai_reason": _clean_text(row.get("ai_reason")),
                    "ai_uncertain": _clean_text(row.get("ai_uncertain")),
                    "ai_validation_status": _clean_text(
                        row.get("ai_validation_status")
                    ),
                }
            )
        )
    return out, ""


__all__ = [
    "AliasHistoryHitRow",
    "AliasManualPendingRow",
    "format_alias_history_hits_text",
    "load_alias_manual_pending_rows",
]
