from __future__ import annotations

import importlib
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from functools import cache
from types import ModuleType
from typing_extensions import TypedDict

from alphavault.ai._errors import format_llm_error_one_line
from alphavault.ai.analyze import _call_ai_with_openai
from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import (
    infer_platform_from_post_uid,
    require_postgres_source_from_env,
)
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql_rows import read_sql_rows
from alphavault.domains.signal.aggregator import coerce_signal_timestamp
from alphavault.domains.stock.keys import normalize_stock_key
from alphavault.infra.ai.runtime_config import (
    AI_TASK_TRADE_SIGNAL_REVIEW,
    ai_task_runtime_config_from_env,
)
from alphavault.timeutil import now_cst_str

from .service import get_research_workbench_engine_from_env
from .trade_signal_review_repo import (
    TradeSignalReviewKey,
    TradeSignalReviewRecord,
    list_trade_signal_reviews_by_keys,
    upsert_trade_signal_reviews,
)


logger = logging.getLogger(__name__)

TRADE_SIGNAL_REVIEW_VERSION = "trade_signal_review_v1"
DEFAULT_TRADE_SIGNAL_REVIEW_TIMEOUT_SECONDS = 240.0
DEFAULT_TRADE_SIGNAL_REVIEW_HISTORY_WINDOW_DAYS = 90
_MAX_HISTORY_ROWS = 12
_MAX_RAW_TEXT_LENGTH = 420
_MAX_TREE_TEXT_LENGTH = 520
_MAX_REASON_TEXT_LENGTH = 220
_MAX_EVIDENCE_QUOTES = 4
_MAX_EVIDENCE_QUOTE_LENGTH = 120
_READY_STATUS = "ready"
_SKIPPED_STATUS = "skipped"
_FAILED_STATUS = "failed"
_REVIEW_STATUSES = frozenset({_READY_STATUS, _SKIPPED_STATUS, _FAILED_STATUS})
_FRESH_REVIEW_STATUSES = frozenset({_READY_STATUS, _SKIPPED_STATUS})
_MATCH_KIND_ASSERTION = "assertion"
_POSITION_PHASES = frozenset(
    {
        "new_buy",
        "replenish_add",
        "low_cost_old_position_add",
        "rotation_buy",
        "trim_on_strength",
        "exit",
        "unknown",
    }
)
_COPYABILITY_LEVELS = frozenset({"high", "medium", "low", "unknown"})
_BLOCKING_FLAGS = frozenset(
    {
        "low_cost_anchor",
        "partial_rotation",
        "trim_after_runup",
        "prior_position_context",
        "text_explicitly_not_replicable",
    }
)
_EMPTY_TREE_MESSAGE = "没有对话流。"


class TradeReviewHistoryRef(TypedDict):
    assertion_id: str
    post_uid: str
    created_at: str
    action: str
    action_strength: str
    summary: str


class TradeReviewResult(TypedDict):
    review_status: str
    position_phase: str
    copyability: str
    hard_block: bool
    blocking_flags: list[str]
    reason_text: str
    evidence_quotes: list[str]
    history_window_days: int
    history_signal_count: int
    history_refs: list[TradeReviewHistoryRef]
    review_version: str
    reviewed_at: str
    error_text: str


@dataclass(frozen=True)
class _TradeReviewRequest:
    platform: str
    assertion_id: str
    post_uid: str
    stock_key: str
    author: str
    action: str
    action_strength: int
    created_at: str
    summary: str
    raw_text: str
    tree_text: str
    related_stock_keys: tuple[str, ...]


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_int(value: object, *, default: int = 0) -> int:
    raw = _clean_text(value)
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _trim_text(value: object, *, limit: int) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(limit - 3, 0)].rstrip() + "..."


def _normalize_phase(value: object) -> str:
    phase = _clean_text(value).lower()
    return phase if phase in _POSITION_PHASES else "unknown"


def _normalize_review_status(value: object, *, default: str = _FAILED_STATUS) -> str:
    status = _clean_text(value).lower()
    if status in _REVIEW_STATUSES:
        return status
    fallback = _clean_text(default).lower()
    return fallback if fallback in _REVIEW_STATUSES else _FAILED_STATUS


def _normalize_copyability(value: object) -> str:
    level = _clean_text(value).lower()
    return level if level in _COPYABILITY_LEVELS else "unknown"


def _normalize_blocking_flags(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = _clean_text(item).lower()
        if not text or text not in _BLOCKING_FLAGS or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _normalize_evidence_quotes(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = _trim_text(item, limit=_MAX_EVIDENCE_QUOTE_LENGTH)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
        if len(out) >= _MAX_EVIDENCE_QUOTES:
            break
    return out


def _normalize_history_refs(value: object) -> list[TradeReviewHistoryRef]:
    if not isinstance(value, list):
        return []
    out: list[TradeReviewHistoryRef] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "assertion_id": _clean_text(item.get("assertion_id")),
                "post_uid": _clean_text(item.get("post_uid")),
                "created_at": _clean_text(item.get("created_at")),
                "action": _clean_text(item.get("action")),
                "action_strength": _clean_text(item.get("action_strength")),
                "summary": _clean_text(item.get("summary")),
            }
        )
    return out


def _make_review_result(
    *,
    review_status: str,
    position_phase: str = "unknown",
    copyability: str = "unknown",
    hard_block: bool = False,
    blocking_flags: object | None = None,
    reason_text: str = "",
    evidence_quotes: object | None = None,
    history_window_days: int = DEFAULT_TRADE_SIGNAL_REVIEW_HISTORY_WINDOW_DAYS,
    history_signal_count: int = 0,
    history_refs: object | None = None,
    review_version: str = TRADE_SIGNAL_REVIEW_VERSION,
    reviewed_at: str = "",
    error_text: str = "",
) -> TradeReviewResult:
    return {
        "review_status": _normalize_review_status(review_status),
        "position_phase": _normalize_phase(position_phase),
        "copyability": _normalize_copyability(copyability),
        "hard_block": bool(hard_block),
        "blocking_flags": _normalize_blocking_flags(blocking_flags),
        "reason_text": _trim_text(reason_text, limit=_MAX_REASON_TEXT_LENGTH),
        "evidence_quotes": _normalize_evidence_quotes(evidence_quotes),
        "history_window_days": max(0, int(history_window_days or 0)),
        "history_signal_count": max(0, int(history_signal_count or 0)),
        "history_refs": _normalize_history_refs(history_refs),
        "review_version": _clean_text(review_version),
        "reviewed_at": _clean_text(reviewed_at),
        "error_text": _clean_text(error_text),
    }


def coerce_trade_review_result(
    value: object,
    *,
    default_status: str = _SKIPPED_STATUS,
    reason_text: str = "",
) -> TradeReviewResult:
    normalized_default_status = _normalize_review_status(
        default_status,
        default=_SKIPPED_STATUS,
    )
    if not isinstance(value, dict):
        return _make_review_result(
            review_status=normalized_default_status,
            reason_text=reason_text,
        )
    return _make_review_result(
        review_status=_normalize_review_status(
            value.get("review_status"),
            default=normalized_default_status,
        ),
        position_phase=_clean_text(value.get("position_phase")),
        copyability=_clean_text(value.get("copyability")),
        hard_block=bool(value.get("hard_block")),
        blocking_flags=value.get("blocking_flags"),
        reason_text=_clean_text(value.get("reason_text")) or _clean_text(reason_text),
        evidence_quotes=value.get("evidence_quotes"),
        history_window_days=_coerce_int(value.get("history_window_days")),
        history_signal_count=_coerce_int(value.get("history_signal_count")),
        history_refs=value.get("history_refs"),
        review_version=_clean_text(value.get("review_version")),
        reviewed_at=_clean_text(value.get("reviewed_at")),
        error_text=_clean_text(value.get("error_text")),
    )


def _review_result_from_record(record: TradeSignalReviewRecord) -> TradeReviewResult:
    return coerce_trade_review_result(record, default_status=_FAILED_STATUS)


def _record_payload_from_result(
    *,
    request: _TradeReviewRequest,
    result: TradeReviewResult,
    review_model: str,
) -> dict[str, object]:
    return {
        "platform": request.platform,
        "assertion_id": request.assertion_id,
        "post_uid": request.post_uid,
        "stock_key": request.stock_key,
        "author": request.author,
        "action": request.action,
        "action_strength": request.action_strength,
        "position_phase": result["position_phase"],
        "copyability": result["copyability"],
        "review_status": result["review_status"],
        "hard_block": bool(result["hard_block"]),
        "blocking_flags": list(result["blocking_flags"]),
        "reason_text": result["reason_text"],
        "evidence_quotes": list(result["evidence_quotes"]),
        "history_window_days": result["history_window_days"],
        "history_signal_count": result["history_signal_count"],
        "history_refs": list(result["history_refs"]),
        "review_model": _clean_text(review_model),
        "review_version": result["review_version"],
        "error_text": result["error_text"],
        "reviewed_at": result["reviewed_at"],
        "updated_at": result["reviewed_at"] or now_cst_str(),
    }


def _skip_review() -> TradeReviewResult:
    return coerce_trade_review_result(
        None,
        default_status=_SKIPPED_STATUS,
        reason_text="当前信号没有可审查的交易断言。",
    )


def _failed_review(
    *,
    error_text: str,
    history_signal_count: int,
    history_refs: list[TradeReviewHistoryRef],
) -> TradeReviewResult:
    return _make_review_result(
        review_status=_FAILED_STATUS,
        history_signal_count=history_signal_count,
        history_refs=history_refs,
        reviewed_at=now_cst_str(),
        error_text=error_text,
    )


def _is_trade_action(value: object) -> bool:
    return _clean_text(value).startswith("trade.")


def _normalize_related_stock_keys(
    stock_key: str,
    related_stock_keys: Sequence[str] | None,
) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for raw_key in [stock_key, *(related_stock_keys or [])]:
        normalized_key = normalize_stock_key(_clean_text(raw_key))
        if not normalized_key or normalized_key in seen:
            continue
        seen.add(normalized_key)
        out.append(normalized_key)
    return tuple(out)


def _normalize_platform(row: dict[str, object]) -> str:
    raw_platform = _clean_text(row.get("platform") or row.get("source"))
    if raw_platform:
        return raw_platform.lower()
    return infer_platform_from_post_uid(row.get("post_uid"))


def _build_request(
    row: dict[str, object],
    *,
    stock_key: str,
    related_stock_keys: tuple[str, ...],
) -> _TradeReviewRequest | None:
    match_kind = _clean_text(row.get("match_kind")) or _MATCH_KIND_ASSERTION
    action = _clean_text(row.get("action"))
    assertion_id = _clean_text(row.get("assertion_id"))
    platform = _normalize_platform(row)
    post_uid = _clean_text(row.get("post_uid"))
    if match_kind != _MATCH_KIND_ASSERTION or not _is_trade_action(action):
        return None
    if not platform or not assertion_id or not post_uid:
        return None
    return _TradeReviewRequest(
        platform=platform,
        assertion_id=assertion_id,
        post_uid=post_uid,
        stock_key=stock_key,
        author=_clean_text(row.get("author")),
        action=action,
        action_strength=_coerce_int(row.get("action_strength")),
        created_at=_clean_text(row.get("created_at")),
        summary=_clean_text(row.get("summary") or row.get("title")),
        raw_text=_clean_text(row.get("raw_text")),
        tree_text=_clean_text(row.get("tree_text")),
        related_stock_keys=related_stock_keys,
    )


def _format_history_cutoff(created_at: str, *, window_days: int) -> tuple[str, str]:
    ts = coerce_signal_timestamp(created_at)
    if ts is None:
        return "", ""
    window_start = ts - timedelta(days=max(1, int(window_days or 1)))
    return (
        ts.strftime("%Y-%m-%d %H:%M:%S"),
        window_start.strftime("%Y-%m-%d %H:%M:%S"),
    )


def _load_trade_history_rows(
    *,
    request: _TradeReviewRequest,
    history_window_days: int,
) -> tuple[list[TradeReviewHistoryRef], int]:
    if not request.author or not request.related_stock_keys:
        return [], 0
    source = require_postgres_source_from_env(request.platform)
    engine = ensure_postgres_engine(source.dsn, schema_name=source.schema)
    posts_table = _source_table(source.schema, "posts")
    assertions_table = _source_table(source.schema, "assertions")
    assertion_entities_table = _source_table(source.schema, "assertion_entities")
    key_placeholders = make_in_placeholders(
        prefix="stock",
        count=len(request.related_stock_keys),
    )
    params = make_in_params(prefix="stock", values=request.related_stock_keys)
    params.update(
        {
            "author": request.author,
            "current_assertion_id": request.assertion_id,
            "limit": _MAX_HISTORY_ROWS,
        }
    )
    created_before, created_after = _format_history_cutoff(
        request.created_at,
        window_days=history_window_days,
    )
    created_at_expr = _trade_board_created_at_sql_expr("p.created_at")
    time_clauses = ""
    if created_before:
        params["created_before"] = created_before
        time_clauses += (
            f"\n  AND {created_at_expr} <= CAST(:created_before AS timestamptz)"
        )
    if created_after:
        params["created_after"] = created_after
        time_clauses += (
            f"\n  AND {created_at_expr} >= CAST(:created_after AS timestamptz)"
        )
    query = f"""
WITH filtered AS (
    SELECT DISTINCT
        a.assertion_id,
        a.post_uid,
        a.action,
        a.action_strength,
        a.summary,
        p.created_at,
        {created_at_expr} AS created_at_sort
    FROM {assertions_table} a
    JOIN {posts_table} p
      ON p.post_uid = a.post_uid
    JOIN {assertion_entities_table} ae
      ON ae.assertion_id = a.assertion_id
    WHERE ae.entity_type = 'stock'
      AND ae.entity_key IN ({key_placeholders})
      AND a.action LIKE 'trade.%'
      AND a.assertion_id <> :current_assertion_id
      AND p.author = :author
      AND p.processed_at IS NOT NULL{time_clauses}
),
ranked AS (
    SELECT
        assertion_id,
        post_uid,
        action,
        action_strength,
        summary,
        created_at,
        created_at_sort,
        COUNT(*) OVER() AS total_count
    FROM filtered
)
SELECT
    assertion_id,
    post_uid,
    action,
    action_strength,
    summary,
    created_at,
    total_count
FROM ranked
ORDER BY created_at_sort DESC, assertion_id DESC
LIMIT :limit
"""
    with postgres_connect_autocommit(engine) as conn:
        rows = read_sql_rows(conn, query, params=params)
    total_count = _coerce_int(rows[0].get("total_count")) if rows else 0
    history_refs: list[TradeReviewHistoryRef] = []
    for row in rows:
        history_refs.append(
            {
                "assertion_id": _clean_text(row.get("assertion_id")),
                "post_uid": _clean_text(row.get("post_uid")),
                "created_at": _clean_text(row.get("created_at")),
                "action": _clean_text(row.get("action")),
                "action_strength": _clean_text(row.get("action_strength")),
                "summary": _trim_text(row.get("summary"), limit=100),
            }
        )
    return history_refs, max(total_count, len(history_refs))


@cache
def _load_source_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.source_read")


@cache
def _load_homework_board_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.homework_board")


def _load_post_texts(post_uid: str) -> tuple[str, str]:
    from alphavault.domains.thread_tree.service import (
        normalize_tree_lookup_post_uid,
        slice_posts_for_single_post_tree,
    )

    uid = normalize_tree_lookup_post_uid(post_uid)
    if not uid:
        return "", ""
    posts, err = _load_source_read_module().load_single_post_for_tree_from_env(uid)
    if err or not posts:
        return "", ""
    matched_post = next(
        (
            dict(row)
            for row in posts
            if normalize_tree_lookup_post_uid(row.get("post_uid")) == uid
        ),
        dict(posts[0]),
    )
    raw_text = _clean_text(matched_post.get("raw_text"))
    posts_view = slice_posts_for_single_post_tree(post_uid=uid, posts=posts)
    _label, tree_text = (
        _load_homework_board_module().build_tree(post_uid=uid, posts=posts_view)
        if posts_view
        else ("", "")
    )
    return raw_text, _clean_text(tree_text)


def _ensure_request_texts(request: _TradeReviewRequest) -> _TradeReviewRequest:
    if request.raw_text or request.tree_text:
        return request
    raw_text, tree_text = _load_post_texts(request.post_uid)
    return _TradeReviewRequest(
        platform=request.platform,
        assertion_id=request.assertion_id,
        post_uid=request.post_uid,
        stock_key=request.stock_key,
        author=request.author,
        action=request.action,
        action_strength=request.action_strength,
        created_at=request.created_at,
        summary=request.summary,
        raw_text=raw_text,
        tree_text=tree_text,
        related_stock_keys=request.related_stock_keys,
    )


def _format_history_lines(history_refs: list[TradeReviewHistoryRef]) -> str:
    if not history_refs:
        return "（空）"
    return "\n".join(
        [
            (
                f"{idx}. 时间={item['created_at'] or '未知'} | 动作={item['action'] or '未知'}"
                f" | 强度={item['action_strength'] or '0'} | 摘要={item['summary'] or '（空）'}"
            )
            for idx, item in enumerate(history_refs, start=1)
        ]
    )


def _build_trade_review_prompt(
    *,
    request: _TradeReviewRequest,
    history_refs: list[TradeReviewHistoryRef],
    history_signal_count: int,
) -> str:
    raw_text = _trim_text(request.raw_text, limit=_MAX_RAW_TEXT_LENGTH)
    tree_text = _trim_text(request.tree_text, limit=_MAX_TREE_TEXT_LENGTH)
    return f"""
你是交易语境审查助手。你只根据当前帖子文本、对话流和作者在同一只股票上的历史交易发言，判断这条交易信号属于什么仓位阶段，以及普通跟随者当前能不能复制。

当前信号：
- 平台：{request.platform}
- 作者：{request.author or "未知"}
- 股票：{request.stock_key}
- 动作：{request.action}
- 动作强度：{request.action_strength}
- 摘要：{request.summary or "（空）"}
- 原文：{raw_text or "（空）"}
- 对话流：{tree_text or _EMPTY_TREE_MESSAGE}

作者同票历史（90天内，当前信号之前，共 {history_signal_count} 条，展示最近 {len(history_refs)} 条）：
{_format_history_lines(history_refs)}

请输出严格 JSON（不要 Markdown）：
{{
  "position_phase": "new_buy|replenish_add|low_cost_old_position_add|rotation_buy|trim_on_strength|exit|unknown",
  "copyability": "high|medium|low|unknown",
  "blocking_flags": ["low_cost_anchor|partial_rotation|trim_after_runup|prior_position_context|text_explicitly_not_replicable"],
  "reason_text": "一句中文，直接说结论和依据",
  "evidence_quotes": ["最多4条原文或摘要短句"]
}}

判定要求：
- `new_buy`：当前文本和历史都更像第一次建仓或新开仓。
- `replenish_add`：当前是回补、继续加仓、再买一点，但没有明显低成本老仓特征。
- `low_cost_old_position_add`：当前文本或历史明确显示已有低成本老仓、底仓、成本远低于现阶段位置。
- `rotation_buy`：当前文本明确在说换仓、挪仓、卖旧买新、分批换入。
- `trim_on_strength`：当前文本明确在说涨起来先减、兑现、落袋、卖一点。
- `exit`：当前文本明确在说卖出、清仓、走掉。
- `copyability=low`：普通人当前难以复制，常见原因是低成本老仓、上涨后的减仓、部分换仓、文本明确说当前不适合跟。
- 证据不足时用 `unknown`，不要脑补。
""".strip()


def _normalize_ai_review_result(
    parsed: object,
    *,
    history_signal_count: int,
    history_refs: list[TradeReviewHistoryRef],
) -> TradeReviewResult:
    if not isinstance(parsed, dict):
        raise RuntimeError("trade_signal_review_invalid_json")
    copyability = _normalize_copyability(parsed.get("copyability"))
    return _make_review_result(
        review_status=_READY_STATUS,
        position_phase=_normalize_phase(parsed.get("position_phase")),
        copyability=copyability,
        hard_block=copyability == "low",
        blocking_flags=_normalize_blocking_flags(parsed.get("blocking_flags")),
        reason_text=_clean_text(parsed.get("reason_text")),
        evidence_quotes=_normalize_evidence_quotes(parsed.get("evidence_quotes")),
        history_signal_count=history_signal_count,
        history_refs=history_refs,
        reviewed_at=now_cst_str(),
    )


def _generate_review(
    *,
    request: _TradeReviewRequest,
    history_refs: list[TradeReviewHistoryRef],
    history_signal_count: int,
) -> tuple[TradeReviewResult, str]:
    config = ai_task_runtime_config_from_env(
        task_key=AI_TASK_TRADE_SIGNAL_REVIEW,
        timeout_seconds_default=DEFAULT_TRADE_SIGNAL_REVIEW_TIMEOUT_SECONDS,
    )
    if not _clean_text(config.api_key):
        return (
            _failed_review(
                error_text="Missing AI_API_KEY",
                history_signal_count=history_signal_count,
                history_refs=history_refs,
            ),
            "",
        )
    parsed = _call_ai_with_openai(
        prompt=_build_trade_review_prompt(
            request=request,
            history_refs=history_refs,
            history_signal_count=history_signal_count,
        ),
        api_mode=config.api_mode,
        ai_stream=False,
        model_name=config.model,
        base_url=config.base_url,
        api_key=config.api_key,
        timeout_seconds=float(config.timeout_seconds),
        retry_count=int(config.retries),
        temperature=float(config.temperature),
        reasoning_effort=str(config.reasoning_effort),
        trace_out=None,
        trace_label=f"trade_signal_review:{request.assertion_id}",
    )
    return (
        _normalize_ai_review_result(
            parsed,
            history_signal_count=history_signal_count,
            history_refs=history_refs,
        ),
        _clean_text(config.model),
    )


def _trade_board_created_at_sql_expr(column: str) -> str:
    return _load_trade_board_loader_module().trade_board_created_at_sql_expr(column)


@cache
def _load_trade_board_loader_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.trade_board_loader")


def _source_table(source_name: str, table_name: str) -> str:
    return _load_source_loader_module().source_table(source_name, table_name)


@cache
def _load_source_loader_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.source_loader")


def _row_cache_key(request: _TradeReviewRequest) -> TradeSignalReviewKey:
    return (request.platform, request.assertion_id, request.stock_key)


def _is_current_review_fresh(record: TradeSignalReviewRecord) -> bool:
    if _clean_text(record.get("review_version")) != TRADE_SIGNAL_REVIEW_VERSION:
        return False
    return _clean_text(record.get("review_status")) in _FRESH_REVIEW_STATUSES


def _strip_internal_fields(row: dict[str, object]) -> dict[str, object]:
    cleaned = dict(row)
    cleaned.pop("platform", None)
    cleaned.pop("assertion_id", None)
    return cleaned


def enrich_trade_signal_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    stock_key: str,
    related_stock_keys: Sequence[str] | None = None,
) -> list[dict[str, object]]:
    normalized_stock_key = normalize_stock_key(stock_key)
    if not normalized_stock_key or not rows:
        return [
            _strip_internal_fields({**dict(row), "trade_review": _skip_review()})
            for row in rows
        ]
    normalized_related_stock_keys = _normalize_related_stock_keys(
        normalized_stock_key,
        related_stock_keys,
    )
    engine = get_research_workbench_engine_from_env()
    prepared_rows: list[dict[str, object]] = [dict(row) for row in rows]
    requests_by_index: dict[int, _TradeReviewRequest] = {}
    request_keys: list[TradeSignalReviewKey] = []
    for index, row in enumerate(prepared_rows):
        request = _build_request(
            row,
            stock_key=normalized_stock_key,
            related_stock_keys=normalized_related_stock_keys,
        )
        if request is None:
            row["trade_review"] = _skip_review()
            continue
        requests_by_index[index] = _ensure_request_texts(request)
        request_keys.append(_row_cache_key(requests_by_index[index]))
    cached_reviews = list_trade_signal_reviews_by_keys(engine, keys=request_keys)
    payloads_to_upsert: list[dict[str, object]] = []
    for index, row in enumerate(prepared_rows):
        request = requests_by_index.get(index)
        if request is None:
            continue
        cache_key = _row_cache_key(request)
        cached = cached_reviews.get(cache_key)
        if cached is not None and _is_current_review_fresh(cached):
            row["trade_review"] = _review_result_from_record(cached)
            continue
        history_refs, history_signal_count = _load_trade_history_rows(
            request=request,
            history_window_days=DEFAULT_TRADE_SIGNAL_REVIEW_HISTORY_WINDOW_DAYS,
        )
        try:
            review_result, review_model = _generate_review(
                request=request,
                history_refs=history_refs,
                history_signal_count=history_signal_count,
            )
        except BaseException as err:
            logger.warning(
                "[trade_review] generation_error platform=%s assertion_id=%s error=%s",
                request.platform,
                request.assertion_id,
                format_llm_error_one_line(err, limit=240),
            )
            review_result = _failed_review(
                error_text=format_llm_error_one_line(err, limit=240),
                history_signal_count=history_signal_count,
                history_refs=history_refs,
            )
            review_model = ""
        row["trade_review"] = review_result
        payloads_to_upsert.append(
            _record_payload_from_result(
                request=request,
                result=review_result,
                review_model=review_model,
            )
        )
    if payloads_to_upsert:
        upsert_trade_signal_reviews(engine, rows=payloads_to_upsert)
    return [_strip_internal_fields(row) for row in prepared_rows]


def build_trade_review_for_row(
    row: Mapping[str, object],
    *,
    stock_key: str,
    related_stock_keys: Sequence[str] | None = None,
) -> TradeReviewResult:
    enriched_rows = enrich_trade_signal_rows(
        [row],
        stock_key=stock_key,
        related_stock_keys=related_stock_keys,
    )
    if not enriched_rows:
        return _skip_review()
    return coerce_trade_review_result(enriched_rows[0].get("trade_review"))


__all__ = [
    "AI_TASK_TRADE_SIGNAL_REVIEW",
    "DEFAULT_TRADE_SIGNAL_REVIEW_HISTORY_WINDOW_DAYS",
    "DEFAULT_TRADE_SIGNAL_REVIEW_TIMEOUT_SECONDS",
    "TRADE_SIGNAL_REVIEW_VERSION",
    "TradeReviewHistoryRef",
    "TradeReviewResult",
    "build_trade_review_for_row",
    "coerce_trade_review_result",
    "enrich_trade_signal_rows",
]
