from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import TypedDict

from alphavault.db.postgres_db import PostgresConnection, PostgresEngine
from alphavault.db.sql.research_workbench import (
    select_trade_signal_reviews_by_keys,
    upsert_trade_signal_review,
)

from .schema import (
    RESEARCH_TRADE_SIGNAL_REVIEWS_TABLE,
    handle_db_error,
    use_conn,
)


TradeSignalReviewKey = tuple[str, str, str]


TradeSignalReviewRecord = TypedDict(
    "TradeSignalReviewRecord",
    {
        "platform": str,
        "assertion_id": str,
        "post_uid": str,
        "stock_key": str,
        "author": str,
        "action": str,
        "action_strength": int,
        "position_phase": str,
        "copyability": str,
        "review_status": str,
        "hard_block": bool,
        "blocking_flags": list[str],
        "reason_text": str,
        "evidence_quotes": list[str],
        "history_window_days": int,
        "history_signal_count": int,
        "history_refs": list[dict[str, str]],
        "review_model": str,
        "review_version": str,
        "error_text": str,
        "reviewed_at": str,
        "updated_at": str,
    },
)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_int(value: object) -> int:
    raw = _clean_text(value)
    if not raw:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    raw = _clean_text(value).lower()
    return raw in {"1", "true", "t", "yes", "y"}


def _load_json(value: object) -> object:
    if isinstance(value, (list, dict)):
        return value
    text = _clean_text(value)
    if not text:
        return []
    try:
        return json.loads(text)
    except Exception:
        return []


def _coerce_text_list(value: object) -> list[str]:
    parsed = _load_json(value)
    if not isinstance(parsed, list):
        return []
    out: list[str] = []
    for item in parsed:
        text = _clean_text(item)
        if text:
            out.append(text)
    return out


def _coerce_ref_rows(value: object) -> list[dict[str, str]]:
    parsed = _load_json(value)
    if not isinstance(parsed, list):
        return []
    out: list[dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        normalized = {
            str(key): _clean_text(raw)
            for key, raw in item.items()
            if str(key or "").strip()
        }
        if normalized:
            out.append(normalized)
    return out


def _normalize_record(row: Mapping[str, object]) -> TradeSignalReviewRecord:
    return {
        "platform": _clean_text(row.get("platform")),
        "assertion_id": _clean_text(row.get("assertion_id")),
        "post_uid": _clean_text(row.get("post_uid")),
        "stock_key": _clean_text(row.get("stock_key")),
        "author": _clean_text(row.get("author")),
        "action": _clean_text(row.get("action")),
        "action_strength": _coerce_int(row.get("action_strength")),
        "position_phase": _clean_text(row.get("position_phase")),
        "copyability": _clean_text(row.get("copyability")),
        "review_status": _clean_text(row.get("review_status")),
        "hard_block": _coerce_bool(row.get("hard_block")),
        "blocking_flags": _coerce_text_list(row.get("blocking_flags_json")),
        "reason_text": _clean_text(row.get("reason_text")),
        "evidence_quotes": _coerce_text_list(row.get("evidence_quotes_json")),
        "history_window_days": _coerce_int(row.get("history_window_days")),
        "history_signal_count": _coerce_int(row.get("history_signal_count")),
        "history_refs": _coerce_ref_rows(row.get("history_refs_json")),
        "review_model": _clean_text(row.get("review_model")),
        "review_version": _clean_text(row.get("review_version")),
        "error_text": _clean_text(row.get("error_text")),
        "reviewed_at": _clean_text(row.get("reviewed_at")),
        "updated_at": _clean_text(row.get("updated_at")),
    }


def _select_keys_sql_params(keys: list[TradeSignalReviewKey]) -> dict[str, object]:
    params: dict[str, object] = {}
    for idx, (platform, assertion_id, stock_key) in enumerate(keys):
        params[f"platform_{idx}"] = _clean_text(platform)
        params[f"assertion_id_{idx}"] = _clean_text(assertion_id)
        params[f"stock_key_{idx}"] = _clean_text(stock_key)
    return params


def list_trade_signal_reviews_by_keys(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    keys: list[TradeSignalReviewKey],
) -> dict[TradeSignalReviewKey, TradeSignalReviewRecord]:
    normalized_keys = [
        (_clean_text(platform), _clean_text(assertion_id), _clean_text(stock_key))
        for platform, assertion_id, stock_key in keys
        if (
            _clean_text(platform)
            and _clean_text(assertion_id)
            and _clean_text(stock_key)
        )
    ]
    if not normalized_keys:
        return {}
    try:
        with use_conn(engine_or_conn) as conn:
            rows = (
                conn.execute(
                    select_trade_signal_reviews_by_keys(
                        RESEARCH_TRADE_SIGNAL_REVIEWS_TABLE,
                        key_count=len(normalized_keys),
                    ),
                    _select_keys_sql_params(normalized_keys),
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    out: dict[TradeSignalReviewKey, TradeSignalReviewRecord] = {}
    for row in rows:
        normalized_row = _normalize_record(dict(row))
        key = (
            normalized_row["platform"],
            normalized_row["assertion_id"],
            normalized_row["stock_key"],
        )
        out[key] = normalized_row
    return out


def _normalize_payload(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "platform": _clean_text(row.get("platform")),
        "assertion_id": _clean_text(row.get("assertion_id")),
        "post_uid": _clean_text(row.get("post_uid")),
        "stock_key": _clean_text(row.get("stock_key")),
        "author": _clean_text(row.get("author")),
        "action": _clean_text(row.get("action")),
        "action_strength": _coerce_int(row.get("action_strength")),
        "position_phase": _clean_text(row.get("position_phase")),
        "copyability": _clean_text(row.get("copyability")),
        "review_status": _clean_text(row.get("review_status")),
        "hard_block": bool(row.get("hard_block")),
        "blocking_flags_json": json.dumps(
            row.get("blocking_flags") or [],
            ensure_ascii=False,
        ),
        "reason_text": _clean_text(row.get("reason_text")),
        "evidence_quotes_json": json.dumps(
            row.get("evidence_quotes") or [],
            ensure_ascii=False,
        ),
        "history_window_days": max(0, _coerce_int(row.get("history_window_days"))),
        "history_signal_count": max(0, _coerce_int(row.get("history_signal_count"))),
        "history_refs_json": json.dumps(
            row.get("history_refs") or [],
            ensure_ascii=False,
        ),
        "review_model": _clean_text(row.get("review_model")),
        "review_version": _clean_text(row.get("review_version")),
        "error_text": _clean_text(row.get("error_text")),
        "reviewed_at": _clean_text(row.get("reviewed_at")),
        "updated_at": _clean_text(row.get("updated_at")),
    }


def upsert_trade_signal_reviews(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    rows: Sequence[Mapping[str, object]],
) -> None:
    payloads = [
        _normalize_payload(row)
        for row in rows
        if _clean_text(row.get("platform"))
        and _clean_text(row.get("assertion_id"))
        and _clean_text(row.get("stock_key"))
    ]
    if not payloads:
        return
    try:
        with use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_trade_signal_review(RESEARCH_TRADE_SIGNAL_REVIEWS_TABLE),
                payloads,
            )
    except BaseException as err:
        handle_db_error(engine_or_conn, err)


__all__ = [
    "TradeSignalReviewKey",
    "TradeSignalReviewRecord",
    "list_trade_signal_reviews_by_keys",
    "upsert_trade_signal_reviews",
]
