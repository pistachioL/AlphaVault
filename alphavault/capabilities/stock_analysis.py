from __future__ import annotations

import importlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import cache
from types import ModuleType
from typing import TypedDict

from alphavault.capabilities.stock_lookup import (
    STOCK_RESOLVE_REQUIRED_ERROR,
    resolve_requested_stock_key,
)
from alphavault.domains.stock.keys import normalize_stock_key, stock_value
from alphavault.domains.stock.view_scope import STOCK_VIEW_SCOPE_COMPANY
from alphavault.research_workbench.trade_signal_review_service import (
    TradeReviewResult,
    coerce_trade_review_result,
    enrich_trade_signal_rows,
)

DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS = 90
DEFAULT_STOCK_EVIDENCE_MAX_POSTS = 40
DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS = 12
MAX_STOCK_EVIDENCE_WINDOW_DAYS = 365
MAX_STOCK_EVIDENCE_MAX_POSTS = 120
MAX_PORTFOLIO_INPUT_STOCKS = 30
_MAX_EVIDENCE_SOURCE_ROWS = 180
_ANALYSIS_SOURCE_ROW_LIMIT = _MAX_EVIDENCE_SOURCE_ROWS
_MAX_EVIDENCE_ROWS_PER_AUTHOR = 2
_MAX_TOP_AUTHORS = 12
_MAX_SHARED_AUTHORS = 20
_TEXT_EXCERPT_LIMIT = 280
_RAW_TEXT_EXCERPT_LIMIT = 420
_TREE_TEXT_EXCERPT_LIMIT = 640
_PORTFOLIO_INPUT_REQUIRED_ERROR = "持仓列表为空。"
_COMPANY_STOCK_KEY_MARKET_RANK = {"SH": 0, "SZ": 1, "BJ": 2, "HK": 3}
_COPYABILITY_HIGH = "high"
_COPYABILITY_MEDIUM = "medium"
_COPYABILITY_LOW = "low"
_POSITION_PHASE_NEW_BUY = "new_buy"
_POSITION_PHASE_REPLENISH_ADD = "replenish_add"
_ADD_CANDIDATE_COPYABILITY = frozenset((_COPYABILITY_HIGH, _COPYABILITY_MEDIUM))
_ADD_CANDIDATE_POSITION_PHASES = frozenset(
    (_POSITION_PHASE_NEW_BUY, _POSITION_PHASE_REPLENISH_ADD)
)
_ADD_CANDIDATES_KEY = "add_candidates"
_WATCH_CANDIDATES_KEY = "watch_candidates"
_BLOCKED_CANDIDATES_KEY = "blocked_candidates"

_STANCE_BULLISH = "bullish"
_STANCE_BEARISH = "bearish"
_STANCE_WATCH = "watch"
_STANCE_OTHER = "other"
_STANCE_ORDER = (
    _STANCE_BULLISH,
    _STANCE_BEARISH,
    _STANCE_WATCH,
    _STANCE_OTHER,
)
_BULLISH_ACTIONS = frozenset(
    ("trade.buy", "trade.add", "view.bullish", "valuation.cheap")
)
_BEARISH_ACTIONS = frozenset(
    (
        "trade.sell",
        "trade.reduce",
        "view.bearish",
        "valuation.expensive",
        "risk.warning",
        "risk.event",
    )
)
_WATCH_ACTIONS = frozenset(("trade.hold", "trade.watch"))


class StockActionCountRow(TypedDict):
    action: str
    count: int


class StockStanceCountRow(TypedDict):
    stance: str
    count: int


class StockMatchKindCountRow(TypedDict):
    match_kind: str
    count: int


class StockAuthorRow(TypedDict):
    author: str
    signal_count: int
    bullish_count: int
    bearish_count: int
    watch_count: int
    other_count: int
    latest_created_at: str


class StockEvidenceRow(TypedDict):
    post_uid: str
    author: str
    created_at: str
    url: str
    match_kind: str
    action: str
    action_strength: int
    stance: str
    summary: str
    raw_text: str
    tree_text: str
    trade_review: TradeReviewResult


class StockEvidencePack(TypedDict):
    requested_stock: str
    resolved_stock_key: str
    view_scope: str
    covered_stock_keys: list[str]
    page_title: str
    same_company_stocks: list[dict[str, str]]
    window_days: int
    signal_total: int
    sampled_signal_total: int
    evidence_row_total: int
    author_total: int
    latest_created_at: str
    controversy_score: float
    action_counts: list[StockActionCountRow]
    stance_counts: list[StockStanceCountRow]
    match_kind_counts: list[StockMatchKindCountRow]
    top_authors: list[StockAuthorRow]
    evidence_rows: list[StockEvidenceRow]
    load_error: str


class PortfolioInputRow(TypedDict):
    requested_stock: str
    resolved_stock_key: str
    page_title: str
    status: str
    merged_into_stock_key: str
    error: str


class PortfolioSharedAuthorRow(TypedDict):
    author: str
    signal_count: int
    stock_count: int
    stocks: list[str]


class PortfolioCandidateRow(TypedDict):
    requested_stocks: list[str]
    input_count: int
    requested_stock: str
    resolved_stock_key: str
    page_title: str
    latest_created_at: str
    review_status: str
    copyability: str
    hard_block: bool
    position_phase: str
    blocking_flags: list[str]
    reason_text: str


PortfolioCandidateBuckets = TypedDict(
    "PortfolioCandidateBuckets",
    {
        "add_candidates": list[PortfolioCandidateRow],
        "watch_candidates": list[PortfolioCandidateRow],
        "blocked_candidates": list[PortfolioCandidateRow],
    },
)


class PortfolioCompanyRow(TypedDict):
    requested_stocks: list[str]
    input_count: int
    requested_stock: str
    resolved_stock_key: str
    view_scope: str
    covered_stock_keys: list[str]
    page_title: str
    same_company_stocks: list[dict[str, str]]
    window_days: int
    signal_total: int
    sampled_signal_total: int
    evidence_row_total: int
    author_total: int
    latest_created_at: str
    controversy_score: float
    action_counts: list[StockActionCountRow]
    stance_counts: list[StockStanceCountRow]
    match_kind_counts: list[StockMatchKindCountRow]
    top_authors: list[StockAuthorRow]
    evidence_rows: list[StockEvidenceRow]
    latest_trade_review: TradeReviewResult
    load_error: str


class PortfolioContext(TypedDict):
    requested_stocks: list[str]
    window_days: int
    evidence_row_limit_per_stock: int
    requested_total: int
    resolved_input_total: int
    unresolved_input_total: int
    company_total: int
    covered_security_total: int
    total_signal_total: int
    portfolio_author_total: int
    shared_author_total: int
    covered_stock_keys: list[str]
    inputs: list[PortfolioInputRow]
    unresolved_inputs: list[PortfolioInputRow]
    companies: list[PortfolioCompanyRow]
    candidate_buckets: PortfolioCandidateBuckets
    shared_authors: list[PortfolioSharedAuthorRow]
    load_error: str


@dataclass(frozen=True)
class _AuthorStats:
    signal_count: int
    stance_counts: Counter[str]
    latest_created_at: str


@dataclass(frozen=True)
class _ComputedEvidence:
    pack: StockEvidencePack
    author_counts: Counter[str]
    latest_trade_review: TradeReviewResult


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _trim_text(value: object, *, limit: int) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(limit - 3, 0)].rstrip() + "..."


def _coerce_int(value: object, *, default: int = 0) -> int:
    try:
        return int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(default)


def _clamp_window_days(value: object) -> int:
    parsed = _coerce_int(value, default=DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS)
    if parsed <= 0:
        return DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS
    return min(parsed, MAX_STOCK_EVIDENCE_WINDOW_DAYS)


def _clamp_evidence_limit(value: object, *, default: int) -> int:
    parsed = _coerce_int(value, default=default)
    if parsed <= 0:
        return default
    return max(1, min(parsed, MAX_STOCK_EVIDENCE_MAX_POSTS))


def _dedupe_texts(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _normalize_requested_stocks(stocks: list[str]) -> list[str]:
    cleaned = [_clean_text(item) for item in stocks if _clean_text(item)]
    return cleaned[:MAX_PORTFOLIO_INPUT_STOCKS]


def _source_row_limit(evidence_limit: int) -> int:
    del evidence_limit
    return _ANALYSIS_SOURCE_ROW_LIMIT


def _stance_from_action(action: str) -> str:
    clean_action = _clean_text(action)
    if clean_action in _BULLISH_ACTIONS:
        return _STANCE_BULLISH
    if clean_action in _BEARISH_ACTIONS:
        return _STANCE_BEARISH
    if clean_action in _WATCH_ACTIONS:
        return _STANCE_WATCH
    return _STANCE_OTHER


def _extract_summary(row: dict[str, object]) -> str:
    summary = _trim_text(row.get("summary"), limit=_TEXT_EXCERPT_LIMIT)
    if summary:
        return summary
    tree_text = _trim_text(row.get("tree_text"), limit=_TEXT_EXCERPT_LIMIT)
    if tree_text:
        return tree_text
    return _trim_text(row.get("raw_text"), limit=_TEXT_EXCERPT_LIMIT)


def _normalize_signal_row(row: dict[str, object]) -> StockEvidenceRow:
    action = _clean_text(row.get("action"))
    return {
        "post_uid": _clean_text(row.get("post_uid")),
        "author": _clean_text(row.get("author")),
        "created_at": _clean_text(row.get("created_at_line"))
        or _clean_text(row.get("created_at")),
        "url": _clean_text(row.get("url")),
        "match_kind": _clean_text(row.get("match_kind")),
        "action": action,
        "action_strength": max(0, min(_coerce_int(row.get("action_strength")), 3)),
        "stance": _stance_from_action(action),
        "summary": _extract_summary(row),
        "raw_text": _trim_text(row.get("raw_text"), limit=_RAW_TEXT_EXCERPT_LIMIT),
        "tree_text": _trim_text(row.get("tree_text"), limit=_TREE_TEXT_EXCERPT_LIMIT),
        "trade_review": coerce_trade_review_result(row.get("trade_review")),
    }


def _signal_row_dedupe_key(row: StockEvidenceRow) -> str:
    match_kind = _clean_text(row.get("match_kind"))
    action = _clean_text(row.get("action"))
    author = _clean_text(row.get("author"))
    post_uid = _clean_text(row.get("post_uid"))
    url = _clean_text(row.get("url"))
    text = (
        _clean_text(row.get("raw_text"))
        or _clean_text(row.get("tree_text"))
        or _clean_text(row.get("summary"))
    )
    if url and text:
        return (
            f"url:{url}|kind:{match_kind}|action:{action}|author:{author}|text:{text}"
        )
    if text:
        return f"kind:{match_kind}|action:{action}|author:{author}|text:{text}"
    if url:
        return f"url:{url}|kind:{match_kind}|action:{action}|author:{author}"
    if post_uid:
        return f"post_uid:{post_uid}"
    created_at = _clean_text(row.get("created_at"))
    return f"kind:{match_kind}|action:{action}|author:{author}|created_at:{created_at}"


def _dedupe_signal_rows(rows: list[StockEvidenceRow]) -> list[StockEvidenceRow]:
    out: list[StockEvidenceRow] = []
    seen: set[str] = set()
    for row in rows:
        dedupe_key = _signal_row_dedupe_key(row)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(row)
    return out


def _count_rows_by_key(
    rows: list[StockEvidenceRow],
    *,
    key: str,
) -> list[dict[str, int | str]]:
    counts = Counter(
        _clean_text(row.get(key)) for row in rows if _clean_text(row.get(key))
    )
    return [
        {key: name, "count": int(count)}
        for name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _build_action_counts(rows: list[StockEvidenceRow]) -> list[StockActionCountRow]:
    ranked = _count_rows_by_key(rows, key="action")
    return [
        {"action": _clean_text(row.get("action")), "count": int(row.get("count") or 0)}
        for row in ranked
    ]


def _build_match_kind_counts(
    rows: list[StockEvidenceRow],
) -> list[StockMatchKindCountRow]:
    ranked = _count_rows_by_key(rows, key="match_kind")
    return [
        {
            "match_kind": _clean_text(row.get("match_kind")),
            "count": int(row.get("count") or 0),
        }
        for row in ranked
    ]


def _build_stance_counts(rows: list[StockEvidenceRow]) -> list[StockStanceCountRow]:
    counts = Counter(
        str(row.get("stance") or "").strip() for row in rows if row.get("stance")
    )
    return [
        {"stance": stance, "count": int(counts.get(stance, 0))}
        for stance in _STANCE_ORDER
        if int(counts.get(stance, 0)) > 0
    ]


def _build_author_stats(rows: list[StockEvidenceRow]) -> dict[str, _AuthorStats]:
    author_counts: Counter[str] = Counter()
    author_stances: dict[str, Counter[str]] = defaultdict(Counter)
    latest_created_at: dict[str, str] = {}
    for row in rows:
        author = _clean_text(row.get("author"))
        if not author:
            continue
        author_counts[author] += 1
        author_stances[author][_clean_text(row.get("stance"))] += 1
        created_at = _clean_text(row.get("created_at"))
        if created_at and created_at >= _clean_text(latest_created_at.get(author)):
            latest_created_at[author] = created_at
    return {
        author: _AuthorStats(
            signal_count=int(author_counts[author]),
            stance_counts=Counter(author_stances[author]),
            latest_created_at=_clean_text(latest_created_at.get(author)),
        )
        for author in author_counts
    }


def _build_top_authors(rows: list[StockEvidenceRow]) -> list[StockAuthorRow]:
    author_stats = _build_author_stats(rows)
    ranked_authors = sorted(
        author_stats.items(),
        key=lambda item: (
            int(item[1].signal_count),
            item[1].latest_created_at,
            item[0],
        ),
        reverse=True,
    )
    out: list[StockAuthorRow] = []
    for author, stats in ranked_authors[:_MAX_TOP_AUTHORS]:
        out.append(
            {
                "author": author,
                "signal_count": int(stats.signal_count),
                "bullish_count": int(stats.stance_counts.get(_STANCE_BULLISH, 0)),
                "bearish_count": int(stats.stance_counts.get(_STANCE_BEARISH, 0)),
                "watch_count": int(stats.stance_counts.get(_STANCE_WATCH, 0)),
                "other_count": int(stats.stance_counts.get(_STANCE_OTHER, 0)),
                "latest_created_at": stats.latest_created_at,
            }
        )
    return out


def _latest_trade_review_from_rows(rows: list[StockEvidenceRow]) -> TradeReviewResult:
    if not rows:
        return coerce_trade_review_result(None)
    return coerce_trade_review_result(rows[0].get("trade_review"))


def _compute_controversy_score(rows: list[StockEvidenceRow]) -> float:
    if not rows:
        return 0.0
    stance_counts = Counter(_clean_text(row.get("stance")) for row in rows)
    bullish = int(stance_counts.get(_STANCE_BULLISH, 0))
    bearish = int(stance_counts.get(_STANCE_BEARISH, 0))
    watch = int(stance_counts.get(_STANCE_WATCH, 0))
    total = max(len(rows), 1)
    decisive = bullish + bearish
    decisive_share = decisive / total
    watch_share = watch / total
    if decisive <= 0:
        return round(watch_share * 25.0, 1)
    balance = 1.0 - abs(bullish - bearish) / decisive
    score = 100.0 * ((0.7 * balance) + (0.2 * decisive_share) + (0.1 * watch_share))
    return round(max(0.0, min(score, 100.0)), 1)


def _compress_evidence_rows(
    rows: list[StockEvidenceRow],
    *,
    limit: int,
) -> list[StockEvidenceRow]:
    deduped_rows = _dedupe_signal_rows(rows)
    if len(deduped_rows) <= limit:
        return deduped_rows
    buckets: dict[str, list[StockEvidenceRow]] = {
        stance: [] for stance in _STANCE_ORDER
    }
    for row in deduped_rows:
        buckets[_clean_text(row.get("stance")) or _STANCE_OTHER].append(row)
    non_empty_stances = [stance for stance in _STANCE_ORDER if buckets.get(stance)]
    if not non_empty_stances:
        return deduped_rows[:limit]
    target_per_stance = max(1, limit // len(non_empty_stances))
    selected: list[StockEvidenceRow] = []
    selected_row_keys: set[str] = set()
    author_counts: Counter[str] = Counter()
    for stance in non_empty_stances:
        picked = 0
        for row in buckets.get(stance, []):
            row_key = _signal_row_dedupe_key(row)
            author = _clean_text(row.get("author"))
            if row_key in selected_row_keys:
                continue
            if author and int(author_counts[author]) >= _MAX_EVIDENCE_ROWS_PER_AUTHOR:
                continue
            selected.append(row)
            selected_row_keys.add(row_key)
            if author:
                author_counts[author] += 1
            picked += 1
            if picked >= target_per_stance or len(selected) >= limit:
                break
        if len(selected) >= limit:
            return selected[:limit]
    for row in deduped_rows:
        if len(selected) >= limit:
            break
        row_key = _signal_row_dedupe_key(row)
        author = _clean_text(row.get("author"))
        if row_key in selected_row_keys:
            continue
        if author and int(author_counts[author]) >= _MAX_EVIDENCE_ROWS_PER_AUTHOR:
            continue
        selected.append(row)
        selected_row_keys.add(row_key)
        if author:
            author_counts[author] += 1
    if len(selected) >= limit:
        return selected[:limit]
    for row in deduped_rows:
        if len(selected) >= limit:
            break
        row_key = _signal_row_dedupe_key(row)
        if row_key in selected_row_keys:
            continue
        selected.append(row)
        selected_row_keys.add(row_key)
    return selected[:limit]


@cache
def _load_stock_hot_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.stock_hot_read")


def _load_stock_view(
    resolved_stock_key: str,
    *,
    window_days: int,
    source_row_limit: int,
) -> dict[str, object]:
    return _load_stock_hot_read_module().load_stock_page_rows_from_env(
        resolved_stock_key,
        signal_page=1,
        signal_page_size=max(1, int(source_row_limit)),
        signal_window_days=window_days,
        related_filter="all",
        view_scope=STOCK_VIEW_SCOPE_COMPANY,
    )


def _trim_named_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        out.append(
            {str(key): _clean_text(raw) for key, raw in row.items() if _clean_text(key)}
        )
    return out


def _trim_stock_key_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        stock_key = normalize_stock_key(_clean_text(item))
        if not stock_key or stock_key in seen:
            continue
        seen.add(stock_key)
        out.append(stock_key)
    return out


def _stock_key_sort_key(stock_key: str) -> tuple[int, str]:
    normalized_stock_key = normalize_stock_key(stock_key)
    stock_code = stock_value(normalized_stock_key)
    if "." not in stock_code:
        return (99, normalized_stock_key)
    code, market = stock_code.rsplit(".", 1)
    if not code.strip().isdigit():
        return (99, normalized_stock_key)
    return (_COMPANY_STOCK_KEY_MARKET_RANK.get(market.strip(), 9), stock_code)


def _sort_stock_keys(stock_keys: list[str]) -> list[str]:
    return sorted(_trim_stock_key_list(stock_keys), key=_stock_key_sort_key)


def _same_company_rows_by_stock_key(
    same_company_stocks: object,
) -> dict[str, dict[str, str]]:
    rows_by_stock_key: dict[str, dict[str, str]] = {}
    for row in _trim_named_rows(same_company_stocks):
        stock_key = normalize_stock_key(_clean_text(row.get("stock_key")))
        if not stock_key or stock_key in rows_by_stock_key:
            continue
        rows_by_stock_key[stock_key] = {
            "stock_key": stock_key,
            "label": _clean_text(row.get("label")) or stock_key.removeprefix("stock:"),
            "official_name": _clean_text(row.get("official_name")),
        }
    return rows_by_stock_key


def _company_stock_keys_from_pack(pack: StockEvidencePack) -> list[str]:
    rows_by_stock_key = _same_company_rows_by_stock_key(pack.get("same_company_stocks"))
    return _sort_stock_keys(
        [
            *_trim_stock_key_list(pack.get("covered_stock_keys")),
            *rows_by_stock_key.keys(),
            _clean_text(pack.get("resolved_stock_key")),
        ]
    )


def _company_identity_from_pack(
    pack: StockEvidencePack,
) -> tuple[str, str, list[str]]:
    company_stock_keys = _company_stock_keys_from_pack(pack)
    representative_stock_key = (
        company_stock_keys[0]
        if company_stock_keys
        else normalize_stock_key(_clean_text(pack.get("resolved_stock_key")))
    )
    group_key = "|".join(company_stock_keys) or representative_stock_key
    return group_key, representative_stock_key, company_stock_keys


def _canonicalize_company_pack(
    computed: _ComputedEvidence,
    *,
    requested_stock: str,
    representative_stock_key: str,
    company_stock_keys: list[str],
) -> _ComputedEvidence:
    base_pack = computed.pack
    rows_by_stock_key = _same_company_rows_by_stock_key(
        base_pack.get("same_company_stocks")
    )
    for stock_key in company_stock_keys:
        rows_by_stock_key.setdefault(
            stock_key,
            {
                "stock_key": stock_key,
                "label": stock_key.removeprefix("stock:"),
                "official_name": "",
            },
        )
    canonical_pack: StockEvidencePack = {
        **base_pack,
        "requested_stock": _clean_text(requested_stock),
        "resolved_stock_key": _clean_text(representative_stock_key),
        "covered_stock_keys": list(company_stock_keys),
        "same_company_stocks": [
            rows_by_stock_key[stock_key]
            for stock_key in _sort_stock_keys(list(rows_by_stock_key))
        ],
    }
    return _ComputedEvidence(
        pack=canonical_pack,
        author_counts=Counter(computed.author_counts),
        latest_trade_review=coerce_trade_review_result(computed.latest_trade_review),
    )


def _empty_stock_evidence_pack(
    *,
    requested_stock: str,
    evidence_limit: int,
    window_days: int,
    load_error: str,
) -> StockEvidencePack:
    return {
        "requested_stock": _clean_text(requested_stock),
        "resolved_stock_key": "",
        "view_scope": STOCK_VIEW_SCOPE_COMPANY,
        "covered_stock_keys": [],
        "page_title": "",
        "same_company_stocks": [],
        "window_days": int(window_days),
        "signal_total": 0,
        "sampled_signal_total": 0,
        "evidence_row_total": 0,
        "author_total": 0,
        "latest_created_at": "",
        "controversy_score": 0.0,
        "action_counts": [],
        "stance_counts": [],
        "match_kind_counts": [],
        "top_authors": [],
        "evidence_rows": [],
        "load_error": _clean_text(load_error),
    }


def _build_stock_evidence_pack_for_resolved_key(
    requested_stock: str,
    resolved_stock_key: str,
    *,
    window_days: int,
    evidence_limit: int,
) -> _ComputedEvidence:
    source_row_limit = _source_row_limit(evidence_limit)
    view = _load_stock_view(
        resolved_stock_key,
        window_days=window_days,
        source_row_limit=source_row_limit,
    )
    raw_rows = view.get("signals")
    signal_row_source = raw_rows if isinstance(raw_rows, list) else []
    covered_stock_keys = _trim_stock_key_list(view.get("covered_stock_keys"))
    signal_row_source = enrich_trade_signal_rows(
        [dict(row) for row in signal_row_source if isinstance(row, dict)],
        stock_key=resolved_stock_key,
        related_stock_keys=covered_stock_keys,
    )
    signal_rows = _dedupe_signal_rows(
        [
            _normalize_signal_row(row)
            for row in signal_row_source
            if isinstance(row, dict)
        ]
    )
    evidence_rows = _compress_evidence_rows(signal_rows, limit=evidence_limit)
    author_counts = Counter(
        _clean_text(row.get("author"))
        for row in signal_rows
        if _clean_text(row.get("author"))
    )
    pack: StockEvidencePack = {
        "requested_stock": _clean_text(requested_stock),
        "resolved_stock_key": _clean_text(resolved_stock_key),
        "view_scope": STOCK_VIEW_SCOPE_COMPANY,
        "covered_stock_keys": covered_stock_keys,
        "page_title": _clean_text(view.get("page_title")),
        "same_company_stocks": _trim_named_rows(view.get("same_company_stocks")),
        "window_days": int(window_days),
        "signal_total": max(0, _coerce_int(view.get("signal_total"))),
        "sampled_signal_total": len(signal_rows),
        "evidence_row_total": len(evidence_rows),
        "author_total": len(author_counts),
        "latest_created_at": _clean_text(signal_rows[0].get("created_at"))
        if signal_rows
        else "",
        "controversy_score": _compute_controversy_score(signal_rows),
        "action_counts": _build_action_counts(signal_rows),
        "stance_counts": _build_stance_counts(signal_rows),
        "match_kind_counts": _build_match_kind_counts(signal_rows),
        "top_authors": _build_top_authors(signal_rows),
        "evidence_rows": evidence_rows,
        "load_error": _clean_text(view.get("load_error")),
    }
    return _ComputedEvidence(
        pack=pack,
        author_counts=author_counts,
        latest_trade_review=_latest_trade_review_from_rows(signal_rows),
    )


def get_stock_evidence_pack(
    stock: str,
    *,
    window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    max_posts: int = DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
) -> StockEvidencePack:
    safe_window_days = _clamp_window_days(window_days)
    safe_max_posts = _clamp_evidence_limit(
        max_posts,
        default=DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
    )
    requested_stock = _clean_text(stock)
    resolved_stock_key = resolve_requested_stock_key(
        requested_stock,
        view_scope=STOCK_VIEW_SCOPE_COMPANY,
    )
    if not resolved_stock_key:
        return _empty_stock_evidence_pack(
            requested_stock=requested_stock,
            evidence_limit=safe_max_posts,
            window_days=safe_window_days,
            load_error=STOCK_RESOLVE_REQUIRED_ERROR,
        )
    return _build_stock_evidence_pack_for_resolved_key(
        requested_stock,
        resolved_stock_key,
        window_days=safe_window_days,
        evidence_limit=safe_max_posts,
    ).pack


def _portfolio_company_row(
    *,
    requested_stocks: list[str],
    computed: _ComputedEvidence,
) -> PortfolioCompanyRow:
    pack = computed.pack
    return {
        "requested_stocks": requested_stocks,
        "input_count": len(requested_stocks),
        "requested_stock": pack["requested_stock"],
        "resolved_stock_key": pack["resolved_stock_key"],
        "view_scope": pack["view_scope"],
        "covered_stock_keys": list(pack["covered_stock_keys"]),
        "page_title": pack["page_title"],
        "same_company_stocks": list(pack["same_company_stocks"]),
        "window_days": pack["window_days"],
        "signal_total": pack["signal_total"],
        "sampled_signal_total": pack["sampled_signal_total"],
        "evidence_row_total": pack["evidence_row_total"],
        "author_total": pack["author_total"],
        "latest_created_at": pack["latest_created_at"],
        "controversy_score": pack["controversy_score"],
        "action_counts": list(pack["action_counts"]),
        "stance_counts": list(pack["stance_counts"]),
        "match_kind_counts": list(pack["match_kind_counts"]),
        "top_authors": list(pack["top_authors"]),
        "evidence_rows": list(pack["evidence_rows"]),
        "latest_trade_review": coerce_trade_review_result(computed.latest_trade_review),
        "load_error": pack["load_error"],
    }


def _empty_portfolio_candidate_buckets() -> PortfolioCandidateBuckets:
    return {
        "add_candidates": [],
        "watch_candidates": [],
        "blocked_candidates": [],
    }


def _portfolio_candidate_row(
    company: PortfolioCompanyRow,
) -> PortfolioCandidateRow:
    review = coerce_trade_review_result(company.get("latest_trade_review"))
    return {
        "requested_stocks": [
            _clean_text(item)
            for item in company.get("requested_stocks", [])
            if _clean_text(item)
        ],
        "input_count": int(company.get("input_count") or 0),
        "requested_stock": _clean_text(company.get("requested_stock")),
        "resolved_stock_key": _clean_text(company.get("resolved_stock_key")),
        "page_title": _clean_text(company.get("page_title")),
        "latest_created_at": _clean_text(company.get("latest_created_at")),
        "review_status": _clean_text(review.get("review_status")),
        "copyability": _clean_text(review.get("copyability")),
        "hard_block": bool(review.get("hard_block")),
        "position_phase": _clean_text(review.get("position_phase")),
        "blocking_flags": [
            _clean_text(item)
            for item in review.get("blocking_flags", [])
            if _clean_text(item)
        ],
        "reason_text": _clean_text(review.get("reason_text")),
    }


def _sort_portfolio_candidate_rows(
    rows: list[PortfolioCandidateRow],
) -> list[PortfolioCandidateRow]:
    ordered_rows = list(rows)
    ordered_rows.sort(
        key=lambda row: (
            _clean_text(row.get("page_title")),
            _clean_text(row.get("resolved_stock_key")),
        )
    )
    ordered_rows.sort(
        key=lambda row: _clean_text(row.get("latest_created_at")),
        reverse=True,
    )
    return ordered_rows


def _build_portfolio_candidate_buckets(
    companies: list[PortfolioCompanyRow],
) -> PortfolioCandidateBuckets:
    buckets: dict[str, list[PortfolioCandidateRow]] = {
        _ADD_CANDIDATES_KEY: [],
        _WATCH_CANDIDATES_KEY: [],
        _BLOCKED_CANDIDATES_KEY: [],
    }
    for company in companies:
        candidate = _portfolio_candidate_row(company)
        hard_block = bool(candidate.get("hard_block"))
        copyability = _clean_text(candidate.get("copyability"))
        position_phase = _clean_text(candidate.get("position_phase"))
        if hard_block or copyability == _COPYABILITY_LOW:
            buckets[_BLOCKED_CANDIDATES_KEY].append(candidate)
            continue
        if (
            copyability in _ADD_CANDIDATE_COPYABILITY
            and position_phase in _ADD_CANDIDATE_POSITION_PHASES
        ):
            buckets[_ADD_CANDIDATES_KEY].append(candidate)
            continue
        buckets[_WATCH_CANDIDATES_KEY].append(candidate)
    return {
        "add_candidates": _sort_portfolio_candidate_rows(buckets[_ADD_CANDIDATES_KEY]),
        "watch_candidates": _sort_portfolio_candidate_rows(
            buckets[_WATCH_CANDIDATES_KEY]
        ),
        "blocked_candidates": _sort_portfolio_candidate_rows(
            buckets[_BLOCKED_CANDIDATES_KEY]
        ),
    }


def _build_shared_author_rows(
    *,
    computed_by_company_key: dict[str, _ComputedEvidence],
    page_title_by_company_key: dict[str, str],
) -> tuple[list[PortfolioSharedAuthorRow], int]:
    author_to_companies: dict[str, set[str]] = defaultdict(set)
    author_signal_counts: Counter[str] = Counter()
    for company_key, computed in computed_by_company_key.items():
        for author, signal_count in computed.author_counts.items():
            clean_author = _clean_text(author)
            if not clean_author:
                continue
            author_to_companies[clean_author].add(company_key)
            author_signal_counts[clean_author] += int(signal_count)
    ranked = sorted(
        (
            (
                author,
                author_signal_counts[author],
                sorted(author_to_companies[author]),
            )
            for author in author_to_companies
            if len(author_to_companies[author]) > 1
        ),
        key=lambda item: (-item[1], -len(item[2]), item[0]),
    )
    return (
        [
            {
                "author": author,
                "signal_count": int(signal_count),
                "stock_count": len(company_keys),
                "stocks": [
                    _clean_text(page_title_by_company_key.get(company_key))
                    or company_key
                    for company_key in company_keys
                ],
            }
            for author, signal_count, company_keys in ranked[:_MAX_SHARED_AUTHORS]
        ],
        len(ranked),
    )


def get_portfolio_context(
    stocks: list[str],
    *,
    window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    max_posts_per_stock: int = DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS,
) -> PortfolioContext:
    requested_stocks = _normalize_requested_stocks(stocks)
    safe_window_days = _clamp_window_days(window_days)
    safe_max_posts = _clamp_evidence_limit(
        max_posts_per_stock,
        default=DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS,
    )
    if not requested_stocks:
        return {
            "requested_stocks": [],
            "window_days": safe_window_days,
            "evidence_row_limit_per_stock": safe_max_posts,
            "requested_total": 0,
            "resolved_input_total": 0,
            "unresolved_input_total": 0,
            "company_total": 0,
            "covered_security_total": 0,
            "total_signal_total": 0,
            "portfolio_author_total": 0,
            "shared_author_total": 0,
            "covered_stock_keys": [],
            "inputs": [],
            "unresolved_inputs": [],
            "companies": [],
            "candidate_buckets": _empty_portfolio_candidate_buckets(),
            "shared_authors": [],
            "load_error": _PORTFOLIO_INPUT_REQUIRED_ERROR,
        }
    input_rows: list[PortfolioInputRow] = []
    unresolved_inputs: list[PortfolioInputRow] = []
    requested_by_resolved_key: dict[str, list[str]] = defaultdict(list)
    resolved_total = 0
    for requested_stock in requested_stocks:
        resolved_stock_key = resolve_requested_stock_key(
            requested_stock,
            view_scope=STOCK_VIEW_SCOPE_COMPANY,
        )
        if not resolved_stock_key:
            row: PortfolioInputRow = {
                "requested_stock": requested_stock,
                "resolved_stock_key": "",
                "page_title": "",
                "status": "unresolved",
                "merged_into_stock_key": "",
                "error": STOCK_RESOLVE_REQUIRED_ERROR,
            }
            input_rows.append(row)
            unresolved_inputs.append(row)
            continue
        resolved_total += 1
        requested_by_resolved_key[resolved_stock_key].append(requested_stock)
        input_rows.append(
            {
                "requested_stock": requested_stock,
                "resolved_stock_key": resolved_stock_key,
                "page_title": "",
                "status": "resolved",
                "merged_into_stock_key": resolved_stock_key,
                "error": "",
            }
        )
    raw_computed_by_resolved_key: dict[str, _ComputedEvidence] = {}
    company_key_by_resolved_key: dict[str, str] = {}
    company_stock_keys_by_company_key: dict[str, list[str]] = {}
    requested_by_company_key: dict[str, list[str]] = defaultdict(list)
    resolved_stock_keys_by_company_key: dict[str, list[str]] = defaultdict(list)
    computed_by_company_key: dict[str, _ComputedEvidence] = {}
    merged_into_stock_key_by_resolved_key: dict[str, str] = {}
    companies: list[PortfolioCompanyRow] = []
    covered_stock_keys: list[str] = []
    author_counts: Counter[str] = Counter()
    for (
        resolved_stock_key,
        merged_requested_stocks,
    ) in requested_by_resolved_key.items():
        computed = _build_stock_evidence_pack_for_resolved_key(
            merged_requested_stocks[0],
            resolved_stock_key,
            window_days=safe_window_days,
            evidence_limit=safe_max_posts,
        )
        company_key, _representative_stock_key, company_stock_keys = (
            _company_identity_from_pack(computed.pack)
        )
        raw_computed_by_resolved_key[resolved_stock_key] = computed
        company_key_by_resolved_key[resolved_stock_key] = company_key
        company_stock_keys_by_company_key.setdefault(company_key, company_stock_keys)
        requested_by_company_key[company_key].extend(merged_requested_stocks)
        resolved_stock_keys_by_company_key[company_key].append(resolved_stock_key)
    for company_key, merged_requested_stocks in requested_by_company_key.items():
        resolved_stock_keys = _sort_stock_keys(
            resolved_stock_keys_by_company_key.get(company_key, [])
        )
        company_stock_keys = company_stock_keys_by_company_key.get(company_key, [])
        representative_stock_key = (
            company_stock_keys[0]
            if len(resolved_stock_keys) > 1 and company_stock_keys
            else resolved_stock_keys[0]
        )
        for resolved_stock_key in resolved_stock_keys:
            merged_into_stock_key_by_resolved_key[resolved_stock_key] = (
                representative_stock_key
            )
        base_computed = raw_computed_by_resolved_key.get(representative_stock_key)
        if base_computed is None and resolved_stock_keys:
            base_computed = raw_computed_by_resolved_key.get(resolved_stock_keys[0])
        if base_computed is None:
            continue
        computed = _canonicalize_company_pack(
            base_computed,
            requested_stock=merged_requested_stocks[0],
            representative_stock_key=representative_stock_key,
            company_stock_keys=company_stock_keys,
        )
        computed_by_company_key[company_key] = computed
        merged_requested_stocks = requested_by_company_key.get(company_key, [])
        companies.append(
            _portfolio_company_row(
                requested_stocks=merged_requested_stocks,
                computed=computed,
            )
        )
        covered_stock_keys.extend(computed.pack["covered_stock_keys"])
        author_counts.update(computed.author_counts)
    page_title_by_company_key = {
        _clean_text(company_key): _clean_text(computed.pack.get("page_title"))
        for company_key, computed in computed_by_company_key.items()
        if _clean_text(company_key)
    }
    page_title_by_merged_stock_key = {
        _clean_text(computed.pack.get("resolved_stock_key")): _clean_text(
            computed.pack.get("page_title")
        )
        for computed in computed_by_company_key.values()
        if _clean_text(computed.pack.get("resolved_stock_key"))
    }
    normalized_input_rows: list[PortfolioInputRow] = []
    for row in input_rows:
        resolved_stock_key = _clean_text(row.get("resolved_stock_key"))
        company_key = _clean_text(company_key_by_resolved_key.get(resolved_stock_key))
        merged_into_stock_key = _clean_text(
            merged_into_stock_key_by_resolved_key.get(resolved_stock_key)
        )
        page_title = _clean_text(
            page_title_by_company_key.get(company_key)
            or page_title_by_merged_stock_key.get(merged_into_stock_key)
        )
        normalized_input_rows.append(
            {
                "requested_stock": _clean_text(row.get("requested_stock")),
                "resolved_stock_key": resolved_stock_key,
                "page_title": page_title,
                "status": _clean_text(row.get("status")),
                "merged_into_stock_key": (
                    merged_into_stock_key
                    if resolved_stock_key
                    else _clean_text(row.get("merged_into_stock_key"))
                ),
                "error": _clean_text(row.get("error")),
            }
        )
    companies.sort(
        key=lambda row: (
            -float(row.get("controversy_score") or 0.0),
            -int(row.get("signal_total") or 0),
            _clean_text(row.get("page_title")),
        )
    )
    shared_authors, shared_author_total = _build_shared_author_rows(
        computed_by_company_key=computed_by_company_key,
        page_title_by_company_key=page_title_by_company_key,
    )
    candidate_buckets = _build_portfolio_candidate_buckets(companies)
    total_signal_total = sum(int(row.get("signal_total") or 0) for row in companies)
    unique_covered_stock_keys = _dedupe_texts(covered_stock_keys)
    load_error = ""
    if not companies and unresolved_inputs:
        load_error = STOCK_RESOLVE_REQUIRED_ERROR
    return {
        "requested_stocks": requested_stocks,
        "window_days": safe_window_days,
        "evidence_row_limit_per_stock": safe_max_posts,
        "requested_total": len(requested_stocks),
        "resolved_input_total": resolved_total,
        "unresolved_input_total": len(unresolved_inputs),
        "company_total": len(companies),
        "covered_security_total": len(unique_covered_stock_keys),
        "total_signal_total": total_signal_total,
        "portfolio_author_total": len(author_counts),
        "shared_author_total": shared_author_total,
        "covered_stock_keys": unique_covered_stock_keys,
        "inputs": normalized_input_rows,
        "unresolved_inputs": unresolved_inputs,
        "companies": companies,
        "candidate_buckets": candidate_buckets,
        "shared_authors": shared_authors,
        "load_error": load_error,
    }


__all__ = [
    "DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS",
    "DEFAULT_STOCK_EVIDENCE_MAX_POSTS",
    "DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS",
    "MAX_PORTFOLIO_INPUT_STOCKS",
    "MAX_STOCK_EVIDENCE_MAX_POSTS",
    "MAX_STOCK_EVIDENCE_WINDOW_DAYS",
    "PortfolioCandidateBuckets",
    "PortfolioCandidateRow",
    "PortfolioCompanyRow",
    "PortfolioContext",
    "PortfolioInputRow",
    "PortfolioSharedAuthorRow",
    "StockActionCountRow",
    "StockAuthorRow",
    "StockEvidencePack",
    "StockEvidenceRow",
    "StockMatchKindCountRow",
    "StockStanceCountRow",
    "get_portfolio_context",
    "get_stock_evidence_pack",
]
