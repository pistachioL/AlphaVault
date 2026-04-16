from __future__ import annotations

from collections import Counter, defaultdict

from alphavault.domains.stock.keys import STOCK_KEY_PREFIX
from alphavault.domains.stock.object_index import (
    StockObjectIndex,
    build_stock_object_index,
    filter_assertions_for_stock_object,
)


RELATION_LABEL_RELATED = "related"
RELATION_LABEL_PARENT_CHILD = "parent_child"


def build_stock_alias_candidates(
    assertions: list[dict[str, object]],
    *,
    stock_key: str,
    stock_index: StockObjectIndex | None = None,
) -> list[dict[str, str]]:
    target = str(stock_key or "").strip()
    if not assertions or not target:
        return []

    resolved_stock_index = stock_index or build_stock_object_index(assertions)
    entity_key = resolved_stock_index.resolve(target)
    if not entity_key:
        return []
    stock_view = filter_assertions_for_stock_object(
        assertions,
        stock_key=entity_key,
        stock_index=resolved_stock_index,
    )
    if not stock_view:
        return []
    member_keys = resolved_stock_index.member_keys_by_object_key.get(entity_key, set())

    alias_scores: Counter[str] = Counter()
    for member_key in member_keys:
        alias_key = str(member_key or "").strip()
        if not alias_key or alias_key == entity_key:
            continue
        topic_hits = sum(
            1
            for row in stock_view
            if str(row.get("entity_key") or "").strip() == alias_key
        )
        if topic_hits:
            alias_scores[alias_key] += int(topic_hits)

    for row in stock_view:
        for name in _coerce_list(row.get("stock_names")):
            alias_key = f"{STOCK_KEY_PREFIX}{name}"
            if alias_key != entity_key:
                alias_scores[alias_key] += 1

    return [
        {
            "candidate_key": alias_key,
            "alias_key": alias_key,
            "score": str(score),
            "evidence_summary": f"同票名称共现 {score} 次",
            "reason_code": "stock_alias_overlap",
        }
        for alias_key, score in alias_scores.most_common()
    ]


def build_stock_sector_candidates(
    assertions: list[dict[str, object]],
    *,
    stock_key: str,
    stock_index: StockObjectIndex | None = None,
) -> list[dict[str, str]]:
    target = str(stock_key or "").strip()
    if not assertions or not target:
        return []

    resolved_stock_index = stock_index or build_stock_object_index(assertions)
    entity_key = resolved_stock_index.resolve(target)
    if not entity_key:
        return []
    stock_view = filter_assertions_for_stock_object(
        assertions,
        stock_key=entity_key,
        stock_index=resolved_stock_index,
    )
    if not stock_view:
        return []

    sector_scores: Counter[str] = Counter()
    for row in stock_view:
        for sector_key in _row_sector_keys(row):
            sector_scores[sector_key] += 1

    ranked = sorted(sector_scores.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {
            "candidate_key": sector_key,
            "sector_key": sector_key,
            "score": str(score),
            "evidence_summary": f"该个股与板块共现 {score} 次",
            "reason_code": "stock_sector_overlap",
        }
        for sector_key, score in ranked
    ]


def build_sector_relation_candidates(
    assertions: list[dict[str, object]],
    *,
    sector_key: str,
) -> list[dict[str, str]]:
    target = str(sector_key or "").strip()
    if not assertions or not target:
        return []

    stocks_by_sector: dict[str, set[str]] = defaultdict(set)
    for row in assertions:
        entity_key = str(row.get("entity_key") or "").strip()
        if not entity_key.startswith(STOCK_KEY_PREFIX):
            continue
        for member_sector in _row_sector_keys(row):
            stocks_by_sector[member_sector].add(entity_key)

    base_stocks = stocks_by_sector.get(target, set())
    if not base_stocks:
        return []

    out: list[dict[str, str]] = []
    for candidate_key, stock_keys in stocks_by_sector.items():
        if candidate_key == target:
            continue
        overlap = len(base_stocks & stock_keys)
        if overlap <= 0:
            continue
        out.append(
            {
                "candidate_key": candidate_key,
                "sector_key": candidate_key,
                "score": str(overlap),
                "evidence_summary": f"相关个股重合 {overlap} 个",
                "reason_code": "sector_sector_stock_overlap",
            }
        )
    return sorted(out, key=lambda row: (-int(row["score"]), row["sector_key"]))


def classify_sector_relation_label(*, ai_enabled: bool, explanation: str) -> str:
    if not ai_enabled:
        return RELATION_LABEL_RELATED
    text = str(explanation or "").strip().lower()
    if "parent" in text or "child" in text or "上下级" in text:
        return RELATION_LABEL_PARENT_CHILD
    return RELATION_LABEL_RELATED


def _row_sector_keys(row: dict[str, object]) -> list[str]:
    keys = _coerce_list(row.get("cluster_keys"))
    if keys:
        return keys
    raw_key = str(row.get("cluster_key") or "").strip()
    if raw_key:
        return [raw_key]
    return []


def _coerce_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []
