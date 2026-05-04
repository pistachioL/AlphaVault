from __future__ import annotations

from alphavault.domains.stock.service import unique_sector_keys, unique_stock_keys

SECTION_STOCK_ALIAS = "stock_alias"
SECTION_STOCK_SECTOR = "stock_sector"
SECTION_SECTOR_SECTOR = "sector_sector"


def build_section_candidates(
    assertions: list[dict[str, object]],
    *,
    section: str,
    limit: int,
    candidate_builders_module,
    stock_object_index_module,
) -> list[dict[str, str]]:
    if section == SECTION_STOCK_ALIAS:
        return _stock_candidates_for_relation_type(
            assertions,
            relation_type=SECTION_STOCK_ALIAS,
            limit=limit,
            candidate_builders_module=candidate_builders_module,
            stock_object_index_module=stock_object_index_module,
        )
    if section == SECTION_STOCK_SECTOR:
        return _stock_candidates_for_relation_type(
            assertions,
            relation_type=SECTION_STOCK_SECTOR,
            limit=limit,
            candidate_builders_module=candidate_builders_module,
            stock_object_index_module=stock_object_index_module,
        )
    return _sector_relation_candidates(
        assertions,
        limit=limit,
        candidate_builders_module=candidate_builders_module,
    )


def _stock_candidates_for_relation_type(
    assertions: list[dict[str, object]],
    *,
    relation_type: str,
    limit: int,
    candidate_builders_module,
    stock_object_index_module,
) -> list[dict[str, str]]:
    stock_index = stock_object_index_module.build_stock_object_index(assertions)
    out: list[dict[str, str]] = []
    for stock_key in unique_stock_keys(assertions):
        out.extend(
            candidate_builders_module.build_stock_pending_candidates(
                assertions,
                stock_key=stock_key,
                ai_enabled=False,
                relation_type=relation_type,
                stock_index=stock_index,
            )
        )
        if len(out) >= int(limit):
            break
    return out[: int(limit)]


def _sector_relation_candidates(
    assertions: list[dict[str, object]],
    *,
    limit: int,
    candidate_builders_module,
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for sector_key in unique_sector_keys(assertions):
        out.extend(
            candidate_builders_module.build_sector_pending_candidates(
                assertions,
                sector_key=sector_key,
                ai_enabled=False,
            )
        )
        if len(out) >= int(limit):
            break
    return out[: int(limit)]


__all__ = ["build_section_candidates"]
