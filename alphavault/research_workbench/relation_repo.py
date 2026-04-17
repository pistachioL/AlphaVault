from __future__ import annotations

from alphavault.db.sql.research_workbench import (
    select_all_security_master,
    upsert_research_relation,
)
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    run_postgres_transaction,
)
from alphavault.infra.entity_match_redis import (
    sync_stock_alias_shadow_dict_best_effort,
)
from alphavault.timeutil import now_cst_str
from alphavault.domains.relation.ids import make_relation_id
from alphavault.domains.stock.keys import normalize_stock_key

from .schema import (
    RESEARCH_RELATIONS_TABLE,
    RESEARCH_SECURITY_MASTER_TABLE,
    handle_db_error,
    use_conn,
)

RELATION_TYPE_STOCK_SECTOR = "stock_sector"
RELATION_TYPE_STOCK_ALIAS = "stock_alias"
RELATION_TYPE_STOCK_SIBLING = "stock_sibling"
RELATION_LABEL_ALIAS = "alias_of"
RELATION_LABEL_SAME_COMPANY = "same_company"
_CN_MARKETS = frozenset(("SH", "SZ", "BJ"))


def _now_str() -> str:
    return now_cst_str()


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_relation_key(value: object) -> str:
    key = _clean_text(value)
    if not key.startswith("stock:"):
        return key
    normalized = normalize_stock_key(key)
    return normalized or key


def _market_text(stock_key: str) -> str:
    text = _clean_text(stock_key)
    if "." not in text:
        return ""
    return _clean_text(text.rsplit(".", 1)[1]).upper()


def _is_cn_market(market: str) -> bool:
    return _clean_text(market).upper() in _CN_MARKETS


def _is_hk_market(market: str) -> bool:
    return _clean_text(market).upper() == "HK"


def record_relation(
    conn: PostgresConnection,
    *,
    relation_type: str,
    left_key: str,
    right_key: str,
    relation_label: str,
    source: str,
) -> None:
    now = _now_str()
    resolved_left_key = _normalize_relation_key(left_key)
    resolved_right_key = _normalize_relation_key(right_key)
    conn.execute(
        upsert_research_relation(RESEARCH_RELATIONS_TABLE),
        {
            "relation_id": make_relation_id(
                relation_type=relation_type,
                left_key=resolved_left_key,
                right_key=resolved_right_key,
                relation_label=relation_label,
            ),
            "relation_type": str(relation_type or "").strip(),
            "left_key": resolved_left_key,
            "right_key": resolved_right_key,
            "relation_label": str(relation_label or "").strip(),
            "source": str(source or "").strip(),
            "now": now,
        },
    )


def record_stock_sector_relation(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    stock_key: str,
    sector_key: str,
    source: str,
) -> None:
    def _record(conn: PostgresConnection) -> None:
        record_relation(
            conn,
            relation_type=RELATION_TYPE_STOCK_SECTOR,
            left_key=stock_key,
            right_key=sector_key,
            relation_label="member_of",
            source=source,
        )

    run_postgres_transaction(engine_or_conn, _record)


def record_stock_sibling_relation(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    left_stock_key: str,
    right_stock_key: str,
    source: str,
) -> None:
    normalized_left = normalize_stock_key(left_stock_key)
    normalized_right = normalize_stock_key(right_stock_key)
    if not normalized_left or not normalized_right:
        return
    if normalized_left == normalized_right:
        return

    def _record(conn: PostgresConnection) -> None:
        record_relation(
            conn,
            relation_type=RELATION_TYPE_STOCK_SIBLING,
            left_key=normalized_left,
            right_key=normalized_right,
            relation_label=RELATION_LABEL_SAME_COMPANY,
            source=source,
        )
        record_relation(
            conn,
            relation_type=RELATION_TYPE_STOCK_SIBLING,
            left_key=normalized_right,
            right_key=normalized_left,
            relation_label=RELATION_LABEL_SAME_COMPANY,
            source=source,
        )

    run_postgres_transaction(engine_or_conn, _record)


def list_stock_sibling_keys(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    stock_key: str,
) -> list[str]:
    normalized_key = normalize_stock_key(stock_key)
    if not normalized_key:
        return []
    sql = f"""
SELECT right_key
FROM {RESEARCH_RELATIONS_TABLE}
WHERE relation_type = :relation_type
  AND relation_label = :relation_label
  AND left_key = :left_key
ORDER BY right_key ASC
"""
    try:
        with use_conn(engine_or_conn) as conn:
            rows = conn.execute(
                sql,
                {
                    "relation_type": RELATION_TYPE_STOCK_SIBLING,
                    "relation_label": RELATION_LABEL_SAME_COMPANY,
                    "left_key": normalized_key,
                },
            ).fetchall()
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not row:
            continue
        sibling_key = normalize_stock_key(row[0])
        if not sibling_key or sibling_key == normalized_key or sibling_key in seen:
            continue
        seen.add(sibling_key)
        out.append(sibling_key)
    return out


def sync_stock_sibling_relations_from_security_master(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    source: str,
) -> int:
    try:
        with use_conn(engine_or_conn) as conn:
            rows = conn.execute(
                select_all_security_master(RESEARCH_SECURITY_MASTER_TABLE)
            ).fetchall()
    except BaseException as err:
        handle_db_error(engine_or_conn, err)

    by_name: dict[str, dict[str, set[str]]] = {}
    for row in rows:
        if not row:
            continue
        stock_key = normalize_stock_key(row[0])
        official_name_norm = _clean_text(row[2]).casefold()
        market = _market_text(stock_key)
        if not stock_key or not official_name_norm or not market:
            continue
        bucket = by_name.setdefault(
            official_name_norm,
            {"cn": set(), "hk": set()},
        )
        if _is_cn_market(market):
            bucket["cn"].add(stock_key)
        elif _is_hk_market(market):
            bucket["hk"].add(stock_key)

    pair_rows: list[tuple[str, str]] = []
    for bucket in by_name.values():
        for cn_key in sorted(bucket["cn"]):
            for hk_key in sorted(bucket["hk"]):
                if cn_key == hk_key:
                    continue
                pair_rows.append((cn_key, hk_key))

    for left_key, right_key in pair_rows:
        record_stock_sibling_relation(
            engine_or_conn,
            left_stock_key=left_key,
            right_stock_key=right_key,
            source=source,
        )
    return int(len(pair_rows) * 2)


def record_stock_alias_relation(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    stock_key: str,
    alias_key: str,
    source: str,
) -> None:
    normalized_stock_key = _normalize_relation_key(stock_key)
    normalized_alias_key = _normalize_relation_key(alias_key)

    def _record(conn: PostgresConnection) -> None:
        record_relation(
            conn,
            relation_type=RELATION_TYPE_STOCK_ALIAS,
            left_key=normalized_stock_key,
            right_key=normalized_alias_key,
            relation_label=RELATION_LABEL_ALIAS,
            source=source,
        )

    run_postgres_transaction(engine_or_conn, _record)
    try:
        sync_stock_alias_shadow_dict_best_effort(
            stock_key=normalized_stock_key,
            alias_key=normalized_alias_key,
        )
    except Exception:
        pass


__all__ = [
    "RELATION_LABEL_ALIAS",
    "RELATION_LABEL_SAME_COMPANY",
    "RELATION_TYPE_STOCK_ALIAS",
    "RELATION_TYPE_STOCK_SIBLING",
    "RELATION_TYPE_STOCK_SECTOR",
    "list_stock_sibling_keys",
    "record_relation",
    "record_stock_alias_relation",
    "record_stock_sibling_relation",
    "record_stock_sector_relation",
    "sync_stock_sibling_relations_from_security_master",
]
