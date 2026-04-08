from __future__ import annotations

from alphavault.db.sql.research_workbench import upsert_research_relation
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

from .schema import (
    RESEARCH_RELATIONS_TABLE,
)

RELATION_TYPE_STOCK_SECTOR = "stock_sector"
RELATION_TYPE_STOCK_ALIAS = "stock_alias"
RELATION_LABEL_ALIAS = "alias_of"


def _now_str() -> str:
    return now_cst_str()


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
    conn.execute(
        upsert_research_relation(RESEARCH_RELATIONS_TABLE),
        {
            "relation_id": make_relation_id(
                relation_type=relation_type,
                left_key=left_key,
                right_key=right_key,
                relation_label=relation_label,
            ),
            "relation_type": str(relation_type or "").strip(),
            "left_key": str(left_key or "").strip(),
            "right_key": str(right_key or "").strip(),
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


def record_stock_alias_relation(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    stock_key: str,
    alias_key: str,
    source: str,
) -> None:
    def _record(conn: PostgresConnection) -> None:
        record_relation(
            conn,
            relation_type=RELATION_TYPE_STOCK_ALIAS,
            left_key=stock_key,
            right_key=alias_key,
            relation_label=RELATION_LABEL_ALIAS,
            source=source,
        )

    run_postgres_transaction(engine_or_conn, _record)
    try:
        sync_stock_alias_shadow_dict_best_effort(
            stock_key=stock_key,
            alias_key=alias_key,
        )
    except Exception:
        pass


__all__ = [
    "RELATION_LABEL_ALIAS",
    "RELATION_TYPE_STOCK_ALIAS",
    "RELATION_TYPE_STOCK_SECTOR",
    "record_relation",
    "record_stock_alias_relation",
    "record_stock_sector_relation",
]
