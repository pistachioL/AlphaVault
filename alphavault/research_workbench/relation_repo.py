from __future__ import annotations

from alphavault.db.sql.research_workbench import (
    upsert_research_object,
    upsert_research_relation,
)
from alphavault.db.turso_db import TursoConnection, TursoEngine
from alphavault.db.turso_db import turso_savepoint
from alphavault.timeutil import now_cst_str
from alphavault.domains.relation.ids import make_relation_id

from .schema import (
    RESEARCH_OBJECTS_TABLE,
    RESEARCH_RELATIONS_TABLE,
    handle_turso_error,
    use_conn,
)

RELATION_TYPE_STOCK_SECTOR = "stock_sector"
RELATION_TYPE_STOCK_ALIAS = "stock_alias"
RELATION_LABEL_ALIAS = "alias_of"


def _now_str() -> str:
    return now_cst_str()


def _display_name_from_key(object_key: str) -> str:
    value = str(object_key or "").strip()
    if ":" not in value:
        return value
    return value.split(":", 1)[1].strip()


def _object_type_from_key(object_key: str) -> str:
    value = str(object_key or "").strip()
    if value.startswith("stock:"):
        return "stock"
    if value.startswith("cluster:"):
        return "sector"
    return "unknown"


def _upsert_object(conn: TursoConnection, object_key: str) -> None:
    key = str(object_key or "").strip()
    if not key:
        return
    now = _now_str()
    conn.execute(
        upsert_research_object(RESEARCH_OBJECTS_TABLE),
        {
            "object_key": key,
            "object_type": _object_type_from_key(key),
            "display_name": _display_name_from_key(key),
            "now": now,
        },
    )


def record_relation(
    conn: TursoConnection,
    *,
    relation_type: str,
    left_key: str,
    right_key: str,
    relation_label: str,
    source: str,
) -> None:
    now = _now_str()
    _upsert_object(conn, left_key)
    _upsert_object(conn, right_key)
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
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    sector_key: str,
    source: str,
) -> None:
    try:
        with use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                record_relation(
                    conn,
                    relation_type=RELATION_TYPE_STOCK_SECTOR,
                    left_key=stock_key,
                    right_key=sector_key,
                    relation_label="member_of",
                    source=source,
                )
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)


def record_stock_alias_relation(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    alias_key: str,
    source: str,
) -> None:
    try:
        with use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                record_relation(
                    conn,
                    relation_type=RELATION_TYPE_STOCK_ALIAS,
                    left_key=stock_key,
                    right_key=alias_key,
                    relation_label=RELATION_LABEL_ALIAS,
                    source=source,
                )
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)


__all__ = [
    "RELATION_LABEL_ALIAS",
    "RELATION_TYPE_STOCK_ALIAS",
    "RELATION_TYPE_STOCK_SECTOR",
    "record_relation",
    "record_stock_alias_relation",
    "record_stock_sector_relation",
]
