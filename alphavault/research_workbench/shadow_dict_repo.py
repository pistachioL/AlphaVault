from __future__ import annotations

from alphavault.db.sql.research_workbench import (
    select_all_security_master,
    select_all_stock_alias_relations,
)
from alphavault.db.turso_db import TursoConnection, TursoEngine
from alphavault.domains.stock.keys import normalize_stock_key
from alphavault.infra.entity_match_redis import (
    replace_stock_dict_shadow_best_effort,
)

from .schema import (
    RESEARCH_RELATIONS_TABLE,
    RESEARCH_SECURITY_MASTER_TABLE,
    ensure_research_workbench_schema,
    handle_turso_error,
    use_conn,
)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_name(value: object) -> str:
    return _clean_text(value).casefold()


def _select_unique_mapping(rows: list[tuple[str, str]]) -> dict[str, str]:
    values_by_text: dict[str, set[str]] = {}
    for raw_text, raw_entity_key in rows:
        text = _clean_text(raw_text)
        entity_key = normalize_stock_key(raw_entity_key)
        if not text or not entity_key.startswith("stock:"):
            continue
        values_by_text.setdefault(text, set()).add(entity_key)
    return {
        text: next(iter(entity_keys))
        for text, entity_keys in values_by_text.items()
        if len(entity_keys) == 1
    }


def rebuild_stock_dict_shadow_best_effort(
    engine_or_conn: TursoEngine | TursoConnection,
) -> bool:
    ensure_research_workbench_schema(engine_or_conn)
    try:
        with use_conn(engine_or_conn) as conn:
            security_master_rows = conn.execute(
                select_all_security_master(RESEARCH_SECURITY_MASTER_TABLE)
            ).fetchall()
            alias_rows = conn.execute(
                select_all_stock_alias_relations(RESEARCH_RELATIONS_TABLE)
            ).fetchall()
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)

    official_name_targets = _select_unique_mapping(
        [
            (_clean_text(row[1]), _clean_text(row[0]))
            for row in security_master_rows
            if row
        ]
    )
    alias_targets = _select_unique_mapping(
        [
            (
                _clean_text(row[1]).removeprefix("stock:"),
                _clean_text(row[0]),
            )
            for row in alias_rows
            if row
        ]
    )
    return replace_stock_dict_shadow_best_effort(
        official_name_targets=official_name_targets,
        alias_targets=alias_targets,
    )


__all__ = ["rebuild_stock_dict_shadow_best_effort"]
