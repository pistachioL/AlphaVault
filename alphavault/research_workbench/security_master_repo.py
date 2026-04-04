from __future__ import annotations

from alphavault.db.sql.research_workbench import (
    select_security_master_by_official_names,
    select_security_master_by_stock_key,
    upsert_security_master_stock as upsert_security_master_stock_sql,
)
from alphavault.db.turso_db import TursoConnection, TursoEngine
from alphavault.domains.stock.key_match import normalize_stock_code
from alphavault.domains.stock.keys import normalize_stock_key
from alphavault.infra.entity_match_redis import (
    sync_stock_name_shadow_dict_best_effort,
)
from alphavault.timeutil import now_cst_str

from .schema import RESEARCH_SECURITY_MASTER_TABLE, handle_turso_error, use_conn


def _now_str() -> str:
    return now_cst_str()


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_name(value: object) -> str:
    return _clean_text(value).casefold()


def _resolve_market(*, stock_key: str, market: str) -> str:
    resolved_market = _clean_text(market).upper()
    if resolved_market:
        return resolved_market
    if "." not in stock_key:
        return ""
    return _clean_text(stock_key.rsplit(".", 1)[1]).upper()


def upsert_security_master_stock(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    market: str,
    code: str,
    official_name: str,
) -> None:
    resolved_stock_key = normalize_stock_key(stock_key)
    resolved_code = normalize_stock_code(code)
    resolved_name = _clean_text(official_name)
    resolved_market = _resolve_market(stock_key=resolved_stock_key, market=market)
    if (
        not resolved_stock_key
        or not resolved_code
        or not resolved_name
        or not resolved_market
    ):
        return
    now = _now_str()
    previous_official_name = ""
    try:
        with use_conn(engine_or_conn) as conn:
            row = conn.execute(
                select_security_master_by_stock_key(RESEARCH_SECURITY_MASTER_TABLE),
                {"stock_key": resolved_stock_key},
            ).fetchone()
            if row:
                previous_official_name = _clean_text(row[0])
            conn.execute(
                upsert_security_master_stock_sql(RESEARCH_SECURITY_MASTER_TABLE),
                {
                    "stock_key": resolved_stock_key,
                    "market": resolved_market,
                    "code": resolved_code,
                    "official_name": resolved_name,
                    "official_name_norm": _normalize_name(resolved_name),
                    "now": now,
                },
            )
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)
    try:
        sync_stock_name_shadow_dict_best_effort(
            stock_key=resolved_stock_key,
            official_name=resolved_name,
            previous_official_name=previous_official_name,
        )
    except Exception:
        pass


def get_stock_keys_by_official_names(
    engine_or_conn: TursoEngine | TursoConnection,
    official_names: list[str],
) -> dict[str, str]:
    cleaned_names = [_clean_text(item) for item in official_names if _clean_text(item)]
    if not cleaned_names:
        return {}
    norms_by_name = {name: _normalize_name(name) for name in cleaned_names}
    unique_norms = list(dict.fromkeys(norms_by_name.values()))
    sql = select_security_master_by_official_names(
        RESEARCH_SECURITY_MASTER_TABLE,
        name_count=len(unique_norms),
    )
    try:
        with use_conn(engine_or_conn) as conn:
            rows = conn.execute(sql, unique_norms).fetchall()
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)
    stock_keys_by_norm: dict[str, set[str]] = {}
    for row in rows:
        if not row:
            continue
        stock_key = normalize_stock_key(row[0])
        official_name_norm = _normalize_name(row[2])
        if not stock_key or not official_name_norm:
            continue
        stock_keys_by_norm.setdefault(official_name_norm, set()).add(stock_key)
    unique_keys_by_norm = {
        norm: next(iter(stock_keys))
        for norm, stock_keys in stock_keys_by_norm.items()
        if len(stock_keys) == 1
    }
    return {
        name: unique_keys_by_norm[norm]
        for name, norm in norms_by_name.items()
        if norm in unique_keys_by_norm
    }


__all__ = [
    "get_stock_keys_by_official_names",
    "upsert_security_master_stock",
]
