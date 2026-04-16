from __future__ import annotations

from alphavault.db.sql.research_workbench import (
    select_security_master_by_official_names,
    select_security_master_by_stock_key,
    select_security_master_by_stock_keys,
    upsert_security_master_stock as upsert_security_master_stock_sql,
)
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    run_postgres_transaction,
)
from alphavault.domains.stock.keys import normalize_stock_key, stock_value
from alphavault.domains.stock.names import (
    normalize_stock_official_name,
    normalize_stock_official_name_norm,
)
from alphavault.infra.entity_match_redis import (
    sync_stock_name_shadow_dict_best_effort,
)
from alphavault.timeutil import now_cst_str

from .schema import RESEARCH_SECURITY_MASTER_TABLE, handle_db_error, use_conn


def _now_str() -> str:
    return now_cst_str()


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_name(value: object) -> str:
    return normalize_stock_official_name_norm(value)


def _resolve_market(*, stock_key: str, market: str) -> str:
    resolved_market = _clean_text(market).upper()
    if resolved_market:
        return resolved_market
    if "." not in stock_key:
        return ""
    return _clean_text(stock_key.rsplit(".", 1)[1]).upper()


def _resolve_code(*, stock_key: str, code: str) -> str:
    stock_text = stock_value(stock_key)
    if "." in stock_text:
        code_part, _market = stock_text.rsplit(".", 1)
        code_text = _clean_text(code_part).upper()
        if code_text:
            return code_text
    return _clean_text(code).upper()


def _build_security_master_upsert_payload(
    *,
    stock_key: str,
    market: str,
    code: str,
    official_name: str,
    now: str,
) -> dict[str, str] | None:
    resolved_stock_key = normalize_stock_key(stock_key)
    resolved_market = _resolve_market(stock_key=resolved_stock_key, market=market)
    resolved_code = _resolve_code(stock_key=resolved_stock_key, code=code)
    resolved_name = normalize_stock_official_name(official_name)
    if (
        not resolved_stock_key
        or not resolved_code
        or not resolved_name
        or not resolved_market
    ):
        return None
    return {
        "stock_key": resolved_stock_key,
        "market": resolved_market,
        "code": resolved_code,
        "official_name": resolved_name,
        "official_name_norm": _normalize_name(resolved_name),
        "now": now,
    }


def upsert_security_master_stock(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    stock_key: str,
    market: str,
    code: str,
    official_name: str,
) -> None:
    payload = _build_security_master_upsert_payload(
        stock_key=stock_key,
        market=market,
        code=code,
        official_name=official_name,
        now=_now_str(),
    )
    if payload is None:
        return
    resolved_stock_key = payload["stock_key"]
    resolved_name = payload["official_name"]
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
                payload,
            )
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    try:
        sync_stock_name_shadow_dict_best_effort(
            stock_key=resolved_stock_key,
            official_name=resolved_name,
            previous_official_name=previous_official_name,
        )
    except Exception:
        pass


def bulk_upsert_security_master_stocks(
    engine_or_conn: PostgresEngine | PostgresConnection,
    rows: list[dict[str, str]],
) -> int:
    if not rows:
        return 0
    now = _now_str()
    payloads: list[dict[str, str]] = []
    for row in rows:
        payload = _build_security_master_upsert_payload(
            stock_key=str(row.get("stock_key") or ""),
            market=str(row.get("market") or ""),
            code=str(row.get("code") or ""),
            official_name=str(row.get("official_name") or ""),
            now=now,
        )
        if payload is None:
            continue
        payloads.append(payload)
    if not payloads:
        return 0
    try:

        def _upsert_many(conn: PostgresConnection) -> None:
            conn.execute(
                upsert_security_master_stock_sql(RESEARCH_SECURITY_MASTER_TABLE),
                payloads,
            )

        run_postgres_transaction(engine_or_conn, _upsert_many)
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    return len(payloads)


def get_stock_keys_by_official_names(
    engine_or_conn: PostgresEngine | PostgresConnection,
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
        handle_db_error(engine_or_conn, err)
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


def get_official_names_by_stock_keys(
    engine_or_conn: PostgresEngine | PostgresConnection,
    stock_keys: list[str],
) -> dict[str, str]:
    cleaned_keys = [
        normalize_stock_key(item) for item in stock_keys if normalize_stock_key(item)
    ]
    if not cleaned_keys:
        return {}
    unique_keys = list(dict.fromkeys(cleaned_keys))
    sql = select_security_master_by_stock_keys(
        RESEARCH_SECURITY_MASTER_TABLE,
        key_count=len(unique_keys),
    )
    try:
        with use_conn(engine_or_conn) as conn:
            rows = conn.execute(sql, unique_keys).fetchall()
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    out: dict[str, str] = {}
    for row in rows:
        if not row:
            continue
        stock_key = normalize_stock_key(row[0])
        official_name = _clean_text(row[1])
        if not stock_key or not official_name:
            continue
        out[stock_key] = official_name
    return out


__all__ = [
    "bulk_upsert_security_master_stocks",
    "get_official_names_by_stock_keys",
    "get_stock_keys_by_official_names",
    "upsert_security_master_stock",
]
