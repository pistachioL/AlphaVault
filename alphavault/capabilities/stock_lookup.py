from __future__ import annotations

from typing import TypedDict
import unicodedata

from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.domains.stock.keys import normalize_stock_key, stock_value
from alphavault.domains.stock.names import normalize_stock_official_name_norm
from alphavault.domains.stock.view_scope import (
    DEFAULT_STOCK_VIEW_SCOPE,
    STOCK_VIEW_SCOPE_COMPANY,
    normalize_stock_view_scope,
)
from alphavault.research_workbench.schema import (
    RESEARCH_RELATIONS_TABLE,
    RESEARCH_SECURITY_MASTER_TABLE,
)
from alphavault.research_workbench.service import get_research_workbench_engine_from_env
from alphavault.search_text import (
    build_exact_search_variants,
    normalize_compact_search_text,
    to_simplified_text,
    to_traditional_text,
)

_RELATION_TYPE_STOCK_ALIAS = "stock_alias"
_RELATION_LABEL_ALIAS = "alias_of"
DEFAULT_STOCK_RESULT_LIMIT = 8
STOCK_RESOLVE_REQUIRED_ERROR = (
    "当前输入还不能稳定映射到唯一股票，请先调用 ai_resolve_stock。"
)


class StockLookupRow(TypedDict):
    stock_key: str
    label: str
    subtitle: str
    match_reason: str
    is_exact: str


class StockLookupResult(TypedDict):
    resolved_stock_key: str
    rows: list[StockLookupRow]
    error: str


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_row_stock_key(row: dict[str, object]) -> str:
    return normalize_stock_key(_clean_text(row.get("stock_key")))


def _display_label(*, stock_key: str, official_name: str) -> str:
    official_name_text = _clean_text(official_name)
    stock_code = stock_value(stock_key)
    if official_name_text and stock_code and official_name_text != stock_code:
        return f"{official_name_text} ({stock_code})"
    return official_name_text or stock_code


def _display_subtitle(*, stock_key: str, official_name: str) -> str:
    official_name_text = _clean_text(official_name)
    stock_code = stock_value(stock_key)
    if official_name_text and stock_code and official_name_text != stock_code:
        return stock_code
    return ""


def _exact_alias_variants(query: str) -> list[str]:
    clean_query = _clean_text(query)
    variants = [
        clean_query,
        unicodedata.normalize("NFKC", to_simplified_text(clean_query)).strip(),
        unicodedata.normalize("NFKC", to_traditional_text(clean_query)).strip(),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        if not variant or variant in seen:
            continue
        seen.add(variant)
        out.append(variant)
    return out


def _normalized_code_candidates(query: str) -> tuple[str, ...]:
    candidates: list[str] = []
    normalized_stock = normalize_stock_key(query)
    if normalized_stock:
        stock_code = stock_value(normalized_stock)
        if "." in stock_code:
            code, _market = stock_code.rsplit(".", 1)
            if code:
                candidates.append(code.upper())
    raw_code = _clean_text(query).upper()
    if raw_code and raw_code not in candidates:
        candidates.append(raw_code)
    return tuple(candidates)


def _official_name_norm_variants(query: str) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for variant in build_exact_search_variants(query):
        compact = variant.replace(" ", "")
        official_name_norm = normalize_stock_official_name_norm(compact)
        if not official_name_norm or official_name_norm in seen:
            continue
        seen.add(official_name_norm)
        out.append(official_name_norm)
    return tuple(out)


def _rows_to_lookup_rows(
    rows: list[dict[str, object]],
    *,
    exact_keys: set[str],
) -> list[StockLookupRow]:
    out: list[StockLookupRow] = []
    seen: set[str] = set()
    for row in rows:
        stock_key = _normalize_row_stock_key(row)
        if not stock_key or stock_key in seen:
            continue
        seen.add(stock_key)
        official_name = _clean_text(row.get("official_name"))
        out.append(
            {
                "stock_key": stock_key,
                "label": _display_label(
                    stock_key=stock_key,
                    official_name=official_name,
                ),
                "subtitle": _display_subtitle(
                    stock_key=stock_key,
                    official_name=official_name,
                ),
                "match_reason": _clean_text(row.get("match_reason")) or "股票命中",
                "is_exact": "1" if stock_key in exact_keys else "",
            }
        )
    return out


def _load_exact_stock_rows(query: str) -> list[dict[str, object]]:
    engine = get_research_workbench_engine_from_env()
    normalized_stock_key = normalize_stock_key(query)
    code_candidates = _normalized_code_candidates(query)
    official_name_norms = _official_name_norm_variants(query)
    alias_keys = tuple(f"stock:{variant}" for variant in _exact_alias_variants(query))

    params: dict[str, object] = {}
    exact_clauses: list[str] = []
    if normalized_stock_key:
        params["stock_key"] = normalized_stock_key
        exact_clauses.append("sm.stock_key = :stock_key")
    if code_candidates:
        exact_clauses.append(
            "UPPER(sm.code) IN ({placeholders})".format(
                placeholders=make_in_placeholders(
                    prefix="code",
                    count=len(code_candidates),
                )
            )
        )
        params.update(make_in_params(prefix="code", values=code_candidates))
    if official_name_norms:
        exact_clauses.append(
            "sm.official_name_norm IN ({placeholders})".format(
                placeholders=make_in_placeholders(
                    prefix="name",
                    count=len(official_name_norms),
                )
            )
        )
        params.update(make_in_params(prefix="name", values=official_name_norms))

    exact_sql = ""
    if exact_clauses:
        exact_sql = f"""
SELECT
    sm.stock_key,
    sm.official_name,
    '正式名称或代码精确命中' AS match_reason
FROM {RESEARCH_SECURITY_MASTER_TABLE} sm
WHERE {" OR ".join(exact_clauses)}
"""

    alias_sql = ""
    if alias_keys:
        alias_sql = """
SELECT
    r.left_key AS stock_key,
    COALESCE(sm.official_name, '') AS official_name,
    '已确认简称精确命中' AS match_reason
FROM {relations_table} r
LEFT JOIN {security_master_table} sm
  ON sm.stock_key = r.left_key
WHERE r.relation_type = '{relation_type}'
  AND r.relation_label = '{relation_label}'
  AND r.right_key IN ({placeholders})
""".format(
            relations_table=RESEARCH_RELATIONS_TABLE,
            security_master_table=RESEARCH_SECURITY_MASTER_TABLE,
            relation_type=_RELATION_TYPE_STOCK_ALIAS,
            relation_label=_RELATION_LABEL_ALIAS,
            placeholders=make_in_placeholders(
                prefix="alias",
                count=len(alias_keys),
            ),
        )
        params.update(make_in_params(prefix="alias", values=alias_keys))

    union_parts = [part.strip() for part in (exact_sql, alias_sql) if part.strip()]
    if not union_parts:
        return []
    sql = """
WITH exact_rows AS (
{union_sql}
)
SELECT DISTINCT stock_key, official_name, match_reason
FROM exact_rows
ORDER BY stock_key ASC
""".format(
        union_sql="\nUNION ALL\n".join(union_parts),
    )

    from alphavault.db.postgres_db import postgres_connect_autocommit
    from alphavault.db.sql_rows import read_sql_rows

    with postgres_connect_autocommit(engine) as conn:
        return read_sql_rows(conn, sql, params=params)


def _load_fuzzy_stock_rows(query: str, *, limit: int) -> list[dict[str, object]]:
    engine = get_research_workbench_engine_from_env()
    compact_query = normalize_compact_search_text(query)
    if not compact_query:
        return []
    code_like = f"%{_clean_text(query).upper()}%"
    name_like_variants = tuple(
        norm for norm in _official_name_norm_variants(query) if norm
    )
    clauses: list[str] = []
    params: dict[str, object] = {"limit": max(1, int(limit))}
    if code_like != "%%":
        params["code_like"] = code_like
        clauses.append("UPPER(code) LIKE :code_like")
    if name_like_variants:
        name_or_clauses: list[str] = []
        for idx, norm in enumerate(name_like_variants):
            key = f"name_like_{idx}"
            params[key] = f"%{norm}%"
            name_or_clauses.append(f"official_name_norm LIKE :{key}")
        clauses.append("(" + " OR ".join(name_or_clauses) + ")")
    if not clauses:
        return []
    sql = """
SELECT stock_key, official_name
FROM {security_master_table}
WHERE {where_clause}
ORDER BY
    CASE
        WHEN official_name_norm = :compact_query THEN 0
        WHEN official_name_norm LIKE :compact_prefix THEN 1
        WHEN UPPER(code) = :code_exact THEN 2
        WHEN UPPER(code) LIKE :code_prefix THEN 3
        ELSE 4
    END,
    stock_key ASC
LIMIT :limit
""".format(
        security_master_table=RESEARCH_SECURITY_MASTER_TABLE,
        where_clause=" OR ".join(clauses),
    )
    params["compact_query"] = compact_query
    params["compact_prefix"] = f"{compact_query}%"
    params["code_exact"] = _clean_text(query).upper()
    params["code_prefix"] = f"{_clean_text(query).upper()}%"

    from alphavault.db.postgres_db import postgres_connect_autocommit
    from alphavault.db.sql_rows import read_sql_rows

    with postgres_connect_autocommit(engine) as conn:
        return read_sql_rows(conn, sql, params=params)


def _resolve_exact_stock_keys(query: str) -> list[str]:
    rows = _load_exact_stock_rows(query)
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        stock_key = _normalize_row_stock_key(row)
        if not stock_key or stock_key in seen:
            continue
        seen.add(stock_key)
        out.append(stock_key)
    return out


def resolve_exact_stock_key(query: str) -> str:
    stock_keys = _resolve_exact_stock_keys(query)
    if len(stock_keys) == 1:
        return stock_keys[0]
    return ""


def resolve_exact_stock_keys(query: str) -> list[str]:
    return _resolve_exact_stock_keys(query)


def is_code_stock_key(value: str) -> bool:
    normalized_stock_key = normalize_stock_key(value)
    stock_code = stock_value(normalized_stock_key)
    if "." not in stock_code:
        return False
    code, market = stock_code.rsplit(".", 1)
    return bool(code.strip().isdigit() and market.strip())


def _same_company_stock_key_sort_key(stock_key: str) -> tuple[int, str]:
    stock_code = stock_value(stock_key)
    if "." not in stock_code:
        return (99, stock_key)
    _code, market = stock_code.rsplit(".", 1)
    market_rank = {"SH": 0, "SZ": 1, "BJ": 2, "HK": 3}.get(market.strip(), 9)
    return (market_rank, stock_code)


def _resolve_same_company_exact_stock_key(stock: str) -> str:
    exact_stock_keys = resolve_exact_stock_keys(stock)
    if len(exact_stock_keys) <= 1:
        return ""
    from alphavault.research_workbench import get_official_names_by_stock_keys

    engine = get_research_workbench_engine_from_env()
    official_names = get_official_names_by_stock_keys(engine, exact_stock_keys)
    unique_official_names = {
        str(official_names.get(stock_key) or "").strip()
        for stock_key in exact_stock_keys
        if str(official_names.get(stock_key) or "").strip()
    }
    if len(unique_official_names) != 1 or len(official_names) != len(exact_stock_keys):
        return ""
    return sorted(exact_stock_keys, key=_same_company_stock_key_sort_key)[0]


def resolve_requested_stock_key(
    stock: str,
    *,
    view_scope: str = DEFAULT_STOCK_VIEW_SCOPE,
) -> str:
    normalized_stock_key = normalize_stock_key(stock)
    if is_code_stock_key(normalized_stock_key):
        return normalized_stock_key
    exact_stock_key = resolve_exact_stock_key(stock)
    if exact_stock_key:
        return exact_stock_key
    if normalize_stock_view_scope(view_scope) != STOCK_VIEW_SCOPE_COMPANY:
        return ""
    same_company_stock_key = _resolve_same_company_exact_stock_key(stock)
    if same_company_stock_key:
        return same_company_stock_key
    if is_code_stock_key(stock):
        return normalize_stock_key(stock)
    return ""


def resolve_stock(
    query: str,
    *,
    limit: int = DEFAULT_STOCK_RESULT_LIMIT,
) -> StockLookupResult:
    clean_query = _clean_text(query)
    if not clean_query:
        return {"resolved_stock_key": "", "rows": [], "error": ""}
    try:
        exact_rows = _load_exact_stock_rows(clean_query)
        exact_keys = {
            stock_key
            for row in exact_rows
            for stock_key in (_normalize_row_stock_key(row),)
            if stock_key
        }
        fuzzy_rows: list[dict[str, object]] = []
        if len(exact_keys) != 1:
            fuzzy_rows = _load_fuzzy_stock_rows(clean_query, limit=limit)
    except BaseException as exc:
        return {"resolved_stock_key": "", "rows": [], "error": str(exc)}

    resolved_stock_key = next(iter(exact_keys)) if len(exact_keys) == 1 else ""
    merged_rows = _rows_to_lookup_rows(
        [*exact_rows, *fuzzy_rows],
        exact_keys=exact_keys,
    )
    return {
        "resolved_stock_key": resolved_stock_key,
        "rows": merged_rows[: max(1, int(limit))],
        "error": "",
    }


__all__ = [
    "DEFAULT_STOCK_RESULT_LIMIT",
    "StockLookupResult",
    "StockLookupRow",
    "STOCK_RESOLVE_REQUIRED_ERROR",
    "is_code_stock_key",
    "resolve_exact_stock_key",
    "resolve_exact_stock_keys",
    "resolve_requested_stock_key",
    "resolve_stock",
]
