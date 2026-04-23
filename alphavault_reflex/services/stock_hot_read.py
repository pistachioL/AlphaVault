from __future__ import annotations

from typing import TypedDict

from alphavault.constants import SCHEMA_WEIBO, SCHEMA_XUEQIU
from alphavault.db.postgres_db import (
    PostgresConnection,
    ensure_postgres_engine,
    postgres_connect_autocommit,
    qualify_postgres_table,
    require_postgres_schema_name,
)
from alphavault.db.postgres_env import (
    PostgresSource,
    load_configured_postgres_sources_from_env,
)
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql_rows import read_sql_rows
from alphavault.domains.thread_tree.service import build_post_tree_map
from alphavault.domains.stock.keys import (
    normalize_stock_key as _canonical_stock_key,
    stock_key_lookup_candidates,
    stock_value,
)
from alphavault.env import load_dotenv_if_present
from alphavault.research_stock_cache import (
    load_entity_page_signal_snapshot as _legacy_load_entity_page_signal_snapshot,
)
from alphavault.research_signal_view import (
    coerce_signal_timestamp,
    default_signal_reference_time,
    format_signal_created_at_line,
    merge_post_fields,
)
from alphavault.worker.job_state import (
    load_worker_job_cursor as _legacy_load_worker_job_cursor,
)
from alphavault_reflex.services.source_read import (
    MISSING_POSTGRES_DSN_ERROR,
    load_stock_alias_relations_from_env,
    load_stock_official_names_from_env,
    load_stock_same_company_keys_from_env,
)
from alphavault_reflex.services.source_loader import (
    WANTED_POST_COLUMNS_FOR_TREE,
    standardize_posts_rows,
)
from alphavault_reflex.services.source_read_utils import (
    ensure_platform_post_id_rows,
    normalize_posts_datetime_rows,
)
from alphavault_reflex.services.trade_board_loader import (
    trade_board_created_at_sql_expr,
)

_SOURCE_SCHEMA_NAMES = frozenset((SCHEMA_WEIBO, SCHEMA_XUEQIU))
_DEFAULT_STOCK_POST_WINDOW_DAYS = 0
_DEFAULT_RELATED_FILTER = "all"
_SIGNAL_ONLY_FILTER = "signal"
_RELATED_SECTOR_LIMIT = 100
_RELATED_SECTOR_WINDOW_DAYS = 30


class StockQueryContext(TypedDict):
    requested_stock_key: str
    query_keys: list[str]
    same_company_stocks: list[dict[str, str]]
    official_names: dict[str, str]
    page_title: str


def load_entity_page_signal_snapshot(*args, **kwargs):
    return _legacy_load_entity_page_signal_snapshot(*args, **kwargs)


def load_worker_job_cursor(*args, **kwargs):
    return _legacy_load_worker_job_cursor(*args, **kwargs)


def _load_source_schemas_from_env() -> list[PostgresSource]:
    return [
        source
        for source in load_configured_postgres_sources_from_env()
        if str(getattr(source, "schema", getattr(source, "name", "")) or "").strip()
        in _SOURCE_SCHEMA_NAMES
    ]


def _build_source_engine(db_url: str, *, source_name: str):
    try:
        return ensure_postgres_engine(db_url, schema_name=source_name)
    except TypeError:
        return ensure_postgres_engine(db_url)


def _source_table(conn: PostgresConnection, table_name: str) -> str:
    return qualify_postgres_table(
        require_postgres_schema_name(conn),
        table_name,
    )


def _normalize_stock_key(value: str) -> str:
    return _canonical_stock_key(value)


def _dedupe_stock_keys(keys: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw_key in keys:
        key = str(raw_key or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _is_stock_alias_relation_row(row: dict[str, object]) -> bool:
    relation_type = str(row.get("relation_type") or "").strip()
    relation_label = str(row.get("relation_label") or "").strip()
    return relation_type == "stock_alias" or relation_label == "alias_of"


def _build_alias_relation_graph(
    relations: list[dict[str, object]],
) -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {}
    for row in relations:
        if not _is_stock_alias_relation_row(row):
            continue
        left_key = _normalize_stock_key(str(row.get("left_key") or "").strip())
        right_key = _normalize_stock_key(str(row.get("right_key") or "").strip())
        if not left_key or not right_key:
            continue
        graph.setdefault(left_key, set()).add(right_key)
        graph.setdefault(right_key, set()).add(left_key)
    return graph


def _resolve_alias_component_keys(
    *,
    stock_key: str,
    alias_graph: dict[str, set[str]],
) -> list[str]:
    normalized = _normalize_stock_key(stock_key)
    if not normalized:
        return []
    out: list[str] = []
    seen_query_keys: set[str] = set()
    queue = [normalized]
    seen_nodes = {normalized}
    for candidate in stock_key_lookup_candidates(normalized):
        candidate_text = str(candidate or "").strip()
        if not candidate_text or candidate_text in seen_query_keys:
            continue
        seen_query_keys.add(candidate_text)
        out.append(candidate_text)
    while queue:
        current = queue.pop(0)
        for neighbor in sorted(alias_graph.get(current, set())):
            normalized_neighbor = _normalize_stock_key(neighbor)
            if not normalized_neighbor or normalized_neighbor in seen_nodes:
                continue
            seen_nodes.add(normalized_neighbor)
            for candidate in stock_key_lookup_candidates(normalized_neighbor):
                candidate_text = str(candidate or "").strip()
                if not candidate_text or candidate_text in seen_query_keys:
                    continue
                seen_query_keys.add(candidate_text)
                out.append(candidate_text)
            queue.append(normalized_neighbor)
    return out


def _is_stock_code_key(stock_key: str) -> bool:
    value = stock_value(stock_key)
    if "." not in value:
        return False
    code_part, market = value.rsplit(".", 1)
    return bool(code_part.strip().isdigit() and market.strip())


def _build_same_company_rows(
    *,
    requested_stock_key: str,
    query_keys: list[str],
    official_names: dict[str, str],
) -> list[dict[str, str]]:
    requested = _normalize_stock_key(requested_stock_key)
    code_keys = _dedupe_stock_keys(
        [key for key in query_keys if _is_stock_code_key(key)]
    )
    ordered_keys: list[str] = []
    if requested and requested in code_keys:
        ordered_keys.append(requested)
    ordered_keys.extend(key for key in code_keys if key and key not in ordered_keys)
    return [
        {
            "stock_key": stock_key_text,
            "label": stock_key_text.removeprefix("stock:"),
            "official_name": str(official_names.get(stock_key_text) or "").strip(),
        }
        for stock_key_text in ordered_keys
    ]


def _empty_stock_query_context() -> StockQueryContext:
    return {
        "requested_stock_key": "",
        "query_keys": [],
        "same_company_stocks": [],
        "official_names": {},
        "page_title": "",
    }


def _resolve_stock_query_context(stock_key: str) -> tuple[StockQueryContext, str]:
    normalized = _normalize_stock_key(stock_key)
    if not normalized:
        return _empty_stock_query_context(), ""
    relations, relation_err = load_stock_alias_relations_from_env()
    if relation_err:
        return _empty_stock_query_context(), relation_err
    alias_graph = _build_alias_relation_graph(relations)
    query_keys = _resolve_alias_component_keys(
        stock_key=normalized,
        alias_graph=alias_graph,
    )
    seen_query_keys = set(query_keys)
    queue = _dedupe_stock_keys(
        [
            _normalize_stock_key(item)
            for item in query_keys
            if _normalize_stock_key(item)
        ]
    )
    seen_nodes = set(queue)
    while queue:
        current = queue.pop(0)
        sibling_keys, sibling_err = load_stock_same_company_keys_from_env(current)
        if sibling_err:
            return _empty_stock_query_context(), sibling_err
        for sibling_key in sibling_keys:
            alias_component = _resolve_alias_component_keys(
                stock_key=sibling_key,
                alias_graph=alias_graph,
            )
            for candidate in alias_component:
                if candidate in seen_query_keys:
                    continue
                seen_query_keys.add(candidate)
                query_keys.append(candidate)
                normalized_candidate = _normalize_stock_key(candidate)
                if not normalized_candidate or normalized_candidate in seen_nodes:
                    continue
                seen_nodes.add(normalized_candidate)
                queue.append(normalized_candidate)
    official_names, _official_name_err = load_stock_official_names_from_env(query_keys)
    page_title = next(
        (
            str(official_names.get(key) or "").strip()
            for key in query_keys
            if str(official_names.get(key) or "").strip()
        ),
        normalized.removeprefix("stock:"),
    )
    return {
        "requested_stock_key": normalized,
        "query_keys": query_keys,
        "same_company_stocks": _build_same_company_rows(
            requested_stock_key=normalized,
            query_keys=query_keys,
            official_names=official_names,
        ),
        "official_names": official_names,
        "page_title": page_title,
    }, ""


def _window_cutoff_str(days: int) -> str:
    return f"CURRENT_TIMESTAMP - INTERVAL '{max(1, int(days))} days'"


def _normalize_related_filter(value: object) -> str:
    text = str(value or "").strip().lower()
    if text == _SIGNAL_ONLY_FILTER:
        return _SIGNAL_ONLY_FILTER
    return _DEFAULT_RELATED_FILTER


def _is_signal_only_filter(value: object) -> bool:
    return _normalize_related_filter(value) == _SIGNAL_ONLY_FILTER


def _window_clause(*, created_at_expr: str, signal_window_days: int) -> str:
    if int(signal_window_days or 0) <= 0:
        return ""
    return f"AND {created_at_expr} >= ({_window_cutoff_str(signal_window_days)})"


def _stock_key_clause(
    *,
    stock_keys: list[str],
    prefix: str,
    params: dict[str, object],
) -> str:
    cleaned = [str(key or "").strip() for key in stock_keys if str(key or "").strip()]
    if not cleaned:
        return ""
    placeholders = make_in_placeholders(prefix=prefix, count=len(cleaned))
    params.update(make_in_params(prefix=prefix, values=cleaned))
    return f"ae.entity_key IN ({placeholders})"


def _count_stock_signals(
    conn: PostgresConnection,
    *,
    stock_keys: list[str],
    author: str,
    signal_window_days: int,
    related_filter: str,
) -> int:
    params: dict[str, object] = {}
    key_clause = _stock_key_clause(stock_keys=stock_keys, prefix="k", params=params)
    if not key_clause:
        return 0
    posts_table = _source_table(conn, "posts")
    assertions_table = _source_table(conn, "assertions")
    assertion_entities_table = _source_table(conn, "assertion_entities")
    created_at_expr = trade_board_created_at_sql_expr("p.created_at")
    author_clause = ""
    author_filter = str(author or "").strip()
    if author_filter:
        params["author"] = author_filter
        author_clause = "AND p.author = :author"
    action_clause = (
        "AND a.action LIKE 'trade.%'" if _is_signal_only_filter(related_filter) else ""
    )
    window_clause = _window_clause(
        created_at_expr=created_at_expr,
        signal_window_days=signal_window_days,
    )
    query = f"""
SELECT COUNT(DISTINCT a.post_uid) AS signal_total
FROM {assertions_table} a
JOIN {posts_table} p
  ON p.post_uid = a.post_uid
JOIN {assertion_entities_table} ae
  ON ae.assertion_id = a.assertion_id
WHERE ae.entity_type = 'stock'
  AND {key_clause}
  AND p.processed_at IS NOT NULL
  {action_clause}
  {window_clause}
  {author_clause}
"""
    rows = read_sql_rows(conn, query, params=params)
    if not rows:
        return 0
    try:
        return max(int(rows[0].get("signal_total") or 0), 0)
    except (TypeError, ValueError):
        return 0


def _load_stock_signal_rows(
    conn: PostgresConnection,
    *,
    stock_keys: list[str],
    author: str,
    signal_window_days: int,
    fetch_limit: int,
    related_filter: str,
) -> list[dict[str, str]]:
    params: dict[str, object] = {"limit": max(1, int(fetch_limit))}
    key_clause = _stock_key_clause(stock_keys=stock_keys, prefix="k", params=params)
    if not key_clause:
        return []
    posts_table = _source_table(conn, "posts")
    assertions_table = _source_table(conn, "assertions")
    assertion_entities_table = _source_table(conn, "assertion_entities")
    created_at_expr = trade_board_created_at_sql_expr("p.created_at")
    author_clause = ""
    author_filter = str(author or "").strip()
    if author_filter:
        params["author"] = author_filter
        author_clause = "AND p.author = :author"
    action_clause = (
        "AND a.action LIKE 'trade.%'" if _is_signal_only_filter(related_filter) else ""
    )
    window_clause = _window_clause(
        created_at_expr=created_at_expr,
        signal_window_days=signal_window_days,
    )
    action_select = (
        "a.action"
        if _is_signal_only_filter(related_filter)
        else "CASE WHEN a.action LIKE 'trade.%' THEN a.action ELSE '' END"
    )
    action_strength_select = (
        "CAST(a.action_strength AS TEXT)"
        if _is_signal_only_filter(related_filter)
        else (
            "CASE WHEN a.action LIKE 'trade.%' "
            "THEN CAST(a.action_strength AS TEXT) ELSE '' END"
        )
    )
    order_prefix = (
        "created_at_sort DESC, a.assertion_id ASC"
        if _is_signal_only_filter(related_filter)
        else (
            "CASE WHEN a.action LIKE 'trade.%' THEN 0 ELSE 1 END ASC, "
            "created_at_sort DESC, a.assertion_id ASC"
        )
    )
    query = f"""
WITH matched AS (
    SELECT DISTINCT ON (a.post_uid)
        a.post_uid,
        a.summary,
        {action_select} AS action,
        {action_strength_select} AS action_strength,
        p.author,
        p.created_at,
        {created_at_expr} AS created_at_sort,
        p.url
    FROM {assertions_table} a
    JOIN {posts_table} p
      ON p.post_uid = a.post_uid
    JOIN {assertion_entities_table} ae
      ON ae.assertion_id = a.assertion_id
    WHERE ae.entity_type = 'stock'
      AND {key_clause}
      AND p.processed_at IS NOT NULL
      {action_clause}
      {window_clause}
      {author_clause}
    ORDER BY a.post_uid, {order_prefix}
)
SELECT post_uid, summary, action, action_strength, author, created_at, url
FROM matched
ORDER BY created_at_sort DESC, post_uid DESC
LIMIT :limit
"""
    rows = read_sql_rows(conn, query, params=params)
    reference_now = default_signal_reference_time()
    out: list[dict[str, str]] = []
    for row in rows:
        created_at = str(row.get("created_at") or "").strip()
        out.append(
            {
                "post_uid": str(row.get("post_uid") or "").strip(),
                "summary": str(row.get("summary") or "").strip(),
                "action": str(row.get("action") or "").strip(),
                "action_strength": str(row.get("action_strength") or "").strip(),
                "author": str(row.get("author") or "").strip(),
                "created_at": created_at,
                "created_at_line": format_signal_created_at_line(
                    created_at,
                    now=reference_now,
                ),
                "url": str(row.get("url") or "").strip(),
            }
        )
    return out


def _load_stock_related_sectors(
    conn: PostgresConnection,
    *,
    stock_keys: list[str],
    signal_window_days: int,
) -> list[dict[str, str]]:
    params: dict[str, object] = {"limit": int(_RELATED_SECTOR_LIMIT)}
    key_clause = _stock_key_clause(stock_keys=stock_keys, prefix="k", params=params)
    if not key_clause:
        return []
    posts_table = _source_table(conn, "posts")
    assertions_table = _source_table(conn, "assertions")
    assertion_entities_table = _source_table(conn, "assertion_entities")
    topic_cluster_topics_table = _source_table(conn, "topic_cluster_topics")
    created_at_expr = trade_board_created_at_sql_expr("p.created_at")
    query = f"""
WITH matched_posts AS (
    SELECT DISTINCT a.post_uid
    FROM {assertions_table} a
    JOIN {posts_table} p
      ON p.post_uid = a.post_uid
    JOIN {assertion_entities_table} ae
      ON ae.assertion_id = a.assertion_id
    WHERE a.action LIKE 'trade.%'
      AND ae.entity_type = 'stock'
      AND {key_clause}
      AND p.processed_at IS NOT NULL
      {_window_clause(created_at_expr=created_at_expr, signal_window_days=signal_window_days)}
),
post_sector_pairs AS (
    SELECT DISTINCT mp.post_uid, tct.cluster_key
    FROM matched_posts mp
    JOIN {assertions_table} a
      ON a.post_uid = mp.post_uid
    JOIN {assertion_entities_table} sector_ae
      ON sector_ae.assertion_id = a.assertion_id
    JOIN {topic_cluster_topics_table} tct
      ON tct.topic_key = sector_ae.entity_key
    WHERE a.action LIKE 'trade.%'
      AND sector_ae.entity_type = 'industry'
      AND tct.cluster_key <> ''
)
SELECT cluster_key AS sector_key, COUNT(*) AS mention_count
FROM post_sector_pairs
GROUP BY cluster_key
ORDER BY mention_count DESC, sector_key ASC
LIMIT :limit
"""
    rows = read_sql_rows(conn, query, params=params)
    return [
        {
            "sector_key": str(row.get("sector_key") or "").strip(),
            "mention_count": str(row.get("mention_count") or "").strip(),
        }
        for row in rows
        if str(row.get("sector_key") or "").strip()
    ]


def _load_posts_for_signal_rows(
    conn: PostgresConnection,
    *,
    source_name: str,
    rows: list[dict[str, str]],
) -> list[dict[str, object]]:
    post_uids = tuple(
        dict.fromkeys(
            str(row.get("post_uid") or "").strip()
            for row in rows
            if str(row.get("post_uid") or "").strip()
        )
    )
    if not post_uids:
        return []
    placeholders = ", ".join(["?"] * len(post_uids))
    posts_table = _source_table(conn, "posts")
    query = f"""
SELECT {", ".join(WANTED_POST_COLUMNS_FOR_TREE)}
FROM {posts_table}
WHERE processed_at IS NOT NULL
  AND post_uid IN ({placeholders})
"""
    post_rows = read_sql_rows(conn, query, params=list(post_uids))
    posts = standardize_posts_rows(post_rows, source_name=source_name)
    posts = normalize_posts_datetime_rows(posts)
    return ensure_platform_post_id_rows(posts)


def _enrich_signal_rows_with_tree(
    rows: list[dict[str, str]],
    *,
    posts: list[dict[str, object]],
) -> list[dict[str, str]]:
    if not rows:
        return []
    merged_rows = merge_post_fields(
        [dict(row) for row in rows],
        posts,
    )
    tree_map = build_post_tree_map(
        post_uids=[
            str(row.get("post_uid") or "").strip()
            for row in merged_rows
            if str(row.get("post_uid") or "").strip()
        ],
        posts=posts,
    )
    reference_now = default_signal_reference_time()
    out: list[dict[str, str]] = []
    for row in merged_rows:
        post_uid = str(row.get("post_uid") or "").strip()
        created_at = str(row.get("created_at") or "").strip()
        created_at_line = str(row.get("created_at_line") or "").strip()
        tree_label, tree_text = tree_map.get(post_uid, ("", ""))
        out.append(
            {
                "post_uid": post_uid,
                "summary": str(row.get("summary") or "").strip(),
                "action": str(row.get("action") or "").strip(),
                "action_strength": str(row.get("action_strength") or "").strip(),
                "author": str(row.get("author") or "").strip(),
                "created_at": created_at,
                "created_at_line": created_at_line
                or format_signal_created_at_line(created_at, now=reference_now),
                "url": str(row.get("url") or "").strip(),
                "raw_text": str(row.get("raw_text") or "").strip(),
                "tree_label": str(tree_label or "").strip(),
                "tree_text": str(tree_text or "").strip(),
            }
        )
    return out


def _signal_sort_key(row: dict[str, str]) -> tuple[int, float]:
    ts = coerce_signal_timestamp(row.get("created_at"))
    if ts is None:
        return (1, 0.0)
    return (0, -ts.timestamp())


def _sort_signal_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    if not rows:
        return []
    ranked = sorted(rows, key=_signal_sort_key)
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in ranked:
        post_uid = str(row.get("post_uid") or "").strip()
        if post_uid and post_uid in seen:
            continue
        if post_uid:
            seen.add(post_uid)
        out.append(row)
    return out


def _slice_signals(
    rows: list[dict[str, str]],
    *,
    signal_page: int,
    signal_page_size: int,
) -> tuple[list[dict[str, str]], int, int]:
    total = max(int(len(rows)), 0)
    if total <= 0:
        return [], 0, 1
    page_size = max(int(signal_page_size or 1), 1)
    page = max(int(signal_page or 1), 1)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    start = (page - 1) * page_size
    end = start + page_size
    return rows[start:end], total, page


def _empty_stock_view(
    *,
    entity_key: str,
    requested_stock_key: str,
    page_title: str,
    signal_page_size: int,
    load_error: str = "",
    related_sectors: list[dict[str, str]] | None = None,
    same_company_stocks: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        "entity_key": entity_key,
        "requested_stock_key": requested_stock_key,
        "page_title": page_title,
        "signals": [],
        "signal_total": 0,
        "signal_page": 1,
        "signal_page_size": max(int(signal_page_size or 1), 1),
        "related_sectors": related_sectors or [],
        "same_company_stocks": same_company_stocks or [],
        "load_error": str(load_error or "").strip(),
        "load_warning": "",
        "worker_status_text": "",
        "worker_next_run_at": "",
        "worker_cycle_updated_at": "",
        "worker_running": False,
    }


def _load_source_signal_page(
    source: PostgresSource,
    *,
    stock_keys: list[str],
    author: str,
    signal_page: int,
    signal_page_size: int,
    signal_window_days: int,
    related_filter: str,
) -> tuple[list[dict[str, str]], int]:
    engine = _build_source_engine(source.url, source_name=source.name)
    fetch_limit = max(1, int(signal_page or 1) * max(int(signal_page_size or 1), 1))
    with postgres_connect_autocommit(engine) as conn:
        rows = _load_stock_signal_rows(
            conn,
            stock_keys=stock_keys,
            author=author,
            signal_window_days=signal_window_days,
            fetch_limit=fetch_limit,
            related_filter=related_filter,
        )
        posts = _load_posts_for_signal_rows(
            conn,
            source_name=source.name,
            rows=rows,
        )
        total = _count_stock_signals(
            conn,
            stock_keys=stock_keys,
            author=author,
            signal_window_days=signal_window_days,
            related_filter=related_filter,
        )
    return _enrich_signal_rows_with_tree(rows, posts=posts), total


def load_stock_page_rows_from_env(
    stock_key: str,
    *,
    signal_page: int,
    signal_page_size: int,
    author: str = "",
    signal_window_days: int = _DEFAULT_STOCK_POST_WINDOW_DAYS,
    related_filter: str = _DEFAULT_RELATED_FILTER,
) -> dict[str, object]:
    normalized = _normalize_stock_key(stock_key)
    if not normalized:
        return _empty_stock_view(
            entity_key="",
            requested_stock_key="",
            page_title="",
            signal_page_size=signal_page_size,
        )
    load_dotenv_if_present()
    sources = _load_source_schemas_from_env()
    if not sources:
        return _empty_stock_view(
            entity_key=normalized,
            requested_stock_key=normalized,
            page_title=normalized.removeprefix("stock:"),
            signal_page_size=signal_page_size,
            load_error=MISSING_POSTGRES_DSN_ERROR,
        )
    query_context, relation_err = _resolve_stock_query_context(normalized)
    if relation_err:
        return _empty_stock_view(
            entity_key=normalized,
            requested_stock_key=normalized,
            page_title=normalized.removeprefix("stock:"),
            signal_page_size=signal_page_size,
            load_error=relation_err,
        )
    query_keys = list(query_context["query_keys"])
    normalized_related_filter = _normalize_related_filter(related_filter)
    selected_rows: list[dict[str, str]] = []
    selected_total = 0
    selected_errors: list[str] = []
    for source in sources:
        try:
            rows, total = _load_source_signal_page(
                source,
                stock_keys=query_keys,
                author=str(author or "").strip(),
                signal_page=signal_page,
                signal_page_size=signal_page_size,
                signal_window_days=signal_window_days,
                related_filter=normalized_related_filter,
            )
        except BaseException as err:
            selected_errors.append(
                f"postgres_connect_error:{source.name}:{type(err).__name__}"
            )
            continue
        selected_rows.extend(rows)
        selected_total += max(int(total), 0)
    selected_rows = _sort_signal_rows(selected_rows)
    signal_slice, signal_total_from_rows, safe_page = _slice_signals(
        selected_rows,
        signal_page=signal_page,
        signal_page_size=signal_page_size,
    )
    return {
        "entity_key": normalized,
        "requested_stock_key": query_context["requested_stock_key"] or normalized,
        "page_title": query_context["page_title"] or normalized.removeprefix("stock:"),
        "signals": signal_slice,
        "signal_total": max(int(selected_total), int(signal_total_from_rows)),
        "signal_page": safe_page,
        "signal_page_size": max(int(signal_page_size or 1), 1),
        "related_sectors": [],
        "same_company_stocks": query_context["same_company_stocks"],
        "load_error": ""
        if signal_slice or selected_total > 0
        else (selected_errors[0] if selected_errors else ""),
        "load_warning": "",
        "worker_status_text": "",
        "worker_next_run_at": "",
        "worker_cycle_updated_at": "",
        "worker_running": False,
    }


def load_stock_cached_view_from_env(
    stock_key: str,
    *,
    signal_page: int,
    signal_page_size: int,
    author: str = "",
    related_filter: str = _DEFAULT_RELATED_FILTER,
) -> dict[str, object]:
    normalized = _normalize_stock_key(stock_key)
    if not normalized:
        return _empty_stock_view(
            entity_key="",
            requested_stock_key="",
            page_title="",
            signal_page_size=signal_page_size,
        )
    return load_stock_page_rows_from_env(
        normalized,
        signal_page=signal_page,
        signal_page_size=signal_page_size,
        author=str(author or "").strip(),
        related_filter=related_filter,
    )


def load_stock_sidebar_cached_view(stock_slug: str) -> dict[str, object]:
    normalized = _normalize_stock_key(stock_slug)
    if not normalized:
        return {"related_sectors": [], "load_error": ""}
    load_dotenv_if_present()
    sources = _load_source_schemas_from_env()
    if not sources:
        return {
            "related_sectors": [],
            "load_error": MISSING_POSTGRES_DSN_ERROR,
        }
    query_context, relation_err = _resolve_stock_query_context(normalized)
    if relation_err:
        return {"related_sectors": [], "load_error": relation_err}
    selected_rows: list[dict[str, str]] = []
    selected_errors: list[str] = []
    for source in sources:
        try:
            engine = _build_source_engine(source.url, source_name=source.name)
            with postgres_connect_autocommit(engine) as conn:
                selected_rows.extend(
                    _load_stock_related_sectors(
                        conn,
                        stock_keys=list(query_context["query_keys"]),
                        signal_window_days=_RELATED_SECTOR_WINDOW_DAYS,
                    )
                )
        except BaseException as err:
            selected_errors.append(
                f"postgres_connect_error:{source.name}:{type(err).__name__}"
            )
            continue
    if selected_rows:
        counts: dict[str, int] = {}
        for row in selected_rows:
            sector_key = str(row.get("sector_key") or "").strip()
            if not sector_key:
                continue
            try:
                mention_count = int(str(row.get("mention_count") or "0") or 0)
            except ValueError:
                mention_count = 0
            counts[sector_key] = int(counts.get(sector_key, 0)) + max(mention_count, 0)
        ranked = sorted(counts.items(), key=lambda item: (-int(item[1]), str(item[0])))
        selected_rows = [
            {"sector_key": sector_key, "mention_count": str(count)}
            for sector_key, count in ranked
        ]
    return {
        "related_sectors": selected_rows,
        "load_error": ""
        if selected_rows
        else (selected_errors[0] if selected_errors else ""),
    }


def clear_stock_hot_read_caches() -> None:
    return None


__all__ = [
    "clear_stock_hot_read_caches",
    "load_stock_cached_view_from_env",
    "load_stock_page_rows_from_env",
    "load_stock_sidebar_cached_view",
]
