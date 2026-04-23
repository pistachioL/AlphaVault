from __future__ import annotations

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
from alphavault.domains.stock.keys import (
    normalize_stock_key as _canonical_stock_key,
    stock_key_lookup_candidates,
)
from alphavault.env import load_dotenv_if_present
from alphavault.research_stock_cache import (
    load_entity_page_signal_snapshot as _legacy_load_entity_page_signal_snapshot,
)
from alphavault.research_signal_view import (
    coerce_signal_timestamp,
    default_signal_reference_time,
    format_signal_created_at_line,
)
from alphavault.worker.job_state import (
    load_worker_job_cursor as _legacy_load_worker_job_cursor,
)
from alphavault_reflex.services.source_read import (
    MISSING_POSTGRES_DSN_ERROR,
    load_stock_alias_relations_from_env,
)
from alphavault_reflex.services.trade_board_loader import (
    trade_board_created_at_sql_expr,
)

_SOURCE_SCHEMA_NAMES = frozenset((SCHEMA_WEIBO, SCHEMA_XUEQIU))
_DEFAULT_SIGNAL_WINDOW_DAYS = 30
_RELATED_SECTOR_LIMIT = 100


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


def _resolve_stock_key_candidates(stock_key: str) -> tuple[list[str], str]:
    normalized = _normalize_stock_key(stock_key)
    if not normalized:
        return [], ""
    out = stock_key_lookup_candidates(normalized)
    relations, relation_err = load_stock_alias_relations_from_env()
    if relation_err or not relations:
        if relation_err:
            return [], relation_err
        return out, ""
    seen = set(out)
    for row in relations:
        relation_type = str(row.get("relation_type") or "").strip()
        relation_label = str(row.get("relation_label") or "").strip()
        if relation_type != "stock_alias" and relation_label != "alias_of":
            continue
        right_key = str(row.get("right_key") or "").strip()
        left_key = str(row.get("left_key") or "").strip()
        if left_key != normalized or not right_key.startswith("stock:"):
            continue
        if right_key in seen:
            continue
        seen.add(right_key)
        out.append(right_key)
    return out, ""


def _window_cutoff_str(days: int) -> str:
    return f"CURRENT_TIMESTAMP - INTERVAL '{max(1, int(days))} days'"


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
    query = f"""
SELECT COUNT(DISTINCT a.post_uid) AS signal_total
FROM {assertions_table} a
JOIN {posts_table} p
  ON p.post_uid = a.post_uid
JOIN {assertion_entities_table} ae
  ON ae.assertion_id = a.assertion_id
WHERE a.action LIKE 'trade.%'
  AND ae.entity_type = 'stock'
  AND {key_clause}
  AND p.processed_at IS NOT NULL
  AND {created_at_expr} >= ({_window_cutoff_str(signal_window_days)})
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
    query = f"""
WITH matched AS (
    SELECT DISTINCT ON (a.post_uid)
        a.post_uid,
        a.summary,
        a.action,
        a.action_strength,
        p.author,
        p.created_at,
        {created_at_expr} AS created_at_sort,
        p.url
    FROM {assertions_table} a
    JOIN {posts_table} p
      ON p.post_uid = a.post_uid
    JOIN {assertion_entities_table} ae
      ON ae.assertion_id = a.assertion_id
    WHERE a.action LIKE 'trade.%'
      AND ae.entity_type = 'stock'
      AND {key_clause}
      AND p.processed_at IS NOT NULL
      AND {created_at_expr} >= ({_window_cutoff_str(signal_window_days)})
      {author_clause}
    ORDER BY a.post_uid, created_at_sort DESC, a.assertion_id ASC
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
      AND {created_at_expr} >= ({_window_cutoff_str(signal_window_days)})
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


def _signal_sort_key(row: dict[str, str]) -> tuple[int, float]:
    ts = coerce_signal_timestamp(row.get("created_at"))
    if ts is None:
        return (1, 0.0)
    return (0, -ts.timestamp())


def _sort_signal_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    if not rows:
        return []
    return sorted(rows, key=_signal_sort_key)


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
    page_title: str,
    signal_page_size: int,
    load_error: str = "",
    related_sectors: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        "entity_key": entity_key,
        "page_title": page_title,
        "signals": [],
        "signal_total": 0,
        "signal_page": 1,
        "signal_page_size": max(int(signal_page_size or 1), 1),
        "related_sectors": related_sectors or [],
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
        )
        total = _count_stock_signals(
            conn,
            stock_keys=stock_keys,
            author=author,
            signal_window_days=signal_window_days,
        )
    return rows, total


def load_stock_page_rows_from_env(
    stock_key: str,
    *,
    signal_page: int,
    signal_page_size: int,
    author: str = "",
    signal_window_days: int = _DEFAULT_SIGNAL_WINDOW_DAYS,
) -> dict[str, object]:
    normalized = _normalize_stock_key(stock_key)
    if not normalized:
        return _empty_stock_view(
            entity_key="",
            page_title="",
            signal_page_size=signal_page_size,
        )
    load_dotenv_if_present()
    sources = _load_source_schemas_from_env()
    if not sources:
        return _empty_stock_view(
            entity_key=normalized,
            page_title=normalized.removeprefix("stock:"),
            signal_page_size=signal_page_size,
            load_error=MISSING_POSTGRES_DSN_ERROR,
        )
    key_candidates, relation_err = _resolve_stock_key_candidates(normalized)
    if relation_err:
        return _empty_stock_view(
            entity_key=normalized,
            page_title=normalized.removeprefix("stock:"),
            signal_page_size=signal_page_size,
            load_error=relation_err,
        )
    selected_rows: list[dict[str, str]] = []
    selected_total = 0
    page_title = normalized.removeprefix("stock:")
    selected_error = ""
    for candidate in key_candidates:
        candidate_rows: list[dict[str, str]] = []
        candidate_total = 0
        candidate_errors: list[str] = []
        for source in sources:
            try:
                rows, total = _load_source_signal_page(
                    source,
                    stock_keys=[candidate],
                    author=str(author or "").strip(),
                    signal_page=signal_page,
                    signal_page_size=signal_page_size,
                    signal_window_days=signal_window_days,
                )
            except BaseException as err:
                candidate_errors.append(
                    f"postgres_connect_error:{source.name}:{type(err).__name__}"
                )
                continue
            candidate_rows.extend(rows)
            candidate_total += max(int(total), 0)
        if candidate_rows or candidate_total > 0:
            selected_rows = _sort_signal_rows(candidate_rows)
            selected_total = max(int(candidate_total), len(selected_rows))
            selected_error = candidate_errors[0] if candidate_errors else ""
            break
        if (
            not selected_error
            and candidate_errors
            and len(candidate_errors) == len(sources)
        ):
            selected_error = candidate_errors[0]
    signal_slice, signal_total_from_rows, safe_page = _slice_signals(
        selected_rows,
        signal_page=signal_page,
        signal_page_size=signal_page_size,
    )
    return {
        "entity_key": normalized,
        "page_title": page_title,
        "signals": signal_slice,
        "signal_total": max(int(selected_total), int(signal_total_from_rows)),
        "signal_page": safe_page,
        "signal_page_size": max(int(signal_page_size or 1), 1),
        "related_sectors": [],
        "load_error": "" if signal_slice or selected_total > 0 else selected_error,
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
) -> dict[str, object]:
    normalized = _normalize_stock_key(stock_key)
    if not normalized:
        return _empty_stock_view(
            entity_key="",
            page_title="",
            signal_page_size=signal_page_size,
        )
    return load_stock_page_rows_from_env(
        normalized,
        signal_page=signal_page,
        signal_page_size=signal_page_size,
        author=str(author or "").strip(),
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
    key_candidates, relation_err = _resolve_stock_key_candidates(normalized)
    if relation_err:
        return {"related_sectors": [], "load_error": relation_err}
    selected_rows: list[dict[str, str]] = []
    selected_error = ""
    for candidate in key_candidates:
        candidate_rows: list[dict[str, str]] = []
        candidate_errors: list[str] = []
        for source in sources:
            try:
                engine = _build_source_engine(source.url, source_name=source.name)
                with postgres_connect_autocommit(engine) as conn:
                    candidate_rows.extend(
                        _load_stock_related_sectors(
                            conn,
                            stock_keys=[candidate],
                            signal_window_days=_DEFAULT_SIGNAL_WINDOW_DAYS,
                        )
                    )
            except BaseException as err:
                candidate_errors.append(
                    f"postgres_connect_error:{source.name}:{type(err).__name__}"
                )
                continue
        if candidate_rows:
            counts: dict[str, int] = {}
            for row in candidate_rows:
                sector_key = str(row.get("sector_key") or "").strip()
                if not sector_key:
                    continue
                try:
                    mention_count = int(str(row.get("mention_count") or "0") or 0)
                except ValueError:
                    mention_count = 0
                counts[sector_key] = int(counts.get(sector_key, 0)) + max(
                    mention_count, 0
                )
            ranked = sorted(
                counts.items(), key=lambda item: (-int(item[1]), str(item[0]))
            )
            selected_rows = [
                {"sector_key": sector_key, "mention_count": str(count)}
                for sector_key, count in ranked
            ]
            selected_error = candidate_errors[0] if candidate_errors else ""
            break
        if (
            not selected_error
            and candidate_errors
            and len(candidate_errors) == len(sources)
        ):
            selected_error = candidate_errors[0]
    return {
        "related_sectors": selected_rows,
        "load_error": "" if selected_rows else selected_error,
    }


def clear_stock_hot_read_caches() -> None:
    return None


__all__ = [
    "clear_stock_hot_read_caches",
    "load_stock_cached_view_from_env",
    "load_stock_page_rows_from_env",
    "load_stock_sidebar_cached_view",
]
