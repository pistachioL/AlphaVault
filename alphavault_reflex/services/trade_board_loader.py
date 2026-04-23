from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from functools import lru_cache
import json
import logging
import os
import time

from alphavault.constants import SCHEMA_WEIBO, SCHEMA_XUEQIU
from alphavault.db.sql.ui import (
    build_assertion_projection_expr,
    build_assertion_rollup_ctes,
    build_assertion_rollup_joins,
)
from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import (
    load_configured_postgres_sources_from_env,
    PostgresSource,
)
from alphavault.db.sql_rows import read_sql_rows
from alphavault.env import load_dotenv_if_present
from alphavault.research_workbench import RESEARCH_RELATIONS_TABLE
from alphavault.research_workbench.service import (
    get_research_workbench_engine_from_env,
)
from alphavault_reflex.services.homework_constants import TRADE_BOARD_MAX_WINDOW_DAYS
from alphavault_reflex.services.source_loader import (
    DEFAULT_FATAL_EXCEPTIONS,
    MISSING_POSTGRES_DSN_ERROR,
    SOURCE_SCHEMAS_EMPTY_ERROR,
    source_table,
)

_logger = logging.getLogger(__name__)
ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS = "REFLEX_HOMEWORK_SOURCE_MAX_WORKERS"
DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS = 2
_STANDARD_POSTGRES_ERROR_PREFIX = "postgres_connect_error:standard:"
_SOURCE_SCHEMA_NAMES = frozenset((SCHEMA_WEIBO, SCHEMA_XUEQIU))

TRADE_BOARD_ASSERTION_COLUMNS = [
    "post_uid",
    "idx",
    "entity_key",
    "action",
    "action_strength",
    "summary",
    "stock_codes",
    "stock_names",
]

STOCK_ALIAS_RELATIONS_SQL = """
SELECT relation_type, left_key, right_key, relation_label, source, updated_at
FROM {relations_table}
WHERE relation_type = 'stock_alias' OR relation_label = 'alias_of'
""".format(relations_table=RESEARCH_RELATIONS_TABLE)


def _load_source_schemas_from_env() -> list[PostgresSource]:
    return [
        source
        for source in load_configured_postgres_sources_from_env()
        if str(getattr(source, "schema", getattr(source, "name", "")) or "").strip()
        in _SOURCE_SCHEMA_NAMES
    ]


def _format_trade_board_query_datetime(value: datetime) -> str:
    ts = value
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    else:
        ts = ts.astimezone(UTC)
    return ts.isoformat(sep=" ", timespec="seconds")


def trade_board_cutoff_from_utc_now(*, lookback_days: int) -> str:
    days = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    now = datetime.now(UTC)
    cutoff_day = now.date() - timedelta(days=max(0, int(days) - 1))
    cutoff = datetime(
        cutoff_day.year,
        cutoff_day.month,
        cutoff_day.day,
        tzinfo=UTC,
    )
    return _format_trade_board_query_datetime(cutoff)


def trade_board_select_expr() -> str:
    return ", ".join(
        [build_assertion_projection_expr(TRADE_BOARD_ASSERTION_COLUMNS)]
        + [
            "p.author AS author",
            "p.created_at AS created_at",
            "p.url AS url",
        ]
    )


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def trade_board_created_at_sql_expr(column: str) -> str:
    as_text = f"CAST({column} AS text)"
    normalized = f"btrim(replace({as_text}, 'T', ' '))"
    normalized_utc = f"replace({normalized}, 'Z', '+00:00')"
    with_timezone = (
        f"{normalized} ~ "
        "'^[0-9]{4}-[0-9]{2}-[0-9]{2} "
        "[0-9]{2}:[0-9]{2}(:[0-9]{2}(\\.[0-9]+)?)?"
        "(Z|[+-][0-9]{2}(:?[0-9]{2})?)$'"
    )
    without_timezone = (
        f"{normalized} ~ "
        "'^[0-9]{4}-[0-9]{2}-[0-9]{2} "
        "[0-9]{2}:[0-9]{2}(:[0-9]{2}(\\.[0-9]+)?)?$'"
    )
    return (
        "CASE "
        f"WHEN {normalized} = '' THEN NULL "
        f"WHEN {with_timezone} THEN CAST({normalized_utc} AS timestamptz) "
        f"WHEN {without_timezone} THEN CAST(({normalized} || '+08:00') AS timestamptz) "
        "ELSE NULL "
        "END"
    )


def _normalize_trade_board_assertion_rows(
    rows: list[dict[str, object]],
    *,
    source_name: str,
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    resolved_source_name = _clean_text(source_name)
    for raw_row in rows:
        row = dict(raw_row)
        row["source"] = resolved_source_name
        for col in ["post_uid", "entity_key", "action", "summary", "author", "url"]:
            if col in row:
                row[col] = _clean_text(row.get(col))
        normalized.append(row)
    return normalized


def _normalize_stock_alias_relation_rows(
    rows: list[dict[str, object]],
    *,
    source_name: str,
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    resolved_source_name = _clean_text(source_name)
    for raw_row in rows:
        row = dict(raw_row)
        row["db_source"] = resolved_source_name
        normalized.append(row)
    return normalized


def _dedupe_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    deduped: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw_row in rows:
        row = dict(raw_row)
        marker = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(row)
    return deduped


def query_trade_board_assertion_rows(
    *,
    conn: object,
    start_time: str,
    end_time: str,
    source_name: str,
) -> list[dict[str, object]]:
    posts_table = source_table(source_name, "posts")
    assertions_table = source_table(source_name, "assertions")
    assertion_entities_table = source_table(source_name, "assertion_entities")
    assertion_mentions_table = source_table(source_name, "assertion_mentions")
    topic_cluster_topics_table = source_table(source_name, "topic_cluster_topics")
    created_at_expr = trade_board_created_at_sql_expr("p.created_at")
    sql = f"""
{build_assertion_rollup_ctes(assertion_entities_table=assertion_entities_table, assertion_mentions_table=assertion_mentions_table, topic_cluster_topics_table=topic_cluster_topics_table)}
SELECT {trade_board_select_expr()}
FROM {posts_table} p
JOIN {assertions_table} a ON a.post_uid = p.post_uid
{build_assertion_rollup_joins("a")}
WHERE p.processed_at IS NOT NULL
  AND {created_at_expr} >= CAST(:start_time AS timestamptz)
  AND {created_at_expr} < CAST(:end_time AS timestamptz)
  AND a.action LIKE 'trade.%'
ORDER BY {created_at_expr} DESC, p.post_uid DESC, a.idx DESC
"""
    return _normalize_trade_board_assertion_rows(
        read_sql_rows(
            conn,
            sql,
            params={"start_time": start_time, "end_time": end_time},
        ),
        source_name=source_name,
    )


def query_stock_alias_relation_rows(
    *, conn: object, source_name: str
) -> list[dict[str, object]]:
    return _normalize_stock_alias_relation_rows(
        read_sql_rows(conn, STOCK_ALIAS_RELATIONS_SQL),
        source_name=source_name,
    )


def _standard_postgres_error_text(err: BaseException) -> str:
    text = str(err or "").strip()
    if text.startswith(_STANDARD_POSTGRES_ERROR_PREFIX):
        return text
    return f"{_STANDARD_POSTGRES_ERROR_PREFIX}{type(err).__name__}"


@lru_cache(maxsize=8)
def load_trade_board_assertion_rows_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    lookback_days: int,
) -> tuple[dict[str, object], ...]:
    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    cutoff = trade_board_cutoff_from_utc_now(lookback_days=lookback)
    now_text = _format_trade_board_query_datetime(datetime.now(UTC))
    del auth_token
    engine = ensure_postgres_engine(db_url, schema_name=source_name)
    with postgres_connect_autocommit(engine) as conn:
        rows = query_trade_board_assertion_rows(
            conn=conn,
            start_time=cutoff,
            end_time=now_text,
            source_name=source_name,
        )
    return tuple(dict(row) for row in rows)


@lru_cache(maxsize=8)
def load_trade_board_assertions_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    lookback_days: int,
) -> tuple[dict[str, object], ...]:
    return load_trade_board_assertion_rows_cached(
        db_url,
        auth_token,
        source_name,
        lookback_days,
    )


@lru_cache(maxsize=2)
def load_stock_alias_relation_rows_cached() -> tuple[dict[str, object], ...]:
    engine = get_research_workbench_engine_from_env()
    with postgres_connect_autocommit(engine) as conn:
        rows = query_stock_alias_relation_rows(
            conn=conn,
            source_name="standard",
        )
    return tuple(dict(row) for row in rows)


@lru_cache(maxsize=2)
def load_stock_alias_relations_cached() -> tuple[dict[str, object], ...]:
    return load_stock_alias_relation_rows_cached()


def resolve_homework_source_workers(*, source_count: int) -> int:
    total = max(1, int(source_count or 1))
    raw = os.getenv(ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "").strip()
    if not raw:
        wanted = int(DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS)
    else:
        try:
            wanted = int(raw)
        except ValueError:
            wanted = int(DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS)
    return max(1, min(int(wanted), total))


@lru_cache(maxsize=8)
def load_homework_board_payload_rows_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    start_time: str,
    end_time: str,
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    start = time.perf_counter()
    del auth_token
    engine = ensure_postgres_engine(db_url, schema_name=source_name)
    assertion_count = 0
    relation_count = 0
    with postgres_connect_autocommit(engine) as conn:
        assertions = query_trade_board_assertion_rows(
            conn=conn,
            start_time=start_time,
            end_time=end_time,
            source_name=source_name,
        )
        assertion_count = int(len(assertions))
        try:
            relations = list(load_stock_alias_relation_rows_cached())
            relation_count = int(len(relations))
        except BaseException as err:
            if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                raise
            error_text = _standard_postgres_error_text(err)
            _logger.warning(
                "homework_payload relation_query_failed source=%s err=%s",
                source_name,
                error_text,
            )
            raise RuntimeError(error_text) from err
    _logger.debug(
        "homework_payload source_query source=%s start=%s end=%s assertions=%d relations=%d elapsed=%.3fs",
        source_name,
        start_time,
        end_time,
        assertion_count,
        relation_count,
        time.perf_counter() - start,
    )
    return tuple(dict(row) for row in assertions), tuple(dict(row) for row in relations)


@lru_cache(maxsize=8)
def load_homework_board_payload_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    start_time: str,
    end_time: str,
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    return load_homework_board_payload_rows_cached(
        db_url,
        auth_token,
        source_name,
        start_time,
        end_time,
    )


def load_trade_board_assertions_from_env(
    lookback_days: int,
    *,
    load_cached_fn=load_trade_board_assertions_cached,
) -> tuple[list[dict[str, object]], str]:
    load_dotenv_if_present()
    sources = _load_source_schemas_from_env()
    if not sources:
        return [], MISSING_POSTGRES_DSN_ERROR

    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    rows: list[dict[str, object]] = []
    max_workers = max(1, min(4, int(len(sources))))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                load_cached_fn,
                source.url,
                source.token,
                source.name,
                lookback,
            ): source.name
            for source in sources
        }
        for fut in as_completed(futures):
            name = futures.get(fut, "")
            try:
                rows.extend(dict(row) for row in fut.result())
            except BaseException as err:
                if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                    raise
                return [], f"postgres_connect_error:{name}:{type(err).__name__}"

    if not rows:
        return [], SOURCE_SCHEMAS_EMPTY_ERROR
    return rows, ""


def load_trade_board_assertion_rows_from_env(
    lookback_days: int,
    *,
    load_cached_fn=load_trade_board_assertion_rows_cached,
) -> tuple[list[dict[str, object]], str]:
    load_dotenv_if_present()
    sources = _load_source_schemas_from_env()
    if not sources:
        return [], MISSING_POSTGRES_DSN_ERROR

    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    rows: list[dict[str, object]] = []
    max_workers = max(1, min(4, int(len(sources))))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                load_cached_fn,
                source.url,
                source.token,
                source.name,
                lookback,
            ): source.name
            for source in sources
        }
        for fut in as_completed(futures):
            name = futures.get(fut, "")
            try:
                rows.extend(dict(row) for row in fut.result())
            except BaseException as err:
                if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                    raise
                return [], f"postgres_connect_error:{name}:{type(err).__name__}"

    if not rows:
        return [], SOURCE_SCHEMAS_EMPTY_ERROR
    return rows, ""


def load_homework_board_payload_rows_from_env(
    start_time: str,
    end_time: str,
    *,
    load_cached_fn=load_homework_board_payload_rows_cached,
    resolve_workers_fn=resolve_homework_source_workers,
) -> tuple[list[dict[str, object]], list[dict[str, object]], str]:
    start = time.perf_counter()
    load_dotenv_if_present()
    sources = _load_source_schemas_from_env()
    if not sources:
        return [], [], MISSING_POSTGRES_DSN_ERROR

    assertions_rows: list[dict[str, object]] = []
    relation_rows: list[dict[str, object]] = []
    source_errors: list[str] = []

    max_workers = resolve_workers_fn(source_count=len(sources))
    if max_workers == 1:
        for source in sources:
            source_start = time.perf_counter()
            try:
                assertions, relations = load_cached_fn(
                    source.url,
                    source.token,
                    source.name,
                    start_time,
                    end_time,
                )
            except BaseException as err:
                if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                    raise
                error_text = str(err or "").strip()
                if error_text.startswith(_STANDARD_POSTGRES_ERROR_PREFIX):
                    return [], [], error_text
                source_errors.append(
                    f"postgres_connect_error:{source.name}:{type(err).__name__}"
                )
                _logger.warning(
                    "homework_payload source_failed source=%s mode=serial err=%s",
                    source.name,
                    type(err).__name__,
                )
                continue
            assertions_rows.extend(dict(row) for row in assertions)
            relation_rows.extend(dict(row) for row in relations)
            _logger.debug(
                "homework_payload source_done source=%s mode=serial assertions=%d relations=%d elapsed=%.3fs",
                source.name,
                int(len(assertions)),
                int(len(relations)),
                time.perf_counter() - source_start,
            )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {}
            for source in sources:
                futures[
                    pool.submit(
                        load_cached_fn,
                        source.url,
                        source.token,
                        source.name,
                        start_time,
                        end_time,
                    )
                ] = (source.name, time.perf_counter())
            for fut in as_completed(futures):
                name, source_start = futures.get(fut, ("", time.perf_counter()))
                try:
                    assertions, relations = fut.result()
                except BaseException as err:
                    if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                        raise
                    error_text = str(err or "").strip()
                    if error_text.startswith(_STANDARD_POSTGRES_ERROR_PREFIX):
                        return [], [], error_text
                    source_errors.append(
                        f"postgres_connect_error:{name}:{type(err).__name__}"
                    )
                    _logger.warning(
                        "homework_payload source_failed source=%s mode=parallel err=%s",
                        name,
                        type(err).__name__,
                    )
                    continue
                assertions_rows.extend(dict(row) for row in assertions)
                relation_rows.extend(dict(row) for row in relations)
                _logger.debug(
                    "homework_payload source_done source=%s mode=parallel assertions=%d relations=%d elapsed=%.3fs",
                    name,
                    int(len(assertions)),
                    int(len(relations)),
                    time.perf_counter() - source_start,
                )

    if not assertions_rows:
        if source_errors:
            return [], [], source_errors[0]
        return [], [], SOURCE_SCHEMAS_EMPTY_ERROR

    deduped_relations = _dedupe_rows(relation_rows)
    _logger.debug(
        "homework_payload done sources=%d workers=%d assertions=%d relations=%d elapsed=%.3fs",
        int(len(sources)),
        int(max_workers),
        int(len(assertions_rows)),
        int(len(deduped_relations)),
        time.perf_counter() - start,
    )
    return assertions_rows, deduped_relations, ""


def load_homework_board_payload_from_env(
    start_time: str,
    end_time: str,
    *,
    load_cached_fn=load_homework_board_payload_cached,
    resolve_workers_fn=resolve_homework_source_workers,
) -> tuple[list[dict[str, object]], list[dict[str, object]], str]:
    start = time.perf_counter()
    load_dotenv_if_present()
    sources = _load_source_schemas_from_env()
    if not sources:
        return [], [], MISSING_POSTGRES_DSN_ERROR

    assertions_rows: list[dict[str, object]] = []
    relation_rows: list[dict[str, object]] = []
    source_errors: list[str] = []

    max_workers = resolve_workers_fn(source_count=len(sources))
    if max_workers == 1:
        for source in sources:
            source_start = time.perf_counter()
            try:
                assertions, relations = load_cached_fn(
                    source.url,
                    source.token,
                    source.name,
                    start_time,
                    end_time,
                )
            except BaseException as err:
                if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                    raise
                error_text = str(err or "").strip()
                if error_text.startswith(_STANDARD_POSTGRES_ERROR_PREFIX):
                    return [], [], error_text
                source_errors.append(
                    f"postgres_connect_error:{source.name}:{type(err).__name__}"
                )
                _logger.warning(
                    "homework_payload source_failed source=%s mode=serial err=%s",
                    source.name,
                    type(err).__name__,
                )
                continue
            assertions_rows.extend(dict(row) for row in assertions)
            relation_rows.extend(dict(row) for row in relations)
            _logger.debug(
                "homework_payload source_done source=%s mode=serial assertions=%d relations=%d elapsed=%.3fs",
                source.name,
                int(len(assertions)),
                int(len(relations)),
                time.perf_counter() - source_start,
            )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {}
            for source in sources:
                futures[
                    pool.submit(
                        load_cached_fn,
                        source.url,
                        source.token,
                        source.name,
                        start_time,
                        end_time,
                    )
                ] = (source.name, time.perf_counter())
            for fut in as_completed(futures):
                name, source_start = futures.get(fut, ("", time.perf_counter()))
                try:
                    assertions, relations = fut.result()
                except BaseException as err:
                    if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                        raise
                    error_text = str(err or "").strip()
                    if error_text.startswith(_STANDARD_POSTGRES_ERROR_PREFIX):
                        return [], [], error_text
                    source_errors.append(
                        f"postgres_connect_error:{name}:{type(err).__name__}"
                    )
                    _logger.warning(
                        "homework_payload source_failed source=%s mode=parallel err=%s",
                        name,
                        type(err).__name__,
                    )
                    continue
                assertions_rows.extend(dict(row) for row in assertions)
                relation_rows.extend(dict(row) for row in relations)
                _logger.debug(
                    "homework_payload source_done source=%s mode=parallel assertions=%d relations=%d elapsed=%.3fs",
                    name,
                    int(len(assertions)),
                    int(len(relations)),
                    time.perf_counter() - source_start,
                )

    if not assertions_rows:
        if source_errors:
            return [], [], source_errors[0]
        return [], [], SOURCE_SCHEMAS_EMPTY_ERROR
    deduped_relations = _dedupe_rows(relation_rows)
    _logger.debug(
        "homework_payload done sources=%d workers=%d assertions=%d relations=%d elapsed=%.3fs",
        int(len(sources)),
        int(max_workers),
        int(len(assertions_rows)),
        int(len(deduped_relations)),
        time.perf_counter() - start,
    )
    return assertions_rows, deduped_relations, ""


def load_stock_alias_relations_from_env(
    *,
    load_cached_fn=load_stock_alias_relations_cached,
) -> tuple[list[dict[str, object]], str]:
    load_dotenv_if_present()
    try:
        return [dict(row) for row in load_cached_fn()], ""
    except BaseException as err:
        if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
            raise
        return [], _standard_postgres_error_text(err)


def load_stock_alias_relation_rows_from_env(
    *,
    load_cached_fn=load_stock_alias_relation_rows_cached,
) -> tuple[list[dict[str, object]], str]:
    load_dotenv_if_present()
    try:
        return [dict(row) for row in load_cached_fn()], ""
    except BaseException as err:
        if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
            raise
        return [], _standard_postgres_error_text(err)


__all__ = [
    "DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS",
    "ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS",
    "TRADE_BOARD_ASSERTION_COLUMNS",
    "load_homework_board_payload_rows_cached",
    "load_homework_board_payload_rows_from_env",
    "load_homework_board_payload_cached",
    "load_homework_board_payload_from_env",
    "load_stock_alias_relation_rows_cached",
    "load_stock_alias_relation_rows_from_env",
    "load_stock_alias_relations_cached",
    "load_stock_alias_relations_from_env",
    "load_trade_board_assertion_rows_cached",
    "load_trade_board_assertion_rows_from_env",
    "load_trade_board_assertions_cached",
    "load_trade_board_assertions_from_env",
    "query_stock_alias_relation_rows",
    "query_trade_board_assertion_rows",
    "resolve_homework_source_workers",
]
