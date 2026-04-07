from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import lru_cache
import logging
import os
import time

import pandas as pd

from alphavault.db.sql.ui import (
    build_assertion_projection_expr,
    build_assertion_rollup_ctes,
    build_assertion_rollup_joins,
)
from alphavault.db.turso_db import ensure_turso_engine, turso_connect_autocommit
from alphavault.db.turso_env import load_configured_turso_sources_from_env
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.env import load_dotenv_if_present
from alphavault.research_workbench import RESEARCH_RELATIONS_TABLE
from alphavault.research_workbench.service import (
    get_research_workbench_engine_from_env,
)
from alphavault_reflex.services.homework_constants import TRADE_BOARD_MAX_WINDOW_DAYS
from alphavault_reflex.services.source_loader import (
    DEFAULT_FATAL_EXCEPTIONS,
    MISSING_TURSO_SOURCES_ERROR,
)
from alphavault_reflex.services.turso_read_utils import normalize_assertions_datetime

_logger = logging.getLogger(__name__)
ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS = "REFLEX_HOMEWORK_SOURCE_MAX_WORKERS"
DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS = 2
_STANDARD_TURSO_ERROR_PREFIX = "turso_connect_error:standard:"

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


def trade_board_cutoff_from_utc_now(*, lookback_days: int) -> str:
    days = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    now = datetime.utcnow()
    cutoff_day = now.date() - timedelta(days=max(0, int(days) - 1))
    cutoff = datetime.combine(cutoff_day, datetime.min.time())
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def trade_board_select_expr() -> str:
    return ", ".join(
        [build_assertion_projection_expr(TRADE_BOARD_ASSERTION_COLUMNS)]
        + [
            "p.author AS author",
            "p.created_at AS created_at",
            "p.url AS url",
        ]
    )


def query_trade_board_assertions(
    *, conn: object, cutoff: str, source_name: str
) -> pd.DataFrame:
    sql = f"""
{build_assertion_rollup_ctes()}
SELECT {trade_board_select_expr()}
FROM posts p
JOIN assertions a ON a.post_uid = p.post_uid
{build_assertion_rollup_joins("a")}
WHERE p.processed_at IS NOT NULL
  AND p.created_at >= :cutoff
  AND a.action LIKE 'trade.%'
"""
    df = turso_read_sql_df(conn, sql, params={"cutoff": cutoff})
    if df.empty:
        return df
    df = df.copy()
    df["source"] = str(source_name or "").strip()
    for col in ["post_uid", "entity_key", "action", "summary", "author", "url"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    df = normalize_assertions_datetime(df)
    return df


def query_stock_alias_relations(*, conn: object, source_name: str) -> pd.DataFrame:
    df = turso_read_sql_df(conn, STOCK_ALIAS_RELATIONS_SQL)
    if df.empty:
        return df
    df = df.copy()
    df["db_source"] = source_name
    return df


def _standard_turso_error_text(err: BaseException) -> str:
    text = str(err or "").strip()
    if text.startswith(_STANDARD_TURSO_ERROR_PREFIX):
        return text
    return f"{_STANDARD_TURSO_ERROR_PREFIX}{type(err).__name__}"


@lru_cache(maxsize=8)
def load_trade_board_assertions_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    lookback_days: int,
) -> pd.DataFrame:
    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    cutoff = trade_board_cutoff_from_utc_now(lookback_days=lookback)
    engine = ensure_turso_engine(db_url, auth_token)
    with turso_connect_autocommit(engine) as conn:
        return query_trade_board_assertions(
            conn=conn,
            cutoff=cutoff,
            source_name=source_name,
        )


@lru_cache(maxsize=2)
def load_stock_alias_relations_cached() -> pd.DataFrame:
    engine = get_research_workbench_engine_from_env()
    with turso_connect_autocommit(engine) as conn:
        return query_stock_alias_relations(
            conn=conn,
            source_name="standard",
        )


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
def load_homework_board_payload_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    lookback_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    start = time.perf_counter()
    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    cutoff = trade_board_cutoff_from_utc_now(lookback_days=lookback)
    engine = ensure_turso_engine(db_url, auth_token)
    assertion_count = 0
    relation_count = 0
    with turso_connect_autocommit(engine) as conn:
        assertions = query_trade_board_assertions(
            conn=conn,
            cutoff=cutoff,
            source_name=source_name,
        )
        assertion_count = int(len(assertions))
        try:
            relations = load_stock_alias_relations_cached()
            relation_count = int(len(relations))
        except BaseException as err:
            if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                raise
            error_text = _standard_turso_error_text(err)
            _logger.warning(
                "homework_payload relation_query_failed source=%s err=%s",
                source_name,
                error_text,
            )
            raise RuntimeError(error_text) from err
    _logger.debug(
        "homework_payload source_query source=%s lookback_days=%d assertions=%d relations=%d elapsed=%.3fs",
        source_name,
        int(lookback),
        assertion_count,
        relation_count,
        time.perf_counter() - start,
    )
    return assertions, relations


def load_trade_board_assertions_from_env(
    lookback_days: int,
    *,
    load_cached_fn=load_trade_board_assertions_cached,
) -> tuple[pd.DataFrame, str]:
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    frames: list[pd.DataFrame] = []
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
                frames.append(fut.result())
            except BaseException as err:
                if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                    raise
                return (
                    pd.DataFrame(),
                    f"turso_connect_error:{name}:{type(err).__name__}",
                )

    if not frames:
        return pd.DataFrame(), "turso_sources_empty"
    return pd.concat(frames, ignore_index=True), ""


def load_homework_board_payload_from_env(
    lookback_days: int,
    *,
    load_cached_fn=load_homework_board_payload_cached,
    resolve_workers_fn=resolve_homework_source_workers,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    start = time.perf_counter()
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return pd.DataFrame(), pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    assertions_frames: list[pd.DataFrame] = []
    relation_frames: list[pd.DataFrame] = []
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
                    lookback,
                )
            except BaseException as err:
                if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                    raise
                error_text = str(err or "").strip()
                if error_text.startswith(_STANDARD_TURSO_ERROR_PREFIX):
                    return pd.DataFrame(), pd.DataFrame(), error_text
                source_errors.append(
                    f"turso_connect_error:{source.name}:{type(err).__name__}"
                )
                _logger.warning(
                    "homework_payload source_failed source=%s mode=serial err=%s",
                    source.name,
                    type(err).__name__,
                )
                continue
            assertions_frames.append(assertions)
            relation_frames.append(relations)
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
                        lookback,
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
                    if error_text.startswith(_STANDARD_TURSO_ERROR_PREFIX):
                        return pd.DataFrame(), pd.DataFrame(), error_text
                    source_errors.append(
                        f"turso_connect_error:{name}:{type(err).__name__}"
                    )
                    _logger.warning(
                        "homework_payload source_failed source=%s mode=parallel err=%s",
                        name,
                        type(err).__name__,
                    )
                    continue
                assertions_frames.append(assertions)
                relation_frames.append(relations)
                _logger.debug(
                    "homework_payload source_done source=%s mode=parallel assertions=%d relations=%d elapsed=%.3fs",
                    name,
                    int(len(assertions)),
                    int(len(relations)),
                    time.perf_counter() - source_start,
                )

    if not assertions_frames:
        if source_errors:
            return pd.DataFrame(), pd.DataFrame(), source_errors[0]
        return pd.DataFrame(), pd.DataFrame(), "turso_sources_empty"
    assertions_all = pd.concat(assertions_frames, ignore_index=True)
    relations_all = pd.concat(relation_frames, ignore_index=True)
    if not relations_all.empty:
        relations_all = relations_all.drop_duplicates().reset_index(drop=True)
    _logger.debug(
        "homework_payload done sources=%d workers=%d assertions=%d relations=%d elapsed=%.3fs",
        int(len(sources)),
        int(max_workers),
        int(len(assertions_all)),
        int(len(relations_all)),
        time.perf_counter() - start,
    )
    return assertions_all, relations_all, ""


def load_stock_alias_relations_from_env(
    *,
    load_cached_fn=load_stock_alias_relations_cached,
) -> tuple[pd.DataFrame, str]:
    load_dotenv_if_present()
    try:
        return load_cached_fn(), ""
    except BaseException as err:
        if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
            raise
        return (
            pd.DataFrame(),
            _standard_turso_error_text(err),
        )


__all__ = [
    "DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS",
    "ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS",
    "TRADE_BOARD_ASSERTION_COLUMNS",
    "load_homework_board_payload_cached",
    "load_homework_board_payload_from_env",
    "load_stock_alias_relations_cached",
    "load_stock_alias_relations_from_env",
    "load_trade_board_assertions_cached",
    "load_trade_board_assertions_from_env",
    "resolve_homework_source_workers",
]
