from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache

from alphavault.constants import (
    ENV_POSTGRES_DSN,
    SCHEMA_WEIBO,
    SCHEMA_XUEQIU,
)
from alphavault.domains.stock.names import normalize_stock_official_name_norm
from alphavault.domains.relation.ids import make_candidate_id
from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
    qualify_postgres_table,
)
from alphavault.db.postgres_env import (
    load_configured_postgres_sources_from_env,
    PostgresSource,
)
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql.ui import build_assertions_query
from alphavault.db.sql_rows import read_sql_rows
from alphavault.env import load_dotenv_if_present
from alphavault.research_workbench import (
    RESEARCH_RELATIONS_TABLE,
    get_official_names_by_stock_keys,
    get_research_workbench_engine_from_env,
)
from alphavault_reflex.services.source_read_utils import (
    ensure_platform_post_id_rows,
    normalize_assertions_datetime_rows,
    normalize_posts_datetime_rows,
    parse_json_list,
)

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_SOURCE_SCHEMA_NAMES = frozenset((SCHEMA_WEIBO, SCHEMA_XUEQIU))

MISSING_POSTGRES_DSN_ERROR = f"Missing {ENV_POSTGRES_DSN}"
SOURCE_SCHEMAS_EMPTY_ERROR = "source_schemas_empty"
STOCK_ALIAS_FAST_WINDOW_DAYS = 30
STOCK_ALIAS_FAST_LIMIT = 30

WANTED_TRADE_ASSERTION_COLUMNS = [
    "post_uid",
    "idx",
    "entity_key",
    "action",
    "action_strength",
    "summary",
    "evidence",
    "confidence",
    "stock_codes",
    "stock_names",
    "industries_json",
    "commodities_json",
    "indices_json",
    "author",
    "created_at",
]

WANTED_POST_COLUMNS_FOR_TREE = [
    "post_uid",
    "platform_post_id",
    "author",
    "created_at",
    "url",
    "raw_text",
]


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _stock_name_from_key(value: object) -> str:
    text = _clean_text(value)
    if not text.startswith("stock:"):
        return ""
    return _clean_text(text[len("stock:") :])


def _normalize_name(value: object) -> str:
    return normalize_stock_official_name_norm(value)


def _window_cutoff_str(window_days: int) -> str:
    return (
        datetime.now(timezone.utc) - timedelta(days=max(1, int(window_days or 1)))
    ).strftime("%Y-%m-%d %H:%M:%S")


def _is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def source_schema_name(source: object) -> str:
    if isinstance(source, str):
        return str(source or "").strip()
    schema_name = str(getattr(source, "schema", "") or "").strip()
    if schema_name:
        return schema_name
    return str(getattr(source, "name", "") or "").strip()


def source_table(source_name: str, table_name: str) -> str:
    return qualify_postgres_table(str(source_name or "").strip(), table_name)


def load_configured_source_schemas_from_env() -> list[PostgresSource]:
    return [
        source
        for source in load_configured_postgres_sources_from_env()
        if source_schema_name(source) in _SOURCE_SCHEMA_NAMES
    ]


def standardize_posts_rows(
    rows: list[dict[str, object]],
    *,
    source_name: str,
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    resolved_source_name = _clean_text(source_name)
    defaults: dict[str, object] = {
        "post_uid": "",
        "platform": resolved_source_name,
        "platform_post_id": "",
        "author": "",
        "created_at": "",
        "url": "",
        "raw_text": "",
        "status": "",
        "invest_score": 0.0,
        "processed_at": "",
    }
    text_columns = {
        "post_uid",
        "platform",
        "platform_post_id",
        "author",
        "url",
        "raw_text",
        "status",
    }
    for raw_row in rows:
        row = dict(raw_row)
        for col, default in defaults.items():
            if col not in row or row.get(col) is None:
                row[col] = default
        for col in text_columns:
            row[col] = _clean_text(row.get(col))
        row["platform"] = _clean_text(row.get("platform")) or resolved_source_name
        row["source"] = resolved_source_name
        normalized.append(row)
    return normalized


def standardize_assertions_rows(
    assertions: list[dict[str, object]],
    posts: list[dict[str, object]],
    *,
    source_name: str,
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    resolved_source_name = _clean_text(source_name)
    defaults: dict[str, object] = {
        "post_uid": "",
        "idx": 0,
        "entity_key": "",
        "action": "",
        "action_strength": 0,
        "summary": "",
        "evidence": "",
        "confidence": 0.0,
        "stock_codes": "[]",
        "stock_names": "[]",
        "industries_json": "[]",
        "commodities_json": "[]",
        "indices_json": "[]",
        "author": "",
        "created_at": "",
    }
    posts_by_uid = {
        _clean_text(row.get("post_uid")): dict(row)
        for row in posts
        if _clean_text(row.get("post_uid"))
    }
    text_columns = {
        "post_uid",
        "entity_key",
        "action",
        "summary",
        "evidence",
        "author",
        "url",
        "raw_text",
    }
    for raw_row in assertions:
        row = dict(raw_row)
        for col, default in defaults.items():
            if col not in row or row.get(col) is None:
                row[col] = default

        post_uid = _clean_text(row.get("post_uid"))
        post_row = posts_by_uid.get(post_uid, {})
        row["post_uid"] = post_uid
        row["source"] = resolved_source_name
        row["url"] = _clean_text(post_row.get("url"))
        row["raw_text"] = _clean_text(post_row.get("raw_text"))

        if _is_missing_value(row.get("author")):
            row["author"] = post_row.get("author", "")
        if _is_missing_value(row.get("created_at")):
            row["created_at"] = post_row.get("created_at", "")

        for col in text_columns:
            if col in row:
                row[col] = _clean_text(row.get(col))

        row["stock_codes"] = parse_json_list(row.get("stock_codes"))
        row["stock_names"] = parse_json_list(row.get("stock_names"))
        row["industries"] = parse_json_list(row.get("industries_json"))
        row["commodities"] = parse_json_list(row.get("commodities_json"))
        row["indices"] = parse_json_list(row.get("indices_json"))
        normalized.append(row)
    return normalized


@lru_cache(maxsize=2)
def load_trade_sources_rows_cached(
    db_url: str, auth_token: str, source_name: str
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    del auth_token
    schema_name = source_schema_name(source_name)
    engine = ensure_postgres_engine(db_url, schema_name=schema_name)
    posts_table = source_table(schema_name, "posts")
    assertions_table = source_table(schema_name, "assertions")
    assertion_entities_table = source_table(schema_name, "assertion_entities")
    assertion_mentions_table = source_table(schema_name, "assertion_mentions")
    topic_cluster_topics_table = source_table(schema_name, "topic_cluster_topics")
    with postgres_connect_autocommit(engine) as conn:
        posts_query = f"""
SELECT {", ".join(WANTED_POST_COLUMNS_FOR_TREE)}
FROM {posts_table}
WHERE processed_at IS NOT NULL
"""
        base_query = build_assertions_query(
            WANTED_TRADE_ASSERTION_COLUMNS,
            posts_table=posts_table,
            assertions_table=assertions_table,
            assertion_entities_table=assertion_entities_table,
            assertion_mentions_table=assertion_mentions_table,
            topic_cluster_topics_table=topic_cluster_topics_table,
        )
        trade_query = f"{base_query} WHERE action LIKE 'trade.%'"

        posts = read_sql_rows(conn, posts_query)
        assertions = read_sql_rows(conn, trade_query)

    normalized_posts = standardize_posts_rows(posts, source_name=schema_name)
    normalized_posts = normalize_posts_datetime_rows(normalized_posts)
    normalized_posts = ensure_platform_post_id_rows(normalized_posts)
    normalized_assertions = standardize_assertions_rows(
        assertions,
        normalized_posts,
        source_name=schema_name,
    )
    normalized_assertions = normalize_assertions_datetime_rows(normalized_assertions)
    return tuple(dict(row) for row in normalized_posts), tuple(
        dict(row) for row in normalized_assertions
    )


@lru_cache(maxsize=2)
def load_trade_sources_cached(
    db_url: str, auth_token: str, source_name: str
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    return load_trade_sources_rows_cached(db_url, auth_token, source_name)


@lru_cache(maxsize=2)
def load_trade_assertion_rows_cached(
    db_url: str, auth_token: str, source_name: str
) -> tuple[dict[str, object], ...]:
    _, assertions = load_trade_sources_rows_cached(db_url, auth_token, source_name)
    return tuple(dict(row) for row in assertions)


def load_trade_assertions_cached(
    db_url: str, auth_token: str, source_name: str
) -> tuple[dict[str, object], ...]:
    return load_trade_assertion_rows_cached(db_url, auth_token, source_name)


def _stock_alias_candidate_query(schema_name: str) -> str:
    posts_table = source_table(schema_name, "posts")
    assertions_table = source_table(schema_name, "assertions")
    assertion_entities_table = source_table(schema_name, "assertion_entities")
    assertion_mentions_table = source_table(schema_name, "assertion_mentions")
    return f"""
WITH recent_trade AS (
    SELECT a.assertion_id
    FROM {assertions_table} a
    JOIN {posts_table} p
      ON p.post_uid = a.post_uid
    WHERE a.action LIKE 'trade.%'
      AND p.created_at >= :cutoff
),
stock_entity_per_assertion AS (
    SELECT
        ae.assertion_id AS assertion_id,
        COALESCE(
            MAX(CASE WHEN ae.is_primary = 1 AND ae.entity_type = 'stock' THEN ae.entity_key END),
            MAX(CASE WHEN ae.entity_type = 'stock' THEN ae.entity_key END),
            ''
        ) AS left_key
    FROM {assertion_entities_table} ae
    JOIN recent_trade rt
      ON rt.assertion_id = ae.assertion_id
    GROUP BY ae.assertion_id
),
alias_mentions AS (
    SELECT
        am.assertion_id AS assertion_id,
        TRIM(am.mention_text) AS alias_name
    FROM {assertion_mentions_table} am
    JOIN recent_trade rt
      ON rt.assertion_id = am.assertion_id
    WHERE am.mention_type = 'stock_name'
      AND TRIM(COALESCE(am.mention_text, '')) <> ''
    GROUP BY am.assertion_id, TRIM(am.mention_text)
)
SELECT
    sea.left_key AS left_key,
    CONCAT('stock:', alias_mentions.alias_name) AS right_key,
    COUNT(*) AS score
FROM stock_entity_per_assertion sea
JOIN alias_mentions
  ON alias_mentions.assertion_id = sea.assertion_id
WHERE sea.left_key LIKE 'stock:%'
  AND CONCAT('stock:', alias_mentions.alias_name) <> sea.left_key
GROUP BY sea.left_key, right_key
ORDER BY score DESC, left_key, right_key
"""


def _normalize_stock_alias_candidate_rows(
    rows: list[dict[str, object]],
) -> tuple[dict[str, object], ...]:
    out: list[dict[str, object]] = []
    for row in rows:
        left_key = _clean_text(row.get("left_key"))
        right_key = _clean_text(row.get("right_key"))
        if not left_key or not right_key:
            continue
        out.append(
            {
                "left_key": left_key,
                "right_key": right_key,
                "score": int(str(row.get("score") or "0").strip() or 0),
            }
        )
    return tuple(out)


def _build_stock_alias_candidate_row(
    *,
    left_key: str,
    right_key: str,
    score: int,
    window_days: int,
) -> dict[str, str]:
    evidence_text = f"近{max(1, int(window_days or 1))}天同票提及 {int(score)} 次"
    return {
        "relation_type": "stock_alias",
        "left_key": left_key,
        "right_key": right_key,
        "relation_label": "alias_of",
        "candidate_id": make_candidate_id(
            relation_type="stock_alias",
            left_key=left_key,
            right_key=right_key,
            relation_label="alias_of",
        ),
        "candidate_key": right_key,
        "score": str(int(score)),
        "suggestion_reason": evidence_text,
        "evidence_summary": evidence_text,
    }


def _filter_formal_stock_name_pairs(
    pair_scores: dict[tuple[str, str], int],
) -> dict[tuple[str, str], int]:
    names_by_pair = {
        pair_key: _stock_name_from_key(pair_key[1]) for pair_key in pair_scores
    }
    candidate_stock_keys = list(
        dict.fromkeys(pair_key[0] for pair_key, name in names_by_pair.items() if name)
    )
    if not candidate_stock_keys:
        return dict(pair_scores)
    try:
        engine = get_research_workbench_engine_from_env()
        official_names_by_stock_key = get_official_names_by_stock_keys(
            engine,
            candidate_stock_keys,
        )
    except Exception:
        return dict(pair_scores)
    return {
        pair_key: score
        for pair_key, score in pair_scores.items()
        if _normalize_name(official_names_by_stock_key.get(pair_key[0], ""))
        != _normalize_name(names_by_pair.get(pair_key, ""))
    }


def _load_confirmed_alias_right_keys(right_keys: list[str]) -> set[str]:
    normalized_right_keys = [
        key
        for key in dict.fromkeys(_clean_text(item) for item in right_keys)
        if key.startswith("stock:")
    ]
    if not normalized_right_keys:
        return set()
    placeholders = make_in_placeholders(
        prefix="right_key_",
        count=len(normalized_right_keys),
    )
    params = make_in_params(prefix="right_key_", values=normalized_right_keys)
    sql = f"""
SELECT right_key
FROM {RESEARCH_RELATIONS_TABLE}
WHERE relation_type = 'stock_alias'
  AND relation_label = 'alias_of'
  AND right_key IN ({placeholders})
"""
    try:
        engine = get_research_workbench_engine_from_env()
        with postgres_connect_autocommit(engine) as conn:
            rows = conn.execute(sql, params).fetchall()
    except Exception:
        return set()
    out: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            right_key = _clean_text(row.get("right_key"))
        else:
            right_key = _clean_text(row[0] if row else "")
        if not right_key.startswith("stock:"):
            continue
        out.add(right_key)
    return out


def _filter_confirmed_alias_pairs(
    pair_scores: dict[tuple[str, str], int],
) -> dict[tuple[str, str], int]:
    confirmed_right_keys = _load_confirmed_alias_right_keys(
        [pair_key[1] for pair_key in pair_scores]
    )
    if not confirmed_right_keys:
        return dict(pair_scores)
    return {
        pair_key: score
        for pair_key, score in pair_scores.items()
        if pair_key[1] not in confirmed_right_keys
    }


@lru_cache(maxsize=8)
def load_stock_alias_candidate_rows_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    window_days: int,
) -> tuple[dict[str, object], ...]:
    del auth_token
    schema_name = source_schema_name(source_name)
    engine = ensure_postgres_engine(db_url, schema_name=schema_name)
    with postgres_connect_autocommit(engine) as conn:
        rows = read_sql_rows(
            conn,
            _stock_alias_candidate_query(schema_name),
            params={"cutoff": _window_cutoff_str(window_days)},
        )
    return _normalize_stock_alias_candidate_rows(rows)


def load_trade_assertion_rows_from_env(
    *,
    load_cached_fn=load_trade_assertion_rows_cached,
) -> tuple[list[dict[str, object]], str]:
    load_dotenv_if_present()
    sources = load_configured_source_schemas_from_env()
    if not sources:
        return [], MISSING_POSTGRES_DSN_ERROR

    rows: list[dict[str, object]] = []
    for source in sources:
        try:
            rows.extend(
                dict(row)
                for row in load_cached_fn(source.url, source.token, source.name)
            )
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise
            return [], f"postgres_connect_error:{source.name}:{type(err).__name__}"

    if not rows:
        return [], SOURCE_SCHEMAS_EMPTY_ERROR
    return rows, ""


def load_trade_assertions_from_env(
    *,
    load_cached_fn=load_trade_assertions_cached,
) -> tuple[list[dict[str, object]], str]:
    load_dotenv_if_present()
    sources = load_configured_source_schemas_from_env()
    if not sources:
        return [], MISSING_POSTGRES_DSN_ERROR

    rows: list[dict[str, object]] = []
    for source in sources:
        try:
            rows.extend(
                dict(row)
                for row in load_cached_fn(source.url, source.token, source.name)
            )
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise
            return [], f"postgres_connect_error:{source.name}:{type(err).__name__}"

    if not rows:
        return [], SOURCE_SCHEMAS_EMPTY_ERROR
    return rows, ""


def load_stock_alias_candidates_from_env(
    *,
    window_days: int = STOCK_ALIAS_FAST_WINDOW_DAYS,
    limit: int = STOCK_ALIAS_FAST_LIMIT,
    load_cached_fn=load_stock_alias_candidate_rows_cached,
) -> tuple[list[dict[str, str]], str]:
    load_dotenv_if_present()
    sources = load_configured_source_schemas_from_env()
    if not sources:
        return [], MISSING_POSTGRES_DSN_ERROR

    pair_scores: dict[tuple[str, str], int] = {}
    for source in sources:
        try:
            source_rows = load_cached_fn(
                source.url,
                source.token,
                source.name,
                max(1, int(window_days or 1)),
            )
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise
            return [], f"postgres_connect_error:{source.name}:{type(err).__name__}"
        for row in source_rows:
            left_key = _clean_text(row.get("left_key"))
            right_key = _clean_text(row.get("right_key"))
            if not left_key or not right_key:
                continue
            pair_key = (left_key, right_key)
            pair_scores[pair_key] = int(pair_scores.get(pair_key, 0)) + int(
                row.get("score") or 0
            )

    if not pair_scores:
        return [], SOURCE_SCHEMAS_EMPTY_ERROR

    filtered_pair_scores = _filter_formal_stock_name_pairs(pair_scores)
    if not filtered_pair_scores:
        return [], ""
    filtered_pair_scores = _filter_confirmed_alias_pairs(filtered_pair_scores)
    if not filtered_pair_scores:
        return [], ""

    ranked_pairs = sorted(
        filtered_pair_scores.items(),
        key=lambda item: (-int(item[1]), str(item[0][0]), str(item[0][1])),
    )[: max(1, int(limit or STOCK_ALIAS_FAST_LIMIT))]
    return [
        _build_stock_alias_candidate_row(
            left_key=left_key,
            right_key=right_key,
            score=score,
            window_days=window_days,
        )
        for (left_key, right_key), score in ranked_pairs
    ], ""


def load_sources_rows_from_env(
    *,
    load_posts_for_tree_from_env_fn,
    load_trade_assertions_from_env_fn=load_trade_assertion_rows_from_env,
) -> tuple[list[dict[str, object]], list[dict[str, object]], str]:
    load_dotenv_if_present()
    posts, posts_err = load_posts_for_tree_from_env_fn()
    if posts_err:
        return [], [], posts_err
    assertions, assertions_err = load_trade_assertions_from_env_fn()
    if assertions_err:
        return [], [], assertions_err
    return posts, assertions, ""


def load_sources_from_env(
    *,
    load_posts_for_tree_from_env_fn,
    load_trade_assertions_from_env_fn=load_trade_assertions_from_env,
) -> tuple[list[dict[str, object]], list[dict[str, object]], str]:
    load_dotenv_if_present()
    posts, posts_err = load_posts_for_tree_from_env_fn()
    if posts_err:
        return [], [], posts_err
    assertions, assertions_err = load_trade_assertions_from_env_fn()
    if assertions_err:
        return [], [], assertions_err
    return posts, assertions, ""


__all__ = [
    "DEFAULT_FATAL_EXCEPTIONS",
    "MISSING_POSTGRES_DSN_ERROR",
    "SOURCE_SCHEMAS_EMPTY_ERROR",
    "WANTED_POST_COLUMNS_FOR_TREE",
    "WANTED_TRADE_ASSERTION_COLUMNS",
    "load_configured_source_schemas_from_env",
    "load_sources_from_env",
    "load_sources_rows_from_env",
    "load_stock_alias_candidate_rows_cached",
    "load_stock_alias_candidates_from_env",
    "load_trade_assertion_rows_cached",
    "load_trade_assertion_rows_from_env",
    "load_trade_assertions_cached",
    "load_trade_assertions_from_env",
    "load_trade_sources_cached",
    "load_trade_sources_rows_cached",
    "source_schema_name",
    "source_table",
    "standardize_assertions_rows",
    "standardize_posts_rows",
]

DEFAULT_FATAL_EXCEPTIONS = _FATAL_BASE_EXCEPTIONS
