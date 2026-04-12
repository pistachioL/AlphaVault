from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import TypedDict

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.constants import SCHEMA_WEIBO, SCHEMA_XUEQIU  # noqa: E402
from alphavault.db.postgres_db import (  # noqa: E402
    PostgresConnection,
    ensure_postgres_engine,
    postgres_connect_autocommit,
    qualify_postgres_table,
)
from alphavault.db.postgres_env import load_configured_postgres_sources_from_env  # noqa: E402
from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.logging_config import (  # noqa: E402
    add_log_level_argument,
    configure_logging,
    get_logger,
)

BAD_CN_STOCK_US_KEY_REGEX = r"^stock:(SH|SZ|BJ)(\d{6})\.US$"
BAD_CN_STOCK_US_KEY_REPLACEMENT = r"stock:\2.\1"
DEFAULT_SAMPLE_LIMIT = 20
_SOURCE_SCHEMAS = (SCHEMA_WEIBO, SCHEMA_XUEQIU)
logger = get_logger(__name__)


class FixPlanRow(TypedDict):
    bad_key: str
    good_key: str
    row_count: int


class FixSampleRow(TypedDict):
    assertion_id: str
    bad_key: str
    good_key: str
    has_conflict: bool


class SchemaFixPlan(TypedDict):
    schema_name: str
    bad_key_count: int
    bad_row_count: int
    conflict_delete_count: int
    update_row_count: int
    group_rows: list[FixPlanRow]
    sample_rows: list[FixSampleRow]


class ApplyStats(TypedDict):
    schema_name: str
    deleted_conflict_rows: int
    updated_rows: int
    remaining_bad_rows: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检查并修复 CN 假 .US 股票键")
    parser.add_argument(
        "--schema",
        choices=(*_SOURCE_SCHEMAS, "all"),
        default="all",
        help="只看某个 source schema，默认 all",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=DEFAULT_SAMPLE_LIMIT,
        help="dry-run 最多打印多少条样例",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="真的写库；默认只做 dry-run",
    )
    add_log_level_argument(parser)
    return parser.parse_args(argv)


def _assertion_entities_table(schema_name: str) -> str:
    return qualify_postgres_table(schema_name, "assertion_entities")


def _bad_rows_cte(table_name: str) -> str:
    return f"""
WITH bad AS (
    SELECT
        assertion_id,
        entity_key AS bad_key,
        regexp_replace(
            entity_key,
            :bad_key_regex,
            :good_key_replacement
        ) AS good_key
    FROM {table_name}
    WHERE entity_type = 'stock'
      AND entity_key ~ :bad_key_regex
)
"""


def _query_params() -> dict[str, str]:
    return {
        "bad_key_regex": BAD_CN_STOCK_US_KEY_REGEX,
        "good_key_replacement": BAD_CN_STOCK_US_KEY_REPLACEMENT,
    }


def _count_bad_rows(conn: PostgresConnection, *, table_name: str) -> int:
    row = conn.execute(
        f"""
SELECT COUNT(*)
FROM {table_name}
WHERE entity_type = 'stock'
  AND entity_key ~ :bad_key_regex
""",
        _query_params(),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _group_rows(conn: PostgresConnection, *, table_name: str) -> list[FixPlanRow]:
    rows = (
        conn.execute(
            f"""
SELECT
    entity_key AS bad_key,
    regexp_replace(
        entity_key,
        :bad_key_regex,
        :good_key_replacement
    ) AS good_key,
    COUNT(*) AS row_count
FROM {table_name}
WHERE entity_type = 'stock'
  AND entity_key ~ :bad_key_regex
GROUP BY entity_key, good_key
ORDER BY COUNT(*) DESC, entity_key ASC
""",
            _query_params(),
        )
        .mappings()
        .all()
    )
    return [
        {
            "bad_key": str(row.get("bad_key") or "").strip(),
            "good_key": str(row.get("good_key") or "").strip(),
            "row_count": int(row.get("row_count") or 0),
        }
        for row in rows
    ]


def _count_conflicts(conn: PostgresConnection, *, table_name: str) -> int:
    row = conn.execute(
        _bad_rows_cte(table_name)
        + f"""
SELECT COUNT(*)
FROM bad b
JOIN {table_name} good
  ON good.assertion_id = b.assertion_id
 AND good.entity_key = b.good_key
""",
        _query_params(),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _sample_rows(
    conn: PostgresConnection,
    *,
    table_name: str,
    sample_limit: int,
) -> list[FixSampleRow]:
    rows = (
        conn.execute(
            _bad_rows_cte(table_name)
            + f"""
SELECT
    b.assertion_id,
    b.bad_key,
    b.good_key,
    EXISTS(
        SELECT 1
        FROM {table_name} good
        WHERE good.assertion_id = b.assertion_id
          AND good.entity_key = b.good_key
    ) AS has_conflict
FROM bad b
ORDER BY b.assertion_id ASC, b.bad_key ASC
LIMIT :sample_limit
""",
            {**_query_params(), "sample_limit": max(1, int(sample_limit or 1))},
        )
        .mappings()
        .all()
    )
    return [
        {
            "assertion_id": str(row.get("assertion_id") or "").strip(),
            "bad_key": str(row.get("bad_key") or "").strip(),
            "good_key": str(row.get("good_key") or "").strip(),
            "has_conflict": bool(row.get("has_conflict")),
        }
        for row in rows
    ]


def build_schema_fix_plan(
    conn: PostgresConnection,
    *,
    schema_name: str,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> SchemaFixPlan:
    table_name = _assertion_entities_table(schema_name)
    group_rows = _group_rows(conn, table_name=table_name)
    bad_row_count = _count_bad_rows(conn, table_name=table_name)
    conflict_delete_count = _count_conflicts(conn, table_name=table_name)
    return {
        "schema_name": str(schema_name or "").strip(),
        "bad_key_count": int(len(group_rows)),
        "bad_row_count": int(bad_row_count),
        "conflict_delete_count": int(conflict_delete_count),
        "update_row_count": int(max(bad_row_count - conflict_delete_count, 0)),
        "group_rows": group_rows,
        "sample_rows": _sample_rows(
            conn,
            table_name=table_name,
            sample_limit=sample_limit,
        ),
    }


def _delete_conflicting_bad_rows(
    conn: PostgresConnection,
    *,
    table_name: str,
) -> int:
    result = conn.execute(
        _bad_rows_cte(table_name)
        + f"""
DELETE FROM {table_name} bad_row
USING bad
WHERE bad_row.assertion_id = bad.assertion_id
  AND bad_row.entity_key = bad.bad_key
  AND EXISTS(
      SELECT 1
      FROM {table_name} good
      WHERE good.assertion_id = bad.assertion_id
        AND good.entity_key = bad.good_key
  )
""",
        _query_params(),
    )
    return max(int(result.rowcount), 0)


def _update_bad_rows(conn: PostgresConnection, *, table_name: str) -> int:
    result = conn.execute(
        f"""
UPDATE {table_name}
SET entity_key = regexp_replace(
    entity_key,
    :bad_key_regex,
    :good_key_replacement
)
WHERE entity_type = 'stock'
  AND entity_key ~ :bad_key_regex
""",
        _query_params(),
    )
    return max(int(result.rowcount), 0)


def apply_schema_fix_plan(
    conn: PostgresConnection,
    *,
    schema_name: str,
) -> ApplyStats:
    table_name = _assertion_entities_table(schema_name)
    deleted_conflict_rows = _delete_conflicting_bad_rows(conn, table_name=table_name)
    updated_rows = _update_bad_rows(conn, table_name=table_name)
    remaining_bad_rows = _count_bad_rows(conn, table_name=table_name)
    return {
        "schema_name": str(schema_name or "").strip(),
        "deleted_conflict_rows": int(deleted_conflict_rows),
        "updated_rows": int(updated_rows),
        "remaining_bad_rows": int(remaining_bad_rows),
    }


def _resolve_target_schemas(selected_schema: str) -> list[str]:
    wanted = str(selected_schema or "all").strip().lower()
    if wanted == "all":
        return [*_SOURCE_SCHEMAS]
    if wanted in _SOURCE_SCHEMAS:
        return [wanted]
    raise RuntimeError(f"unsupported_schema:{wanted}")


def _print_plan(plan: SchemaFixPlan) -> None:
    logger.info(
        "[dry-run] schema=%s bad_keys=%s bad_rows=%s conflicts=%s updates=%s",
        plan["schema_name"],
        plan["bad_key_count"],
        plan["bad_row_count"],
        plan["conflict_delete_count"],
        plan["update_row_count"],
    )
    for group_row in plan["group_rows"]:
        logger.info(
            "[dry-run] schema=%s bad_key=%s good_key=%s rows=%s",
            plan["schema_name"],
            group_row["bad_key"],
            group_row["good_key"],
            group_row["row_count"],
        )
    for sample_row in plan["sample_rows"]:
        logger.info(
            "[sample] schema=%s assertion_id=%s bad_key=%s good_key=%s conflict=%s",
            plan["schema_name"],
            sample_row["assertion_id"],
            sample_row["bad_key"],
            sample_row["good_key"],
            int(bool(sample_row["has_conflict"])),
        )


def _print_apply_stats(stats: ApplyStats) -> None:
    logger.info(
        "[apply] schema=%s deleted_conflicts=%s updated=%s remaining_bad=%s",
        stats["schema_name"],
        stats["deleted_conflict_rows"],
        stats["updated_rows"],
        stats["remaining_bad_rows"],
    )


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_present()
    args = parse_args(argv)
    configure_logging(level=args.log_level)
    sources = {
        str(source.schema or "").strip(): source
        for source in load_configured_postgres_sources_from_env()
        if str(source.schema or "").strip() in _SOURCE_SCHEMAS
    }
    target_schemas = _resolve_target_schemas(str(args.schema or "all"))
    for schema_name in target_schemas:
        source = sources.get(schema_name)
        if source is None:
            raise RuntimeError(f"missing_postgres_source:{schema_name}")
        engine = ensure_postgres_engine(source.dsn, schema_name=schema_name)
        try:
            with postgres_connect_autocommit(engine) as conn:
                plan = build_schema_fix_plan(
                    conn,
                    schema_name=schema_name,
                    sample_limit=max(1, int(args.sample_limit or 1)),
                )
                _print_plan(plan)
                if bool(args.apply):
                    stats = apply_schema_fix_plan(conn, schema_name=schema_name)
                    _print_apply_stats(stats)
        finally:
            engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
