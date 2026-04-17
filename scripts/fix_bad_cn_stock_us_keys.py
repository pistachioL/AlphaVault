from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys
from typing import Callable, TypedDict

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.constants import SCHEMA_STANDARD, SCHEMA_WEIBO, SCHEMA_XUEQIU  # noqa: E402
from alphavault.db.postgres_db import (  # noqa: E402
    PostgresConnection,
    ensure_postgres_engine,
    postgres_connect_autocommit,
    qualify_postgres_table,
)
from alphavault.db.postgres_env import load_configured_postgres_sources_from_env  # noqa: E402
from alphavault.domains.relation.ids import (  # noqa: E402
    make_candidate_id,
    make_relation_id,
)
from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.logging_config import (  # noqa: E402
    add_log_level_argument,
    configure_logging,
    get_logger,
)

BAD_CN_STOCK_US_KEY_REGEX = r"^stock:(SH|SZ|BJ)(\d{6})\.US$"
BAD_CN_STOCK_US_KEY_REPLACEMENT = r"stock:\2.\1"
DEFAULT_SAMPLE_LIMIT = 20
_BAD_CN_STOCK_US_KEY_RE = re.compile(BAD_CN_STOCK_US_KEY_REGEX)
_SOURCE_SCHEMAS = (SCHEMA_WEIBO, SCHEMA_XUEQIU)
_TARGET_SCHEMAS = (*_SOURCE_SCHEMAS, SCHEMA_STANDARD)
logger = get_logger(__name__)


class FixPlanRow(TypedDict):
    bad_key: str
    good_key: str
    row_count: int


class FixSampleRow(TypedDict):
    row_id: str
    bad_key: str
    good_key: str
    has_conflict: bool


class TableFixPlan(TypedDict):
    schema_name: str
    table_name: str
    bad_key_count: int
    bad_row_count: int
    conflict_delete_count: int
    update_row_count: int
    group_rows: list[FixPlanRow]
    sample_rows: list[FixSampleRow]


class TableApplyStats(TypedDict):
    schema_name: str
    table_name: str
    deleted_conflict_rows: int
    updated_rows: int
    remaining_bad_rows: int


class StandardFixRow(TypedDict):
    row_id: str
    next_row_id: str
    relation_type: str
    relation_label: str
    bad_left_key: str
    good_left_key: str
    bad_right_key: str
    good_right_key: str
    has_conflict: bool


class StandardTableSpec(TypedDict):
    table_name: str
    id_column: str
    id_builder: Callable[..., str]


_STANDARD_TABLE_SPECS: tuple[StandardTableSpec, ...] = (
    {
        "table_name": "relation_candidates",
        "id_column": "candidate_id",
        "id_builder": make_candidate_id,
    },
    {
        "table_name": "relations",
        "id_column": "relation_id",
        "id_builder": make_relation_id,
    },
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检查并修复 CN 假 .US 股票键")
    parser.add_argument(
        "--schema",
        choices=(*_TARGET_SCHEMAS, "all"),
        default="all",
        help="只看某个 schema，默认 all",
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


def _query_params() -> dict[str, str]:
    return {
        "bad_key_regex": BAD_CN_STOCK_US_KEY_REGEX,
        "good_key_replacement": BAD_CN_STOCK_US_KEY_REPLACEMENT,
    }


def _replace_bad_key(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return _BAD_CN_STOCK_US_KEY_RE.sub(BAD_CN_STOCK_US_KEY_REPLACEMENT, text)


def _format_key_summary(*, left_key: str, right_key: str = "") -> str:
    if right_key:
        return f"left={left_key} right={right_key}"
    return left_key


def _assertion_entities_table(schema_name: str) -> str:
    return qualify_postgres_table(schema_name, "assertion_entities")


def _source_bad_rows_cte(table_name: str) -> str:
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


def _count_source_bad_rows(conn: PostgresConnection, *, table_name: str) -> int:
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


def _group_source_rows(
    conn: PostgresConnection, *, table_name: str
) -> list[FixPlanRow]:
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


def _count_source_conflicts(conn: PostgresConnection, *, table_name: str) -> int:
    row = conn.execute(
        _source_bad_rows_cte(table_name)
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


def _sample_source_rows(
    conn: PostgresConnection,
    *,
    table_name: str,
    sample_limit: int,
) -> list[FixSampleRow]:
    rows = (
        conn.execute(
            _source_bad_rows_cte(table_name)
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
            "row_id": str(row.get("assertion_id") or "").strip(),
            "bad_key": str(row.get("bad_key") or "").strip(),
            "good_key": str(row.get("good_key") or "").strip(),
            "has_conflict": bool(row.get("has_conflict")),
        }
        for row in rows
    ]


def build_source_fix_plan(
    conn: PostgresConnection,
    *,
    schema_name: str,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> TableFixPlan:
    table_name = "assertion_entities"
    qualified_table = _assertion_entities_table(schema_name)
    group_rows = _group_source_rows(conn, table_name=qualified_table)
    bad_row_count = _count_source_bad_rows(conn, table_name=qualified_table)
    conflict_delete_count = _count_source_conflicts(conn, table_name=qualified_table)
    return {
        "schema_name": str(schema_name or "").strip(),
        "table_name": table_name,
        "bad_key_count": int(len(group_rows)),
        "bad_row_count": int(bad_row_count),
        "conflict_delete_count": int(conflict_delete_count),
        "update_row_count": int(max(bad_row_count - conflict_delete_count, 0)),
        "group_rows": group_rows,
        "sample_rows": _sample_source_rows(
            conn,
            table_name=qualified_table,
            sample_limit=sample_limit,
        ),
    }


def apply_source_fix_plan(
    conn: PostgresConnection,
    *,
    schema_name: str,
) -> TableApplyStats:
    qualified_table = _assertion_entities_table(schema_name)
    deleted_conflict_rows = conn.execute(
        _source_bad_rows_cte(qualified_table)
        + f"""
DELETE FROM {qualified_table} bad_row
USING bad
WHERE bad_row.assertion_id = bad.assertion_id
  AND bad_row.entity_key = bad.bad_key
  AND EXISTS(
      SELECT 1
      FROM {qualified_table} good
      WHERE good.assertion_id = bad.assertion_id
        AND good.entity_key = bad.good_key
  )
""",
        _query_params(),
    )
    updated_rows = conn.execute(
        f"""
UPDATE {qualified_table}
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
    remaining_bad_rows = _count_source_bad_rows(conn, table_name=qualified_table)
    return {
        "schema_name": str(schema_name or "").strip(),
        "table_name": "assertion_entities",
        "deleted_conflict_rows": max(int(deleted_conflict_rows.rowcount), 0),
        "updated_rows": max(int(updated_rows.rowcount), 0),
        "remaining_bad_rows": int(remaining_bad_rows),
    }


def _standard_table_name(schema_name: str, table_name: str) -> str:
    return qualify_postgres_table(schema_name, table_name)


def _count_standard_bad_rows(
    conn: PostgresConnection,
    *,
    table_name: str,
) -> int:
    row = conn.execute(
        f"""
SELECT COUNT(*)
FROM {table_name}
WHERE left_key ~ :bad_key_regex
   OR right_key ~ :bad_key_regex
""",
        _query_params(),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _standard_row_exists(
    conn: PostgresConnection,
    *,
    table_name: str,
    id_column: str,
    row_id: str,
) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {table_name} WHERE {id_column} = :row_id LIMIT 1",
        {"row_id": row_id},
    ).fetchone()
    return bool(row)


def _load_standard_fix_rows(
    conn: PostgresConnection,
    *,
    table_name: str,
    id_column: str,
    id_builder: Callable[..., str],
) -> list[StandardFixRow]:
    raw_rows = (
        conn.execute(
            f"""
SELECT
    {id_column} AS row_id,
    relation_type,
    left_key,
    right_key,
    relation_label
FROM {table_name}
WHERE left_key ~ :bad_key_regex
   OR right_key ~ :bad_key_regex
ORDER BY {id_column} ASC
""",
            _query_params(),
        )
        .mappings()
        .all()
    )
    rows: list[StandardFixRow] = []
    for raw_row in raw_rows:
        row_id = str(raw_row.get("row_id") or "").strip()
        relation_type = str(raw_row.get("relation_type") or "").strip()
        relation_label = str(raw_row.get("relation_label") or "").strip()
        bad_left_key = str(raw_row.get("left_key") or "").strip()
        bad_right_key = str(raw_row.get("right_key") or "").strip()
        good_left_key = _replace_bad_key(bad_left_key)
        good_right_key = _replace_bad_key(bad_right_key)
        next_row_id = id_builder(
            relation_type=relation_type,
            left_key=good_left_key,
            right_key=good_right_key,
            relation_label=relation_label,
        )
        rows.append(
            {
                "row_id": row_id,
                "next_row_id": next_row_id,
                "relation_type": relation_type,
                "relation_label": relation_label,
                "bad_left_key": bad_left_key,
                "good_left_key": good_left_key,
                "bad_right_key": bad_right_key,
                "good_right_key": good_right_key,
                "has_conflict": False,
            }
        )
    if not rows:
        return []

    good_id_counts: dict[str, int] = {}
    keep_row_by_good_id: dict[str, str] = {}
    for fix_row in rows:
        next_row_id = fix_row["next_row_id"]
        good_id_counts[next_row_id] = int(good_id_counts.get(next_row_id, 0)) + 1
        previous_keep = keep_row_by_good_id.get(next_row_id, "")
        if not previous_keep or fix_row["row_id"] < previous_keep:
            keep_row_by_good_id[next_row_id] = fix_row["row_id"]

    for fix_row in rows:
        next_row_id = fix_row["next_row_id"]
        duplicate_conflict = good_id_counts.get(next_row_id, 0) > 1 and fix_row[
            "row_id"
        ] != keep_row_by_good_id.get(next_row_id, "")
        existing_conflict = next_row_id != fix_row["row_id"] and _standard_row_exists(
            conn,
            table_name=table_name,
            id_column=id_column,
            row_id=next_row_id,
        )
        fix_row["has_conflict"] = bool(duplicate_conflict or existing_conflict)
    return rows


def _group_standard_rows(rows: list[StandardFixRow]) -> list[FixPlanRow]:
    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        bad_key = _format_key_summary(
            left_key=row["bad_left_key"],
            right_key=row["bad_right_key"],
        )
        good_key = _format_key_summary(
            left_key=row["good_left_key"],
            right_key=row["good_right_key"],
        )
        key = (bad_key, good_key)
        counts[key] = int(counts.get(key, 0)) + 1
    grouped: list[FixPlanRow] = []
    for (bad_key, good_key), row_count in counts.items():
        grouped.append(
            {
                "bad_key": bad_key,
                "good_key": good_key,
                "row_count": row_count,
            }
        )
    grouped.sort(key=lambda plan_row: (-plan_row["row_count"], plan_row["bad_key"]))
    return grouped


def _sample_standard_rows(
    rows: list[StandardFixRow],
    *,
    sample_limit: int,
) -> list[FixSampleRow]:
    out: list[FixSampleRow] = []
    for row in rows[: max(1, int(sample_limit or 1))]:
        out.append(
            {
                "row_id": row["row_id"],
                "bad_key": _format_key_summary(
                    left_key=row["bad_left_key"],
                    right_key=row["bad_right_key"],
                ),
                "good_key": _format_key_summary(
                    left_key=row["good_left_key"],
                    right_key=row["good_right_key"],
                ),
                "has_conflict": bool(row["has_conflict"]),
            }
        )
    return out


def build_standard_fix_plan(
    conn: PostgresConnection,
    *,
    schema_name: str,
    table_name: str,
    id_column: str,
    id_builder: Callable[..., str],
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> TableFixPlan:
    qualified_table = _standard_table_name(schema_name, table_name)
    rows = _load_standard_fix_rows(
        conn,
        table_name=qualified_table,
        id_column=id_column,
        id_builder=id_builder,
    )
    conflict_delete_count = sum(1 for row in rows if row["has_conflict"])
    bad_row_count = len(rows)
    return {
        "schema_name": str(schema_name or "").strip(),
        "table_name": table_name,
        "bad_key_count": int(len(_group_standard_rows(rows))),
        "bad_row_count": int(bad_row_count),
        "conflict_delete_count": int(conflict_delete_count),
        "update_row_count": int(max(bad_row_count - conflict_delete_count, 0)),
        "group_rows": _group_standard_rows(rows),
        "sample_rows": _sample_standard_rows(rows, sample_limit=sample_limit),
    }


def apply_standard_fix_plan(
    conn: PostgresConnection,
    *,
    schema_name: str,
    table_name: str,
    id_column: str,
    id_builder: Callable[..., str],
) -> TableApplyStats:
    qualified_table = _standard_table_name(schema_name, table_name)
    rows = _load_standard_fix_rows(
        conn,
        table_name=qualified_table,
        id_column=id_column,
        id_builder=id_builder,
    )
    deleted_conflict_rows = 0
    updated_rows = 0
    for row in rows:
        if row["has_conflict"]:
            result = conn.execute(
                f"DELETE FROM {qualified_table} WHERE {id_column} = :row_id",
                {"row_id": row["row_id"]},
            )
            deleted_conflict_rows += max(int(result.rowcount), 0)
            continue
        result = conn.execute(
            f"""
UPDATE {qualified_table}
SET {id_column} = :next_row_id,
    left_key = :good_left_key,
    right_key = :good_right_key
WHERE {id_column} = :row_id
""",
            {
                "row_id": row["row_id"],
                "next_row_id": row["next_row_id"],
                "good_left_key": row["good_left_key"],
                "good_right_key": row["good_right_key"],
            },
        )
        updated_rows += max(int(result.rowcount), 0)
    remaining_bad_rows = _count_standard_bad_rows(conn, table_name=qualified_table)
    return {
        "schema_name": str(schema_name or "").strip(),
        "table_name": table_name,
        "deleted_conflict_rows": int(deleted_conflict_rows),
        "updated_rows": int(updated_rows),
        "remaining_bad_rows": int(remaining_bad_rows),
    }


def _resolve_target_schemas(selected_schema: str) -> list[str]:
    wanted = str(selected_schema or "all").strip().lower()
    if wanted == "all":
        return [*_TARGET_SCHEMAS]
    if wanted in _TARGET_SCHEMAS:
        return [wanted]
    raise RuntimeError(f"unsupported_schema:{wanted}")


def _print_plan(plan: TableFixPlan) -> None:
    logger.info(
        "[dry-run] schema=%s table=%s bad_keys=%s bad_rows=%s conflicts=%s updates=%s",
        plan["schema_name"],
        plan["table_name"],
        plan["bad_key_count"],
        plan["bad_row_count"],
        plan["conflict_delete_count"],
        plan["update_row_count"],
    )
    for group_row in plan["group_rows"]:
        logger.info(
            "[dry-run] schema=%s table=%s bad_key=%s good_key=%s rows=%s",
            plan["schema_name"],
            plan["table_name"],
            group_row["bad_key"],
            group_row["good_key"],
            group_row["row_count"],
        )
    for sample_row in plan["sample_rows"]:
        logger.info(
            "[sample] schema=%s table=%s row_id=%s bad_key=%s good_key=%s conflict=%s",
            plan["schema_name"],
            plan["table_name"],
            sample_row["row_id"],
            sample_row["bad_key"],
            sample_row["good_key"],
            int(bool(sample_row["has_conflict"])),
        )


def _print_apply_stats(stats: TableApplyStats) -> None:
    logger.info(
        "[apply] schema=%s table=%s deleted_conflicts=%s updated=%s remaining_bad=%s",
        stats["schema_name"],
        stats["table_name"],
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
        if str(source.schema or "").strip() in _TARGET_SCHEMAS
    }
    target_schemas = _resolve_target_schemas(str(args.schema or "all"))
    sample_limit = max(1, int(args.sample_limit or 1))
    for schema_name in target_schemas:
        source = sources.get(schema_name)
        if source is None:
            raise RuntimeError(f"missing_postgres_source:{schema_name}")
        engine = ensure_postgres_engine(source.dsn, schema_name=schema_name)
        try:
            with postgres_connect_autocommit(engine) as conn:
                if schema_name in _SOURCE_SCHEMAS:
                    plan = build_source_fix_plan(
                        conn,
                        schema_name=schema_name,
                        sample_limit=sample_limit,
                    )
                    _print_plan(plan)
                    if bool(args.apply):
                        stats = apply_source_fix_plan(conn, schema_name=schema_name)
                        _print_apply_stats(stats)
                    continue

                for spec in _STANDARD_TABLE_SPECS:
                    plan = build_standard_fix_plan(
                        conn,
                        schema_name=schema_name,
                        table_name=spec["table_name"],
                        id_column=spec["id_column"],
                        id_builder=spec["id_builder"],
                        sample_limit=sample_limit,
                    )
                    _print_plan(plan)
                    if bool(args.apply):
                        stats = apply_standard_fix_plan(
                            conn,
                            schema_name=schema_name,
                            table_name=spec["table_name"],
                            id_column=spec["id_column"],
                            id_builder=spec["id_builder"],
                        )
                        _print_apply_stats(stats)
        finally:
            engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
