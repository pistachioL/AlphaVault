from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack, contextmanager
from typing import Iterator, TypedDict

from alphavault.constants import PLATFORM_WEIBO, PLATFORM_XUEQIU
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    ensure_postgres_engine,
    postgres_connect_autocommit,
    run_postgres_transaction,
)
from alphavault.db.postgres_env import require_postgres_source_from_env
from alphavault.env import load_dotenv_if_present

_SOURCE_STANDARD = "standard"
_SOURCE_WEIBO = PLATFORM_WEIBO
_SOURCE_XUEQIU = PLATFORM_XUEQIU

_TABLE_RELATIONS = "relations"
_TABLE_RELATION_CANDIDATES = "relation_candidates"
_TABLE_ALIAS_RESOLVE_TASKS = "alias_resolve_tasks"
_DEFAULT_BATCH_SIZE = 500


class _TableSpec(TypedDict):
    key_column: str
    columns: tuple[str, ...]


_TABLE_SPECS: dict[str, _TableSpec] = {
    _TABLE_RELATIONS: {
        "key_column": "relation_id",
        "columns": (
            "relation_id",
            "relation_type",
            "left_key",
            "right_key",
            "relation_label",
            "source",
            "created_at",
            "updated_at",
        ),
    },
    _TABLE_RELATION_CANDIDATES: {
        "key_column": "candidate_id",
        "columns": (
            "candidate_id",
            "relation_type",
            "left_key",
            "right_key",
            "relation_label",
            "suggestion_reason",
            "evidence_summary",
            "score",
            "ai_status",
            "status",
            "created_at",
            "updated_at",
        ),
    },
    _TABLE_ALIAS_RESOLVE_TASKS: {
        "key_column": "alias_key",
        "columns": (
            "alias_key",
            "status",
            "attempt_count",
            "sample_post_uid",
            "sample_evidence",
            "sample_raw_text_excerpt",
            "created_at",
            "updated_at",
        ),
    },
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate old history tables from weibo/xueqiu schemas to standard schema"
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印对帐，不写标准库")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_DEFAULT_BATCH_SIZE,
        help="标准库每批写入多少行（默认 500）",
    )
    return parser.parse_args(argv)


@contextmanager
def _use_conn(
    engine_or_conn: PostgresEngine | PostgresConnection,
) -> Iterator[PostgresConnection]:
    if isinstance(engine_or_conn, PostgresConnection):
        _set_search_path(engine_or_conn)
        yield engine_or_conn
        return
    with postgres_connect_autocommit(engine_or_conn) as conn:
        _set_search_path(conn)
        yield conn


def _load_table_rows(
    conn: PostgresConnection,
    *,
    table_name: str,
    columns: tuple[str, ...],
    key_column: str,
) -> list[dict[str, object]]:
    sql = f"SELECT {', '.join(columns)} FROM {table_name} ORDER BY {key_column} ASC"
    return [dict(row) for row in conn.execute(sql).mappings().all()]


def _print(msg: str) -> None:
    print(msg, flush=True)


def _normalize_batch_size(batch_size: int) -> int:
    return max(1, int(batch_size or 0))


def _batch_count(total_rows: int, batch_size: int) -> int:
    if total_rows <= 0:
        return 0
    return (int(total_rows) + int(batch_size) - 1) // int(batch_size)


def _iter_row_batches(
    rows: list[dict[str, object]],
    *,
    batch_size: int,
) -> Iterator[list[dict[str, object]]]:
    normalized_batch_size = _normalize_batch_size(batch_size)
    for start in range(0, len(rows), normalized_batch_size):
        yield rows[start : start + normalized_batch_size]


@contextmanager
def _use_migration_connections(
    *,
    weibo_conn: PostgresEngine | PostgresConnection,
    xueqiu_conn: PostgresEngine | PostgresConnection,
    standard_conn: PostgresEngine | PostgresConnection,
) -> Iterator[tuple[PostgresConnection, PostgresConnection, PostgresConnection]]:
    with ExitStack() as stack:
        yield (
            stack.enter_context(_use_conn(weibo_conn)),
            stack.enter_context(_use_conn(xueqiu_conn)),
            stack.enter_context(_use_conn(standard_conn)),
        )


def _updated_at_value(row: dict[str, object]) -> str:
    return str(row.get("updated_at") or "").strip()


def _should_replace_row(
    current_row: dict[str, object],
    *,
    current_source: str,
    candidate_row: dict[str, object],
    candidate_source: str,
) -> bool:
    current_updated = _updated_at_value(current_row)
    candidate_updated = _updated_at_value(candidate_row)
    if candidate_updated > current_updated:
        return True
    if candidate_updated < current_updated:
        return False
    if current_source == _SOURCE_STANDARD:
        return False
    if candidate_source == _SOURCE_STANDARD:
        return True
    return False


def _merge_rows(
    *,
    standard_rows: list[dict[str, object]],
    weibo_rows: list[dict[str, object]],
    xueqiu_rows: list[dict[str, object]],
    key_column: str,
) -> tuple[list[dict[str, object]], int]:
    merged: dict[str, tuple[dict[str, object], str]] = {}
    conflict_keys: set[str] = set()

    for source_name, rows in (
        (_SOURCE_STANDARD, standard_rows),
        (_SOURCE_WEIBO, weibo_rows),
        (_SOURCE_XUEQIU, xueqiu_rows),
    ):
        for row in rows:
            key = str(row.get(key_column) or "").strip()
            if not key:
                continue
            current = merged.get(key)
            if current is None:
                merged[key] = (dict(row), source_name)
                continue
            conflict_keys.add(key)
            current_row, current_source = current
            if _should_replace_row(
                current_row,
                current_source=current_source,
                candidate_row=row,
                candidate_source=source_name,
            ):
                merged[key] = (dict(row), source_name)

    out = [payload[0] for payload in merged.values()]
    out.sort(key=lambda item: str(item.get(key_column) or ""))
    return out, len(conflict_keys)


def _replace_table_rows_in_conn(
    conn: PostgresConnection,
    *,
    table_name: str,
    columns: tuple[str, ...],
    rows: list[dict[str, object]],
    batch_size: int,
) -> int:
    insert_sql = (
        f"INSERT INTO {table_name}({', '.join(columns)}) "
        f"VALUES({', '.join(f':{column}' for column in columns)})"
    )
    normalized_batch_size = _normalize_batch_size(batch_size)
    total_rows = len(rows)
    total_batches = _batch_count(total_rows, normalized_batch_size)

    conn.execute(f"DELETE FROM {table_name}")
    for batch_index, batch_rows in enumerate(
        _iter_row_batches(rows, batch_size=normalized_batch_size),
        start=1,
    ):
        _print(
            f"[write-batch] {table_name} "
            f"batch={batch_index}/{total_batches} "
            f"rows={len(batch_rows)}"
        )
        conn.execute(insert_sql, batch_rows)
    _print(
        f"[write-done] {table_name} rows={total_rows} "
        f"batches={total_batches} batch_size={normalized_batch_size}"
    )
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").scalar() or 0)


def _replace_all_table_rows(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    merged_rows_by_table: dict[str, list[dict[str, object]]],
    batch_size: int,
) -> dict[str, int]:
    def _write(conn: PostgresConnection) -> dict[str, int]:
        _set_search_path(conn)
        counts: dict[str, int] = {}
        for table_name, spec in _TABLE_SPECS.items():
            columns: tuple[str, ...] = tuple(spec["columns"])
            counts[table_name] = _replace_table_rows_in_conn(
                conn,
                table_name=table_name,
                columns=columns,
                rows=merged_rows_by_table.get(table_name, []),
                batch_size=batch_size,
            )
        return counts

    return run_postgres_transaction(engine_or_conn, _write)


def _print_group_counts(table_name: str, rows: list[dict[str, object]]) -> None:
    groups: Counter[str] = Counter()
    if table_name == _TABLE_RELATIONS:
        for row in rows:
            group_key = (
                f"{str(row.get('relation_type') or '').strip()}"
                f"|{str(row.get('relation_label') or '').strip()}"
            )
            groups[group_key] += 1
    else:
        for row in rows:
            groups[str(row.get("status") or "").strip()] += 1

    for key in sorted(groups):
        print(
            f"[verify-group] {table_name} key={key} count={groups[key]}",
            flush=True,
        )


def migrate_standard_history_tables(
    *,
    weibo_conn: PostgresEngine | PostgresConnection,
    xueqiu_conn: PostgresEngine | PostgresConnection,
    standard_conn: PostgresEngine | PostgresConnection,
    dry_run: bool,
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> dict[str, dict[str, int]]:
    normalized_batch_size = _normalize_batch_size(batch_size)
    stats: dict[str, dict[str, int]] = {}
    merged_rows_by_table: dict[str, list[dict[str, object]]] = {}

    _print(
        f"[start] migrate_standard_history_tables "
        f"dry_run={1 if dry_run else 0} batch_size={normalized_batch_size}"
    )
    with _use_migration_connections(
        weibo_conn=weibo_conn,
        xueqiu_conn=xueqiu_conn,
        standard_conn=standard_conn,
    ) as (weibo_read_conn, xueqiu_read_conn, standard_read_conn):
        for table_name, spec in _TABLE_SPECS.items():
            columns: tuple[str, ...] = tuple(spec["columns"])
            key_column: str = str(spec["key_column"])

            weibo_rows = _load_table_rows(
                weibo_read_conn,
                table_name=table_name,
                columns=columns,
                key_column=key_column,
            )
            xueqiu_rows = _load_table_rows(
                xueqiu_read_conn,
                table_name=table_name,
                columns=columns,
                key_column=key_column,
            )
            standard_rows = _load_table_rows(
                standard_read_conn,
                table_name=table_name,
                columns=columns,
                key_column=key_column,
            )
            _print(
                f"[load] {table_name} "
                f"weibo_source_count={len(weibo_rows)} "
                f"xueqiu_source_count={len(xueqiu_rows)} "
                f"standard_source_count={len(standard_rows)}"
            )

            merged_rows, conflict_count = _merge_rows(
                standard_rows=standard_rows,
                weibo_rows=weibo_rows,
                xueqiu_rows=xueqiu_rows,
                key_column=key_column,
            )
            merged_rows_by_table[table_name] = merged_rows
            target_before_count = len(standard_rows)
            target_count = len(merged_rows)

            table_stats = {
                "weibo_source_count": len(weibo_rows),
                "xueqiu_source_count": len(xueqiu_rows),
                "standard_source_count": len(standard_rows),
                "merged_unique_count": len(merged_rows),
                "conflict_count": conflict_count,
                "target_before_count": target_before_count,
                "target_count": target_count,
            }
            stats[table_name] = table_stats

        if not dry_run:
            written_counts = _replace_all_table_rows(
                standard_read_conn,
                merged_rows_by_table=merged_rows_by_table,
                batch_size=normalized_batch_size,
            )
            for table_name, target_count in written_counts.items():
                expected_count = len(merged_rows_by_table.get(table_name, []))
                if target_count != expected_count:
                    raise RuntimeError(
                        f"target_count_mismatch:{table_name}:{target_count}:{expected_count}"
                    )
                stats[table_name]["target_count"] = target_count

    for table_name, table_stats in stats.items():
        _print(
            " ".join(
                [
                    f"[verify] {table_name}",
                    f"weibo_source_count={table_stats['weibo_source_count']}",
                    f"xueqiu_source_count={table_stats['xueqiu_source_count']}",
                    f"standard_source_count={table_stats['standard_source_count']}",
                    f"merged_unique_count={table_stats['merged_unique_count']}",
                    f"conflict_count={table_stats['conflict_count']}",
                    f"target_before_count={table_stats['target_before_count']}",
                    f"target_count={table_stats['target_count']}",
                    f"dry_run={1 if dry_run else 0}",
                ]
            ),
        )
        _print_group_counts(table_name, merged_rows_by_table.get(table_name, []))

    return stats


def _set_search_path(conn: PostgresConnection) -> None:
    schema_name = str(getattr(conn, "schema_name", "") or "").strip()
    if not schema_name:
        return
    conn.execute(f"SET search_path TO {schema_name}")


def _build_source_engine(platform: str) -> PostgresEngine:
    source = require_postgres_source_from_env(platform)
    return ensure_postgres_engine(source.dsn, schema_name=source.schema)


def main() -> int:
    load_dotenv_if_present()
    args = parse_args()
    weibo_engine = _build_source_engine(PLATFORM_WEIBO)
    xueqiu_engine = _build_source_engine(PLATFORM_XUEQIU)
    standard_engine = _build_source_engine(_SOURCE_STANDARD)
    migrate_standard_history_tables(
        weibo_conn=weibo_engine,
        xueqiu_conn=xueqiu_engine,
        standard_conn=standard_engine,
        dry_run=bool(args.dry_run),
        batch_size=int(args.batch_size),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
