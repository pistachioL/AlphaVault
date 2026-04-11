from __future__ import annotations

import argparse
from contextlib import ExitStack
import os
from typing import Any, Iterable, Iterator

import libsql

from alphavault.constants import (
    ENV_POSTGRES_DSN,
    ENV_WEIBO_TURSO_AUTH_TOKEN,
    ENV_WEIBO_TURSO_DATABASE_URL,
    ENV_XUEQIU_TURSO_AUTH_TOKEN,
    ENV_XUEQIU_TURSO_DATABASE_URL,
    SCHEMA_STANDARD,
    SCHEMA_WEIBO,
    SCHEMA_XUEQIU,
)
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import (
    PostgresConnection,
    ensure_postgres_engine,
    postgres_connect_autocommit,
    qualify_postgres_table,
    run_postgres_transaction,
)
from alphavault.env import load_dotenv_if_present
from alphavault.logging_config import (
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from migrate_standard_history_tables import migrate_standard_history_tables


DEFAULT_BATCH_SIZE = 500
logger = get_logger(__name__)

_SOURCE_TABLE_SPECS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "posts",
        (
            "post_uid",
            "platform",
            "platform_post_id",
            "author",
            "created_at",
            "url",
            "raw_text",
            "final_status",
            "invest_score",
            "processed_at",
            "model",
            "prompt_version",
            "archived_at",
            "ingested_at",
        ),
    ),
    (
        "assertions",
        (
            "assertion_id",
            "post_uid",
            "idx",
            "action",
            "action_strength",
            "summary",
            "evidence",
            "created_at",
        ),
    ),
    (
        "assertion_mentions",
        (
            "assertion_id",
            "mention_seq",
            "mention_text",
            "mention_norm",
            "mention_type",
            "evidence",
            "confidence",
        ),
    ),
    (
        "assertion_entities",
        (
            "assertion_id",
            "entity_key",
            "entity_type",
            "match_source",
            "is_primary",
        ),
    ),
    (
        "topic_clusters",
        (
            "cluster_key",
            "cluster_name",
            "description",
            "created_at",
            "updated_at",
        ),
    ),
    (
        "topic_cluster_topics",
        (
            "topic_key",
            "cluster_key",
            "source",
            "confidence",
            "created_at",
        ),
    ),
    (
        "topic_cluster_post_overrides",
        (
            "post_uid",
            "cluster_key",
            "reason",
            "confidence",
            "created_at",
        ),
    ),
    (
        "entity_page_snapshot",
        (
            "entity_key",
            "entity_type",
            "header_json",
            "signal_top_json",
            "related_json",
            "counters_json",
            "content_hash",
            "updated_at",
        ),
    ),
    (
        "projection_dirty",
        (
            "job_type",
            "target_key",
            "reason_mask",
            "dirty_since",
            "last_dirty_at",
            "claim_until",
            "attempt_count",
            "updated_at",
        ),
    ),
    (
        "worker_cursor",
        (
            "state_key",
            "cursor",
            "updated_at",
        ),
    ),
    (
        "worker_locks",
        (
            "lock_key",
            "locked_until",
            "updated_at",
        ),
    ),
)

_HISTORY_TABLE_SPECS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "relations",
        (
            "relation_id",
            "relation_type",
            "left_key",
            "right_key",
            "relation_label",
            "source",
            "created_at",
            "updated_at",
        ),
    ),
    (
        "relation_candidates",
        (
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
    ),
    (
        "alias_resolve_tasks",
        (
            "alias_key",
            "status",
            "attempt_count",
            "sample_post_uid",
            "sample_evidence",
            "sample_raw_text_excerpt",
            "created_at",
            "updated_at",
        ),
    ),
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy Turso source data into Postgres and merge old history tables"
    )
    parser.add_argument(
        "--turso-weibo-url",
        type=str,
        default=os.getenv(ENV_WEIBO_TURSO_DATABASE_URL, ""),
    )
    parser.add_argument(
        "--turso-weibo-token",
        type=str,
        default=os.getenv(ENV_WEIBO_TURSO_AUTH_TOKEN, ""),
    )
    parser.add_argument(
        "--turso-xueqiu-url",
        type=str,
        default=os.getenv(ENV_XUEQIU_TURSO_DATABASE_URL, ""),
    )
    parser.add_argument(
        "--turso-xueqiu-token",
        type=str,
        default=os.getenv(ENV_XUEQIU_TURSO_AUTH_TOKEN, ""),
    )
    parser.add_argument(
        "--postgres-dsn",
        type=str,
        default=os.getenv(ENV_POSTGRES_DSN, ""),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="每批写多少行（默认 500）",
    )
    add_log_level_argument(parser)
    return parser.parse_args(argv)


def _normalize_batch_size(batch_size: int) -> int:
    return max(1, int(batch_size or 0))


def _require_text(value: str, *, label: str) -> str:
    resolved = str(value or "").strip()
    if resolved:
        return resolved
    raise RuntimeError(f"missing_{label}")


def _normalize_turso_url(url: str) -> str:
    resolved = str(url or "").strip()
    if resolved.startswith("https://"):
        return f"libsql://{resolved[len('https://') :]}"
    return resolved


def _open_turso_connection(database_url: str, auth_token: str = "") -> Any:
    connect_kwargs: dict[str, Any] = {"isolation_level": None}
    token = str(auth_token or "").strip()
    if token:
        connect_kwargs["auth_token"] = token
    return libsql.connect(_normalize_turso_url(database_url), **connect_kwargs)


def _sqlite_table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
SELECT 1
FROM sqlite_schema
WHERE type = ? AND name = ?
LIMIT 1
""",
        ("table", table_name),
    ).fetchone()
    return bool(row)


def _sqlite_table_columns(conn: Any, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {
        str(row[1] or "").strip()
        for row in rows
        if isinstance(row, tuple) and len(row) > 1 and str(row[1] or "").strip()
    }


def _iter_cursor_row_batches_as_mappings(
    cursor: Any, *, batch_size: int
) -> Iterator[list[dict[str, Any]]]:
    description = getattr(cursor, "description", None) or ()
    keys = tuple(
        str(col[0]) if isinstance(col, tuple) else str(getattr(col, "name", ""))
        for col in description
        if col
    )
    size = _normalize_batch_size(batch_size)
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            return
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    key: row[idx] if idx < len(row) else None
                    for idx, key in enumerate(keys)
                }
            )
        yield out


def _load_turso_row_batches(
    conn: Any,
    *,
    table_name: str,
    columns: tuple[str, ...],
    batch_size: int,
) -> tuple[tuple[str, ...], Iterable[list[dict[str, Any]]]]:
    if not _sqlite_table_exists(conn, table_name):
        return (), ()
    existing_columns = _sqlite_table_columns(conn, table_name)
    selected_columns = tuple(column for column in columns if column in existing_columns)
    if not selected_columns:
        return (), ()
    query = (
        f"SELECT {', '.join(selected_columns)} FROM {table_name} "
        f"ORDER BY {selected_columns[0]} ASC"
    )
    cursor = conn.execute(query)
    return selected_columns, _iter_cursor_row_batches_as_mappings(
        cursor,
        batch_size=batch_size,
    )


def _replace_postgres_rows(
    conn: PostgresConnection,
    *,
    schema_name: str,
    table_name: str,
    columns: tuple[str, ...],
    row_batches: Iterable[list[dict[str, Any]]],
) -> int:
    qualified_table = qualify_postgres_table(schema_name, table_name)

    def _write(tx_conn: PostgresConnection) -> int:
        tx_conn.execute(f"DELETE FROM {qualified_table}")
        if not columns:
            return 0
        placeholders = ", ".join(f":{column}" for column in columns)
        insert_sql = (
            f"INSERT INTO {qualified_table} ({', '.join(columns)}) "
            f"VALUES ({placeholders})"
        )
        copied = 0
        for batch_rows in row_batches:
            if not batch_rows:
                continue
            tx_conn.execute(insert_sql, batch_rows)
            copied += len(batch_rows)
        return copied

    return run_postgres_transaction(conn, _write)


def _copy_table_group(
    *,
    source_conn: Any,
    target_conn: PostgresConnection,
    schema_name: str,
    specs: tuple[tuple[str, tuple[str, ...]], ...],
    batch_size: int,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name, columns in specs:
        selected_columns, row_batches = _load_turso_row_batches(
            source_conn,
            table_name=table_name,
            columns=columns,
            batch_size=batch_size,
        )
        counts[table_name] = _replace_postgres_rows(
            target_conn,
            schema_name=schema_name,
            table_name=table_name,
            columns=selected_columns,
            row_batches=row_batches,
        )
        logger.info(
            "[migrate] %s.%s copied=%s",
            schema_name,
            table_name,
            counts[table_name],
        )
    return counts


def _install_target_schemas(
    *,
    weibo_conn: PostgresConnection,
    xueqiu_conn: PostgresConnection,
    standard_conn: PostgresConnection,
) -> None:
    apply_cloud_schema(weibo_conn, target="source", schema_name=SCHEMA_WEIBO)
    apply_cloud_schema(xueqiu_conn, target="source", schema_name=SCHEMA_XUEQIU)
    apply_cloud_schema(weibo_conn, target="standard", schema_name=SCHEMA_WEIBO)
    apply_cloud_schema(xueqiu_conn, target="standard", schema_name=SCHEMA_XUEQIU)
    apply_cloud_schema(
        standard_conn,
        target="standard",
        schema_name=SCHEMA_STANDARD,
    )


def migrate_all(
    *,
    turso_weibo_url: str,
    turso_xueqiu_url: str,
    postgres_dsn: str,
    turso_weibo_token: str = "",
    turso_xueqiu_token: str = "",
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int]:
    normalized_batch_size = _normalize_batch_size(batch_size)
    weibo_url = _require_text(turso_weibo_url, label="turso_weibo_url")
    xueqiu_url = _require_text(turso_xueqiu_url, label="turso_xueqiu_url")
    dsn = _require_text(postgres_dsn, label="postgres_dsn")

    summary: dict[str, int] = {}
    weibo_engine = ensure_postgres_engine(dsn, schema_name=SCHEMA_WEIBO)
    xueqiu_engine = ensure_postgres_engine(dsn, schema_name=SCHEMA_XUEQIU)
    standard_engine = ensure_postgres_engine(dsn, schema_name=SCHEMA_STANDARD)

    logger.info(
        "[start] migrate_turso_to_postgres batch_size=%s",
        normalized_batch_size,
    )

    with ExitStack() as stack:
        stack.callback(weibo_engine.dispose)
        stack.callback(xueqiu_engine.dispose)
        stack.callback(standard_engine.dispose)

        weibo_turso_conn = stack.enter_context(
            _open_turso_connection(weibo_url, turso_weibo_token)
        )
        xueqiu_turso_conn = stack.enter_context(
            _open_turso_connection(xueqiu_url, turso_xueqiu_token)
        )
        weibo_postgres_conn = stack.enter_context(
            postgres_connect_autocommit(weibo_engine)
        )
        xueqiu_postgres_conn = stack.enter_context(
            postgres_connect_autocommit(xueqiu_engine)
        )
        standard_postgres_conn = stack.enter_context(
            postgres_connect_autocommit(standard_engine)
        )

        _install_target_schemas(
            weibo_conn=weibo_postgres_conn,
            xueqiu_conn=xueqiu_postgres_conn,
            standard_conn=standard_postgres_conn,
        )

        weibo_source_counts = _copy_table_group(
            source_conn=weibo_turso_conn,
            target_conn=weibo_postgres_conn,
            schema_name=SCHEMA_WEIBO,
            specs=_SOURCE_TABLE_SPECS,
            batch_size=normalized_batch_size,
        )
        xueqiu_source_counts = _copy_table_group(
            source_conn=xueqiu_turso_conn,
            target_conn=xueqiu_postgres_conn,
            schema_name=SCHEMA_XUEQIU,
            specs=_SOURCE_TABLE_SPECS,
            batch_size=normalized_batch_size,
        )
        _copy_table_group(
            source_conn=weibo_turso_conn,
            target_conn=weibo_postgres_conn,
            schema_name=SCHEMA_WEIBO,
            specs=_HISTORY_TABLE_SPECS,
            batch_size=normalized_batch_size,
        )
        _copy_table_group(
            source_conn=xueqiu_turso_conn,
            target_conn=xueqiu_postgres_conn,
            schema_name=SCHEMA_XUEQIU,
            specs=_HISTORY_TABLE_SPECS,
            batch_size=normalized_batch_size,
        )

        standard_stats = migrate_standard_history_tables(
            weibo_conn=weibo_postgres_conn,
            xueqiu_conn=xueqiu_postgres_conn,
            standard_conn=standard_postgres_conn,
            dry_run=False,
            batch_size=normalized_batch_size,
        )

    for table_name, count in weibo_source_counts.items():
        summary[f"{SCHEMA_WEIBO}_{table_name}"] = int(count)
    for table_name, count in xueqiu_source_counts.items():
        summary[f"{SCHEMA_XUEQIU}_{table_name}"] = int(count)
    for table_name, stats in standard_stats.items():
        summary[f"{SCHEMA_STANDARD}_{table_name}"] = int(
            stats.get("target_count", 0) or 0
        )

    for key in sorted(summary):
        logger.info("[summary] %s=%s", key, summary[key])
    return summary


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_present()
    args = parse_args(argv)
    configure_logging(level=args.log_level)
    migrate_all(
        turso_weibo_url=args.turso_weibo_url,
        turso_xueqiu_url=args.turso_xueqiu_url,
        postgres_dsn=args.postgres_dsn,
        turso_weibo_token=args.turso_weibo_token,
        turso_xueqiu_token=args.turso_xueqiu_token,
        batch_size=args.batch_size,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
