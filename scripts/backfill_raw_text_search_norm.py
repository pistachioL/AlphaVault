from __future__ import annotations

import argparse
from pathlib import Path
import sys

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
from alphavault.db.postgres_env import PostgresSource  # noqa: E402
from alphavault.db.postgres_env import load_configured_postgres_sources_from_env  # noqa: E402
from alphavault.db.sql_rows import read_sql_rows  # noqa: E402
from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.logging_config import add_log_level_argument  # noqa: E402
from alphavault.logging_config import configure_logging  # noqa: E402
from alphavault.logging_config import get_logger  # noqa: E402
from alphavault.search_text import build_sparse_search_text  # noqa: E402

DEFAULT_BATCH_SIZE = 500
SOURCE_SCHEMAS = frozenset((SCHEMA_WEIBO, SCHEMA_XUEQIU))
logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回填 posts.raw_text_search_norm")
    parser.add_argument(
        "--schema",
        choices=(*sorted(SOURCE_SCHEMAS), "all"),
        default="all",
        help="只处理某个 source schema，默认 all",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="每批扫描多少行，默认 500",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="真的写库；默认只做 dry-run",
    )
    add_log_level_argument(parser)
    return parser.parse_args(argv)


def _configured_sources(target_schema: str) -> list[PostgresSource]:
    sources = [
        source
        for source in load_configured_postgres_sources_from_env()
        if source.schema in SOURCE_SCHEMAS
    ]
    if target_schema == "all":
        return sources
    return [source for source in sources if source.schema == target_schema]


def _posts_table(source: PostgresSource) -> str:
    return qualify_postgres_table(source.schema, "posts")


def _scan_rows(
    conn: PostgresConnection,
    *,
    source: PostgresSource,
    after_post_uid: str,
    batch_size: int,
) -> list[dict[str, object]]:
    sql = f"""
SELECT post_uid, raw_text, raw_text_search_norm
FROM {_posts_table(source)}
WHERE post_uid > :after_post_uid
ORDER BY post_uid ASC
LIMIT :limit
"""
    return read_sql_rows(
        conn,
        sql,
        params={
            "after_post_uid": after_post_uid,
            "limit": max(1, int(batch_size)),
        },
    )


def _update_rows(
    conn: PostgresConnection,
    *,
    source: PostgresSource,
    rows: list[dict[str, str]],
) -> int:
    if not rows:
        return 0
    sql = f"""
UPDATE {_posts_table(source)}
SET raw_text_search_norm = :raw_text_search_norm
WHERE post_uid = :post_uid
"""
    conn.execute(sql, rows)
    return len(rows)


def _backfill_source(
    source: PostgresSource,
    *,
    batch_size: int,
    apply: bool,
) -> tuple[int, int]:
    engine = ensure_postgres_engine(source.url, schema_name=source.schema)
    scanned_rows = 0
    updated_rows = 0
    after_post_uid = ""
    with postgres_connect_autocommit(engine) as conn:
        while True:
            rows = _scan_rows(
                conn,
                source=source,
                after_post_uid=after_post_uid,
                batch_size=batch_size,
            )
            if not rows:
                break
            after_post_uid = str(rows[-1].get("post_uid") or "").strip()
            scanned_rows += len(rows)
            update_payload: list[dict[str, str]] = []
            for row in rows:
                post_uid = str(row.get("post_uid") or "").strip()
                if not post_uid:
                    continue
                next_value = build_sparse_search_text(row.get("raw_text"))
                current_value = str(row.get("raw_text_search_norm") or "")
                if next_value == current_value:
                    continue
                update_payload.append(
                    {
                        "post_uid": post_uid,
                        "raw_text_search_norm": next_value,
                    }
                )
            if apply:
                updated_rows += _update_rows(conn, source=source, rows=update_payload)
            else:
                updated_rows += len(update_payload)
            logger.info(
                "schema=%s scanned=%s updated=%s dry_run=%s",
                source.schema,
                scanned_rows,
                updated_rows,
                "0" if apply else "1",
            )
    return scanned_rows, updated_rows


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(level=args.log_level)
    load_dotenv_if_present()
    sources = _configured_sources(args.schema)
    if not sources:
        logger.error("没有可用的 source schema，先检查 POSTGRES_DSN。")
        return 1

    total_scanned = 0
    total_updated = 0
    for source in sources:
        scanned_rows, updated_rows = _backfill_source(
            source,
            batch_size=max(1, int(args.batch_size)),
            apply=bool(args.apply),
        )
        total_scanned += scanned_rows
        total_updated += updated_rows
    logger.info(
        "finished scanned=%s updated=%s dry_run=%s",
        total_scanned,
        total_updated,
        "0" if args.apply else "1",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
