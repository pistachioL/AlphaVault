from __future__ import annotations

import argparse
from collections import defaultdict
import logging

from alphavault.constants import PLATFORM_WEIBO, PLATFORM_XUEQIU
from alphavault.env import load_dotenv_if_present

from alphavault.logging_config import (
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from alphavault.ai.tag_validate import validate_assertion_row
from alphavault.db.postgres_db import PostgresEngine, ensure_postgres_engine
from alphavault.db.postgres_env import (
    require_postgres_source_from_env,
    require_postgres_source_platform,
)
from alphavault.db.sql.scripts import scan_invalid_assertion_rows
from alphavault.db.source_queue import reset_ai_results_for_post_uids
from alphavault.rss.utils import now_str


DEFAULT_CHUNK_SIZE = 200
logger = get_logger(__name__)


def _source_engine_for_platform(platform: str) -> PostgresEngine:
    source = require_postgres_source_from_env(platform)
    return ensure_postgres_engine(source.dsn, schema_name=source.schema)


def _iter_source_engines() -> list[tuple[str, PostgresEngine]]:
    out: list[tuple[str, PostgresEngine]] = []
    for platform in (PLATFORM_WEIBO, PLATFORM_XUEQIU):
        out.append((platform, _source_engine_for_platform(platform)))
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan invalid AI tags in DB, then reset posts to rerun AI."
    )
    parser.add_argument(
        "--prompt-version",
        type=str,
        default="",
        help="只扫描这个 prompt_version（默认扫全部；可选：topic-prompt-v4 / weibo_assertions_v1）",
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="最多 reset 多少个 post（0=不限）"
    )
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="只打印，不写 DB")
    parser.add_argument(
        "--yes", action="store_true", help="非 dry-run 时必须加 --yes 确认"
    )
    add_log_level_argument(parser)
    return parser.parse_args()


def _scan_invalid_post_uids(
    engine,
    *,
    prompt_version: str,
    limit: int,
) -> tuple[list[str], dict[str, str], int]:
    params = {"prompt_version": str(prompt_version or "").strip()}
    query = scan_invalid_assertion_rows(
        filter_prompt_version=bool(params["prompt_version"])
    )

    invalid_post_uids: list[str] = []
    first_error_by_uid: dict[str, str] = {}
    scanned_rows = 0

    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings()
        for row in rows:
            scanned_rows += 1
            uid = str(row.get("post_uid") or "").strip()
            if not uid:
                continue
            if uid in first_error_by_uid:
                continue
            try:
                row_prompt_version = str(row.get("prompt_version") or "").strip()
                validate_assertion_row(dict(row), prompt_version=row_prompt_version)
            except Exception as exc:
                idx = str(row.get("idx") or "").strip()
                first_error_by_uid[uid] = f"idx={idx} {type(exc).__name__}:{exc}"
                invalid_post_uids.append(uid)
                if limit > 0 and len(invalid_post_uids) >= int(limit):
                    break

    if logger.isEnabledFor(logging.DEBUG) and invalid_post_uids:
        for uid in invalid_post_uids[:10]:
            logger.debug(
                "[scan_reset] bad post_uid=%s %s",
                uid,
                first_error_by_uid.get(uid, ""),
            )
        if len(invalid_post_uids) > 10:
            logger.debug(
                "[scan_reset] bad_post_uids_more count=%s",
                len(invalid_post_uids) - 10,
            )

    return invalid_post_uids, first_error_by_uid, scanned_rows


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    configure_logging(level=args.log_level)
    prompt_version = str(args.prompt_version or "").strip()
    if not args.dry_run and not args.yes:
        raise SystemExit("危险操作：非 dry-run 请加 --yes 确认")

    limit = max(0, int(args.limit))
    source_engines = dict(_iter_source_engines())
    invalid_uids: list[str] = []
    scanned_rows = 0
    for engine in source_engines.values():
        remaining = max(0, limit - len(invalid_uids)) if limit > 0 else 0
        source_invalid_uids, _errors, source_scanned_rows = _scan_invalid_post_uids(
            engine,
            prompt_version=prompt_version,
            limit=remaining,
        )
        invalid_uids.extend(source_invalid_uids)
        scanned_rows += int(source_scanned_rows)
        if limit > 0 and len(invalid_uids) >= limit:
            invalid_uids = invalid_uids[:limit]
            break

    logger.info(
        " ".join(
            [
                "[scan_reset] plan",
                f"prompt_version={prompt_version}",
                f"scanned_rows={scanned_rows}",
                f"bad_posts={len(invalid_uids)}",
                f"dry_run={1 if args.dry_run else 0}",
            ]
        )
    )

    if args.dry_run or not invalid_uids:
        return

    archived_at = now_str()
    grouped_uids: dict[str, list[str]] = defaultdict(list)
    for uid in invalid_uids:
        platform = require_postgres_source_platform(uid)
        grouped_uids[platform].append(uid)

    updated = 0
    for platform, source_uids in grouped_uids.items():
        engine = source_engines[platform]
        _deleted, source_updated = reset_ai_results_for_post_uids(
            engine,
            post_uids=source_uids,
            archived_at=archived_at,
            chunk_size=max(1, int(args.chunk_size)),
        )
        updated += int(source_updated)
    logger.info(
        "[scan_reset] done bad_posts=%s updated_posts=%s",
        len(invalid_uids),
        updated,
    )


if __name__ == "__main__":
    main()
