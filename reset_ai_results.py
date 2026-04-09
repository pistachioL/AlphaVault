from __future__ import annotations

import argparse
from collections import defaultdict
import re

from alphavault.constants import PLATFORM_WEIBO, PLATFORM_XUEQIU
from alphavault.db.postgres_db import (
    PostgresEngine,
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import (
    require_postgres_source_platform,
    require_postgres_source_from_env,
)
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql.scripts import (
    SELECT_TOTAL_ASSERTIONS,
    SELECT_TOTAL_POSTS,
    select_post_uids_in,
)
from alphavault.env import load_dotenv_if_present

from alphavault.db.turso_queue import (
    reset_ai_results_all,
    reset_ai_results_for_post_uids,
)
from alphavault.rss.utils import now_str


DEFAULT_CHUNK_SIZE = 200


def _source_engine_for_platform(platform: str) -> PostgresEngine:
    source = require_postgres_source_from_env(platform)
    return ensure_postgres_engine(source.dsn, schema_name=source.schema)


def _iter_source_engines() -> list[tuple[str, PostgresEngine]]:
    return [
        (platform, _source_engine_for_platform(platform))
        for platform in (PLATFORM_WEIBO, PLATFORM_XUEQIU)
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reset AI queue state: set posts back to pending (assertions kept until worker overwrites)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="全量重置（不删 assertions，只把 posts 标记为待处理）",
    )
    parser.add_argument(
        "--post-uids",
        type=str,
        default="",
        help="只重置这些 post_uid（逗号/换行分隔；纯数字会自动加 weibo:）",
    )
    parser.add_argument(
        "--yes", action="store_true", help="配合 --all 使用，确认你真的要全量重置"
    )
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="只打印，不写 DB")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _parse_post_uids(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    items: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,\n]+", raw):
        uid = str(part or "").strip()
        if not uid:
            continue
        if uid.isdigit():
            uid = f"weibo:{uid}"
        if uid in seen:
            continue
        seen.add(uid)
        items.append(uid)
    return items


def _select_existing_post_uids(engine, post_uids: list[str]) -> set[str]:
    if not post_uids:
        return set()
    placeholders = make_in_placeholders(prefix="uid", count=len(post_uids))
    params = make_in_params(prefix="uid", values=post_uids)
    query = select_post_uids_in(placeholders)
    with postgres_connect_autocommit(engine) as conn:
        rows = conn.execute(query, params).fetchall()
    return {str(r[0]) for r in rows if r and r[0]}


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    if args.all and str(args.post_uids or "").strip():
        raise SystemExit("参数冲突：--all 和 --post-uids 只能选一个")
    if not args.all and not str(args.post_uids or "").strip():
        raise SystemExit("缺参数：要么用 --all，要么用 --post-uids")
    if args.all and not args.yes:
        raise SystemExit("全量重置太危险：请加 --yes 确认")

    archived_at = now_str()
    source_engines = dict(_iter_source_engines())

    if args.all:
        total_posts = 0
        total_assertions = 0
        for engine in source_engines.values():
            with postgres_connect_autocommit(engine) as conn:
                total_posts += int(conn.execute(SELECT_TOTAL_POSTS).scalar() or 0)
                total_assertions += int(
                    conn.execute(SELECT_TOTAL_ASSERTIONS).scalar() or 0
                )

        print(
            f"[reset] plan all=1 posts={total_posts} assertions={total_assertions} keep_assertions=1 dry_run={int(bool(args.dry_run))}",
            flush=True,
        )
        if args.dry_run:
            return

        deleted = 0
        updated = 0
        for engine in source_engines.values():
            source_deleted, source_updated = reset_ai_results_all(
                engine,
                archived_at=archived_at,
            )
            deleted += int(source_deleted)
            updated += int(source_updated)
        print(
            f"[reset] done all=1 deleted_assertions={deleted} updated_posts={updated}",
            flush=True,
        )
        return

    post_uids = _parse_post_uids(args.post_uids)
    grouped_targets: dict[str, list[str]] = defaultdict(list)
    for uid in post_uids:
        platform = require_postgres_source_platform(uid)
        grouped_targets[platform].append(uid)

    existing: set[str] = set()
    for platform, group_uids in grouped_targets.items():
        existing.update(
            _select_existing_post_uids(source_engines[platform], group_uids)
        )
    missing = [uid for uid in post_uids if uid not in existing]
    targets = [uid for uid in post_uids if uid in existing]

    print(
        " ".join(
            [
                "[reset] plan",
                "all=0",
                f"target={len(targets)}",
                f"missing={len(missing)}",
                f"dry_run={int(bool(args.dry_run))}",
            ]
        ),
        flush=True,
    )
    if missing and args.verbose:
        head = ", ".join(missing[:10])
        tail = "" if len(missing) <= 10 else f" ... (+{len(missing) - 10})"
        print(f"[reset] missing_post_uids={len(missing)} {head}{tail}", flush=True)

    if args.dry_run or not targets:
        return

    deleted = 0
    updated = 0
    for platform, group_uids in grouped_targets.items():
        source_targets = [uid for uid in group_uids if uid in existing]
        if not source_targets:
            continue
        source_deleted, source_updated = reset_ai_results_for_post_uids(
            source_engines[platform],
            post_uids=source_targets,
            archived_at=archived_at,
            chunk_size=max(1, int(args.chunk_size)),
        )
        deleted += int(source_deleted)
        updated += int(source_updated)
    print(
        f"[reset] done all=0 deleted_assertions={deleted} updated_posts={updated}",
        flush=True,
    )


if __name__ == "__main__":
    main()
