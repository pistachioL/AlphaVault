from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.constants import PLATFORM_WEIBO  # noqa: E402
from alphavault.db.postgres_db import (  # noqa: E402
    PostgresConnection,
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import require_postgres_source_from_env  # noqa: E402
from alphavault.db.source_queue import persist_entity_match_followups_batch  # noqa: E402
from alphavault.db.sql.common import make_in_params, make_in_placeholders  # noqa: E402
from alphavault.db.sql_rows import read_sql_rows  # noqa: E402
from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.research_workbench.service import (  # noqa: E402
    get_research_workbench_engine_from_env,
)
from alphavault.logging_config import (  # noqa: E402
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from alphavault.worker.post_processor_topic_prompt_v4 import (  # noqa: E402
    resolve_rows_entity_matches,
)

DEFAULT_SOURCE = PLATFORM_WEIBO
_TEXT_EXCERPT_LIMIT = 220
_ASSERTION_ID_BATCH_SIZE = 500
_UTC = timezone.utc
logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回补 entity match followups")
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="source schema，默认 weibo",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--days",
        type=int,
        help="回补最近多少天的数据",
    )
    group.add_argument(
        "--post-uids",
        nargs="+",
        help="只回补指定 post_uid，空格分隔",
    )
    add_log_level_argument(parser)
    return parser.parse_args()


def _clip_text(value: object, *, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, int(limit))].rstrip()


def _clean_post_uids(values: list[str] | None) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        post_uid = str(raw or "").strip()
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        cleaned.append(post_uid)
    return cleaned


def _days_cutoff_str(days: int) -> str:
    window_days = max(1, int(days))
    cutoff = datetime.now(tz=_UTC) - timedelta(days=window_days)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def _chunked(values: list[str], *, size: int) -> list[list[str]]:
    batch_size = max(1, int(size))
    if not values:
        return []
    return [values[idx : idx + batch_size] for idx in range(0, len(values), batch_size)]


def _load_assertion_rows(
    conn: PostgresConnection,
    *,
    days: int | None,
    post_uids: list[str],
) -> list[dict[str, object]]:
    assertions_table = f"{conn.schema_name}.assertions"
    posts_table = f"{conn.schema_name}.posts"
    params: dict[str, object]
    where_clause: str
    if post_uids:
        placeholders = make_in_placeholders(prefix="uid", count=len(post_uids))
        params = make_in_params(prefix="uid", values=post_uids)
        where_clause = f"a.post_uid IN ({placeholders})"
    else:
        params = {"cutoff": _days_cutoff_str(int(days or 0))}
        where_clause = "p.created_at >= :cutoff"
    query = f"""
SELECT
  a.assertion_id,
  a.post_uid,
  a.idx,
  a.evidence,
  COALESCE(p.raw_text, '') AS raw_text,
  COALESCE(p.created_at, '') AS created_at
FROM {assertions_table} a
JOIN {posts_table} p
  ON p.post_uid = a.post_uid
WHERE {where_clause}
ORDER BY p.created_at DESC, a.post_uid ASC, a.idx ASC
"""
    rows = read_sql_rows(conn, query, params=params)
    out: list[dict[str, object]] = []
    for raw_row in rows:
        row = dict(raw_row)
        row["assertion_id"] = str(row.get("assertion_id") or "").strip()
        row["post_uid"] = str(row.get("post_uid") or "").strip()
        row["evidence"] = str(row.get("evidence") or "").strip()
        row["raw_text"] = str(row.get("raw_text") or "").strip()
        row["created_at"] = str(row.get("created_at") or "").strip()
        out.append(row)
    return out


def _load_mentions_by_assertion_id(
    conn: PostgresConnection,
    assertion_ids: list[str],
) -> dict[str, list[dict[str, object]]]:
    mentions_table = f"{conn.schema_name}.assertion_mentions"
    out: dict[str, list[dict[str, object]]] = {}
    for chunk in _chunked(assertion_ids, size=_ASSERTION_ID_BATCH_SIZE):
        placeholders = make_in_placeholders(prefix="aid", count=len(chunk))
        params = make_in_params(prefix="aid", values=chunk)
        query = f"""
SELECT assertion_id, mention_seq, mention_text, mention_type, confidence
FROM {mentions_table}
WHERE assertion_id IN ({placeholders})
ORDER BY assertion_id ASC, mention_seq ASC
"""
        rows = read_sql_rows(conn, query, params=params)
        for row in rows:
            assertion_id = str(row.get("assertion_id") or "").strip()
            mention_text = str(row.get("mention_text") or "").strip()
            mention_type = str(row.get("mention_type") or "").strip()
            if not assertion_id or not mention_text or not mention_type:
                continue
            out.setdefault(assertion_id, []).append(
                {
                    "mention_text": mention_text,
                    "mention_type": mention_type,
                    "confidence": row.get("confidence"),
                }
            )
    return out


def _build_rows_by_post_uid(
    assertion_rows: list[dict[str, object]],
    mentions_by_assertion_id: dict[str, list[dict[str, object]]],
) -> dict[str, list[dict[str, object]]]:
    rows_by_post_uid: dict[str, list[dict[str, object]]] = {}
    for row in assertion_rows:
        assertion_id = str(row.get("assertion_id") or "").strip()
        post_uid = str(row.get("post_uid") or "").strip()
        mentions = mentions_by_assertion_id.get(assertion_id, [])
        if not assertion_id or not post_uid or not mentions:
            continue
        raw_text = str(row.get("raw_text") or "").strip()
        evidence = str(row.get("evidence") or "").strip()
        rows_by_post_uid.setdefault(post_uid, []).append(
            {
                "assertion_mentions": mentions,
                "evidence": evidence,
                "source_text_excerpt": _clip_text(
                    raw_text or evidence,
                    limit=_TEXT_EXCERPT_LIMIT,
                ),
            }
        )
    return rows_by_post_uid


def main() -> int:
    load_dotenv_if_present()
    args = parse_args()
    configure_logging(level=args.log_level)

    cleaned_post_uids = _clean_post_uids(args.post_uids)
    if args.days is not None and int(args.days) <= 0:
        raise RuntimeError("days_must_be_positive")
    if args.post_uids is not None and not cleaned_post_uids:
        raise RuntimeError("missing_post_uids")

    source_engine = None
    standard_engine = None
    try:
        source = require_postgres_source_from_env(args.source)
        source_engine = ensure_postgres_engine(source.dsn, schema_name=source.schema)
        standard_engine = get_research_workbench_engine_from_env()

        with postgres_connect_autocommit(source_engine) as source_conn:
            assertion_rows = _load_assertion_rows(
                source_conn,
                days=args.days,
                post_uids=cleaned_post_uids,
            )
            if not assertion_rows:
                logger.info(
                    "[entity_match_backfill] source=%s assertions=0 nothing_to_do",
                    source.name,
                )
                return 0
            assertion_ids = [
                str(row.get("assertion_id") or "").strip()
                for row in assertion_rows
                if str(row.get("assertion_id") or "").strip()
            ]
            mentions_by_assertion_id = _load_mentions_by_assertion_id(
                source_conn,
                assertion_ids,
            )

        rows_by_post_uid = _build_rows_by_post_uid(
            assertion_rows, mentions_by_assertion_id
        )
        if not rows_by_post_uid:
            logger.info(
                "[entity_match_backfill] source=%s assertions=%s mention_rows=0 nothing_to_do",
                source.name,
                len(assertion_rows),
            )
            return 0

        followups_by_post_uid = resolve_rows_entity_matches(
            source_engine, rows_by_post_uid
        )
        results = [
            result
            for post_results in followups_by_post_uid.values()
            for result in post_results
            if result.relation_candidates or result.alias_task_keys
        ]
        if not results:
            logger.info(
                "[entity_match_backfill] source=%s posts=%s assertions=%s followups=0 nothing_to_do",
                source.name,
                len(rows_by_post_uid),
                len(assertion_rows),
            )
            return 0

        persist_entity_match_followups_batch(standard_engine, results)
        candidate_count = sum(len(result.relation_candidates) for result in results)
        alias_task_count = sum(len(result.alias_task_keys) for result in results)
        logger.info(
            "[entity_match_backfill] source=%s posts=%s assertions=%s results=%s candidates=%s alias_tasks=%s",
            source.name,
            len(rows_by_post_uid),
            len(assertion_rows),
            len(results),
            candidate_count,
            alias_task_count,
        )
        return 0
    finally:
        if standard_engine is not None:
            standard_engine.dispose()
        if source_engine is not None and source_engine is not standard_engine:
            source_engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
