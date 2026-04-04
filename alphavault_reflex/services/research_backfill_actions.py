from __future__ import annotations

import os
from datetime import datetime

from alphavault.constants import DATETIME_FMT, ENV_AI_MODEL
from alphavault.db.sql.turso_db import (
    SELECT_ASSERTION_ENTITIES_FOR_POST_UID,
    SELECT_ASSERTION_MENTIONS_FOR_POST_UID,
    SELECT_ASSERTIONS_FOR_POST_UID,
)
from alphavault.db.turso_db import ensure_turso_engine, turso_connect_autocommit
from alphavault.db.turso_env import (
    infer_platform_from_post_uid,
    require_turso_source_from_env,
)
from alphavault.db.turso_queue import (
    ensure_cloud_queue_schema,
    reset_ai_results_for_post_uids,
    write_assertions_and_mark_done,
)
from alphavault.env import load_dotenv_if_present
from alphavault.research_backfill_cache import mark_stock_backfill_dirty_from_assertions
from alphavault.research_stock_cache import mark_stock_dirty
from alphavault_reflex.services.stock_backfill import (
    BACKFILL_PROMPT_VERSION,
    merge_post_assertions,
    run_targeted_stock_backfill,
)
from alphavault_reflex.services.turso_read import load_sources_from_env

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def get_turso_engine_for_post_uid(post_uid: str):
    load_dotenv_if_present()
    platform = infer_platform_from_post_uid(post_uid)
    if not platform:
        raise RuntimeError("unknown_post_platform")
    source = require_turso_source_from_env(platform)
    return ensure_turso_engine(source.url, source.token)


def queue_post_for_ai_backfill(post_uid: str) -> None:
    target = str(post_uid or "").strip()
    if not target:
        return
    engine = get_turso_engine_for_post_uid(target)
    ensure_cloud_queue_schema(engine, verbose=False)
    archived_at = datetime.now().strftime(DATETIME_FMT)
    reset_ai_results_for_post_uids(
        engine,
        post_uids=[target],
        archived_at=archived_at,
        chunk_size=1,
    )


def run_direct_stock_backfill(post_uid: str, stock_key: str, display_name: str) -> int:
    target_post_uid = str(post_uid or "").strip()
    target_stock_key = str(stock_key or "").strip()
    if not target_post_uid or not target_stock_key:
        return 0
    posts, _assertions, err = load_sources_from_env()
    if err:
        raise RuntimeError(err)
    if posts.empty:
        raise RuntimeError("posts_empty")
    matched = posts[posts["post_uid"].astype(str).str.strip() == target_post_uid]
    if matched.empty:
        raise RuntimeError("post_not_found")
    post_row = {
        str(key): str(value or "").strip()
        for key, value in matched.iloc[0].to_dict().items()
    }
    new_assertions = run_targeted_stock_backfill(
        post_row,
        stock_key=target_stock_key,
        display_name=display_name,
    )
    if not new_assertions:
        return 0
    engine = get_turso_engine_for_post_uid(target_post_uid)
    ensure_cloud_queue_schema(engine, verbose=False)
    existing_assertions = load_assertions_for_post(engine, post_uid=target_post_uid)
    merged = merge_post_assertions(existing_assertions, new_assertions)
    archived_at = datetime.now().strftime(DATETIME_FMT)
    write_assertions_and_mark_done(
        engine,
        post_uid=target_post_uid,
        final_status="relevant",
        invest_score=1.0,
        processed_at=archived_at,
        model=os.getenv(ENV_AI_MODEL, "").strip() or "targeted-stock-backfill",
        prompt_version=BACKFILL_PROMPT_VERSION,
        archived_at=archived_at,
        ai_result_json=None,
        assertions=merged,
    )
    mark_stock_dirty(
        engine,
        stock_key=target_stock_key,
        reason="direct_backfill",
    )
    mark_stock_backfill_dirty_from_assertions(
        engine,
        assertions=merged,
        reason="direct_backfill",
    )
    return max(0, len(merged) - len(existing_assertions))


def load_assertions_for_post(engine, *, post_uid: str) -> list[dict[str, object]]:
    target = str(post_uid or "").strip()
    if not target:
        return []
    try:
        with turso_connect_autocommit(engine) as conn:
            assertion_rows = (
                conn.execute(
                    SELECT_ASSERTIONS_FOR_POST_UID,
                    {"post_uid": target},
                )
                .mappings()
                .all()
            )
            mention_rows = (
                conn.execute(
                    SELECT_ASSERTION_MENTIONS_FOR_POST_UID,
                    {"post_uid": target},
                )
                .mappings()
                .all()
            )
            entity_rows = (
                conn.execute(
                    SELECT_ASSERTION_ENTITIES_FOR_POST_UID,
                    {"post_uid": target},
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        return []
    mentions_by_idx: dict[int, list[dict[str, object]]] = {}
    for row in mention_rows:
        idx = int(row.get("assertion_idx") or 0)
        if idx <= 0:
            continue
        mentions_by_idx.setdefault(idx, []).append(
            {
                "mention_text": str(row.get("mention_text") or "").strip(),
                "mention_type": str(row.get("mention_type") or "").strip(),
                "evidence": str(row.get("evidence") or "").strip(),
                "confidence": float(row.get("confidence") or 0),
            }
        )
    entities_by_idx: dict[int, list[dict[str, object]]] = {}
    for row in entity_rows:
        idx = int(row.get("assertion_idx") or 0)
        if idx <= 0:
            continue
        entities_by_idx.setdefault(idx, []).append(
            {
                "entity_key": str(row.get("entity_key") or "").strip(),
                "entity_type": str(row.get("entity_type") or "").strip(),
                "source_mention_text": str(
                    row.get("source_mention_text") or ""
                ).strip(),
                "source_mention_type": str(
                    row.get("source_mention_type") or ""
                ).strip(),
                "confidence": float(row.get("confidence") or 0),
            }
        )
    out: list[dict[str, object]] = []
    for row in assertion_rows:
        idx = int(row.get("idx") or 0)
        out.append(
            {
                "speaker": str(row.get("speaker") or "").strip(),
                "relation_to_topic": str(row.get("relation_to_topic") or "new").strip()
                or "new",
                "topic_key": str(row.get("topic_key") or "").strip(),
                "action": str(row.get("action") or "").strip(),
                "action_strength": int(row.get("action_strength") or 0),
                "summary": str(row.get("summary") or "").strip(),
                "evidence": str(row.get("evidence") or "").strip(),
                "evidence_refs_json": str(row.get("evidence_refs_json") or "[]"),
                "confidence": float(row.get("confidence") or 0),
                "stock_codes_json": str(row.get("stock_codes_json") or "[]"),
                "stock_names_json": str(row.get("stock_names_json") or "[]"),
                "industries_json": str(row.get("industries_json") or "[]"),
                "commodities_json": str(row.get("commodities_json") or "[]"),
                "indices_json": str(row.get("indices_json") or "[]"),
                "keywords_json": str(row.get("keywords_json") or "[]"),
                "assertion_mentions": list(mentions_by_idx.get(idx) or []),
                "assertion_entities": list(entities_by_idx.get(idx) or []),
            }
        )
    return out


__all__ = [
    "get_turso_engine_for_post_uid",
    "load_assertions_for_post",
    "queue_post_for_ai_backfill",
    "run_direct_stock_backfill",
]
