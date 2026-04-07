"""
Turso queue helpers.

This module treats Turso (libsql) as the single source of truth for:
- RSS items (raw_text)
- Final AI outputs (assertions + processed posts)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional

from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql.turso_queue import (
    DELETE_ASSERTION_ENTITIES_ALL,
    DELETE_ASSERTION_MENTIONS_ALL,
    DELETE_ASSERTIONS_ALL,
    DELETE_ASSERTION_ENTITIES_BY_POST_UID,
    DELETE_ASSERTION_MENTIONS_BY_POST_UID,
    DELETE_ASSERTIONS_BY_POST_UID,
    INSERT_ASSERTION,
    INSERT_ASSERTION_ENTITY,
    RESET_ALL_POSTS_TO_PENDING,
    INSERT_ASSERTION_MENTION,
    SELECT_ASSERTION_COUNT_ALL,
    SELECT_POST_PROCESSED_AT,
    SELECT_POST_COUNT_ALL,
    SELECT_CLOUD_POST,
    SELECT_UNPROCESSED_POST_QUEUE_ROWS,
    SELECT_UNPROCESSED_POST_QUEUE_ROWS_BY_PLATFORM,
    UPDATE_POST_DONE,
    UPSERT_PENDING_POST,
    delete_assertion_entities_by_post_uids,
    delete_assertion_mentions_by_post_uids,
    delete_assertions_by_post_uids,
    reset_posts_to_pending_by_post_uids,
    select_assertion_count_by_post_uids,
    select_post_count_by_post_uids,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_fatal_base_exception,
    maybe_dispose_turso_engine_on_transient_error,
    turso_connect_autocommit,
    turso_savepoint,
)
from alphavault.rss.utils import now_str

if TYPE_CHECKING:
    from alphavault.domains.entity_match.resolve import EntityMatchResult


class TursoWriteError(RuntimeError):
    """Raised when Turso write fails with a non-fatal BaseException."""


@dataclass(frozen=True)
class CloudPost:
    post_uid: str
    platform: str
    platform_post_id: str
    author: str
    created_at: str
    url: str
    raw_text: str
    ai_retry_count: int


def _make_assertion_id(
    *, post_uid: str, idx: int, raw_assertion: dict[str, Any]
) -> str:
    resolved_post_uid = str(post_uid or "").strip()
    resolved_idx = int(idx)
    default_assertion_id = f"{resolved_post_uid}#{resolved_idx}"
    raw_assertion_id = str(raw_assertion.get("assertion_id") or "").strip()
    return raw_assertion_id or default_assertion_id


def _chunk_post_uids(post_uids: Iterable[str], *, chunk_size: int) -> list[list[str]]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw_uid in post_uids:
        post_uid = str(raw_uid or "").strip()
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        cleaned.append(post_uid)
    if not cleaned:
        return []
    batch_size = max(1, int(chunk_size))
    return [
        cleaned[idx : idx + batch_size] for idx in range(0, len(cleaned), batch_size)
    ]


def persist_entity_match_followups(
    conn: TursoConnection, result: "EntityMatchResult"
) -> None:
    from alphavault.domains.entity_match.resolve import (
        persist_entity_match_followups as persist_followups,
    )

    persist_followups(conn, result)


def _execute_upsert_pending_post(
    conn: TursoConnection,
    *,
    post_uid: str,
    platform: str,
    platform_post_id: str,
    author: str,
    created_at: str,
    url: str,
    raw_text: str,
    archived_at: str,
    ingested_at: int,
) -> None:
    """
    Insert a new RSS item as a pending AI task.

    NOTE: Cloud posts.final_status is required by existing schema, so we set a placeholder
    final_status='irrelevant' and keep processed_at=NULL until AI is done.
    """
    conn.execute(
        UPSERT_PENDING_POST,
        {
            "post_uid": post_uid,
            "platform": platform,
            "platform_post_id": platform_post_id,
            "author": author,
            "created_at": created_at,
            "url": url,
            "raw_text": raw_text,
            "final_status": "irrelevant",
            "archived_at": archived_at,
            "ingested_at": int(ingested_at),
        },
    )


def upsert_pending_post(
    conn_or_engine: TursoConnection | TursoEngine,
    *,
    post_uid: str,
    platform: str,
    platform_post_id: str,
    author: str,
    created_at: str,
    url: str,
    raw_text: str,
    archived_at: str,
    ingested_at: int,
) -> None:
    """
    Insert a new RSS item as a pending AI task.

    Supports both call styles:
    - upsert_pending_post(conn, ...)
    - upsert_pending_post(engine, ...)
    """
    if isinstance(conn_or_engine, TursoConnection):
        _execute_upsert_pending_post(
            conn_or_engine,
            post_uid=post_uid,
            platform=platform,
            platform_post_id=platform_post_id,
            author=author,
            created_at=created_at,
            url=url,
            raw_text=raw_text,
            archived_at=archived_at,
            ingested_at=ingested_at,
        )
        return

    engine = conn_or_engine
    try:
        with turso_connect_autocommit(engine) as conn:
            _execute_upsert_pending_post(
                conn,
                post_uid=post_uid,
                platform=platform,
                platform_post_id=platform_post_id,
                author=author,
                created_at=created_at,
                url=url,
                raw_text=raw_text,
                archived_at=archived_at,
                ingested_at=ingested_at,
            )
    except BaseException as err:
        if is_fatal_base_exception(err):
            raise
        maybe_dispose_turso_engine_on_transient_error(engine, err)
        raise TursoWriteError("upsert_pending_post_failed") from err


def load_cloud_post(engine: TursoEngine, post_uid: str) -> CloudPost:
    with turso_connect_autocommit(engine) as conn:
        row = (
            conn.execute(
                SELECT_CLOUD_POST,
                {"post_uid": post_uid},
            )
            .mappings()
            .fetchone()
        )
        if not row:
            raise RuntimeError("cloud_post_not_found")
        return CloudPost(
            post_uid=str(row["post_uid"]),
            platform=str(row.get("platform") or "weibo"),
            platform_post_id=str(row.get("platform_post_id") or ""),
            author=str(row.get("author") or ""),
            created_at=str(row.get("created_at") or ""),
            url=str(row.get("url") or ""),
            raw_text=str(row.get("raw_text") or ""),
            ai_retry_count=int(row.get("ai_retry_count") or 0),
        )


def load_post_processed_at(conn: TursoConnection, *, post_uid: str) -> str | None:
    row = (
        conn.execute(
            SELECT_POST_PROCESSED_AT,
            {"post_uid": str(post_uid or "").strip()},
        )
        .mappings()
        .fetchone()
    )
    if not row:
        return None
    return str(row.get("processed_at") or "")


def load_unprocessed_post_queue_rows(
    engine: TursoEngine,
    *,
    limit: int,
    platform: Optional[str] = None,
) -> list[dict[str, object]]:
    resolved_platform = str(platform or "").strip().lower() or None
    query = (
        SELECT_UNPROCESSED_POST_QUEUE_ROWS_BY_PLATFORM
        if resolved_platform
        else SELECT_UNPROCESSED_POST_QUEUE_ROWS
    )
    params: dict[str, object] = {"limit": max(0, int(limit))}
    if resolved_platform:
        params["platform"] = resolved_platform
    with turso_connect_autocommit(engine) as conn:
        rows = conn.execute(query, params).mappings().fetchall()
        return [dict(r) for r in rows if r]


def _ensure_post_row_exists_for_done(
    conn: TursoConnection,
    *,
    post_uid: str,
    archived_at: str,
    prefetched_post: CloudPost | None,
    prefetched_ingested_at: int,
) -> None:
    if prefetched_post is None:
        return
    if load_post_processed_at(conn, post_uid=post_uid) is not None:
        return
    platform = str(prefetched_post.platform or "").strip().lower() or "weibo"
    _execute_upsert_pending_post(
        conn,
        post_uid=str(post_uid or "").strip(),
        platform=platform,
        platform_post_id=str(prefetched_post.platform_post_id or "").strip(),
        author=str(prefetched_post.author or ""),
        created_at=str(prefetched_post.created_at or now_str()),
        url=str(prefetched_post.url or "").strip(),
        raw_text=str(prefetched_post.raw_text or ""),
        archived_at=str(archived_at or now_str()),
        ingested_at=max(0, int(prefetched_ingested_at)),
    )


def reset_ai_results_all(
    engine: TursoEngine,
    *,
    archived_at: str,
) -> tuple[int, int]:
    with turso_connect_autocommit(engine) as conn:
        with turso_savepoint(conn):
            deleted = int(conn.execute(SELECT_ASSERTION_COUNT_ALL).scalar() or 0)
            updated = int(conn.execute(SELECT_POST_COUNT_ALL).scalar() or 0)
            conn.execute(DELETE_ASSERTION_ENTITIES_ALL)
            conn.execute(DELETE_ASSERTION_MENTIONS_ALL)
            conn.execute(DELETE_ASSERTIONS_ALL)
            conn.execute(
                RESET_ALL_POSTS_TO_PENDING,
                {"archived_at": str(archived_at or "").strip()},
            )
    return deleted, updated


def reset_ai_results_for_post_uids(
    engine: TursoEngine,
    *,
    post_uids: Iterable[str],
    archived_at: str,
    chunk_size: int,
) -> tuple[int, int]:
    chunks = _chunk_post_uids(post_uids, chunk_size=max(1, int(chunk_size)))
    if not chunks:
        return 0, 0
    deleted_total = 0
    updated_total = 0
    resolved_archived_at = str(archived_at or "").strip()
    with turso_connect_autocommit(engine) as conn:
        with turso_savepoint(conn):
            for chunk in chunks:
                placeholders = make_in_placeholders(prefix="uid", count=len(chunk))
                params = make_in_params(prefix="uid", values=chunk)
                deleted_total += int(
                    conn.execute(
                        select_assertion_count_by_post_uids(placeholders),
                        params,
                    ).scalar()
                    or 0
                )
                updated_total += int(
                    conn.execute(
                        select_post_count_by_post_uids(placeholders),
                        params,
                    ).scalar()
                    or 0
                )
                conn.execute(
                    delete_assertion_entities_by_post_uids(placeholders), params
                )
                conn.execute(
                    delete_assertion_mentions_by_post_uids(placeholders), params
                )
                conn.execute(delete_assertions_by_post_uids(placeholders), params)
                conn.execute(
                    reset_posts_to_pending_by_post_uids(placeholders),
                    {
                        **params,
                        "archived_at": resolved_archived_at,
                    },
                )
    return deleted_total, updated_total


def write_assertions_and_mark_done(
    engine: TursoEngine,
    *,
    post_uid: str,
    final_status: str,
    invest_score: Optional[float],
    processed_at: str,
    model: str,
    prompt_version: str,
    archived_at: str,
    assertions: Iterable[Dict[str, Any]],
    entity_match_results: Iterable["EntityMatchResult"] | None = None,
    prefetched_post: CloudPost | None = None,
    prefetched_ingested_at: int = 0,
) -> None:
    """
    Commit AI outputs in a single atomic unit, without DBAPI commit/rollback.
    - overwrite assertions for post_uid
    - persist entity-match followups
    - mark posts row as done
    """
    resolved_entity_match_results = list(entity_match_results or [])
    with turso_connect_autocommit(engine) as conn:
        with turso_savepoint(conn):
            _ensure_post_row_exists_for_done(
                conn,
                post_uid=str(post_uid or "").strip(),
                archived_at=str(archived_at or "").strip(),
                prefetched_post=prefetched_post,
                prefetched_ingested_at=int(prefetched_ingested_at),
            )
            conn.execute(DELETE_ASSERTION_ENTITIES_BY_POST_UID, {"post_uid": post_uid})
            conn.execute(DELETE_ASSERTION_MENTIONS_BY_POST_UID, {"post_uid": post_uid})
            conn.execute(DELETE_ASSERTIONS_BY_POST_UID, {"post_uid": post_uid})
            assertion_payloads: list[dict[str, object]] = []
            mention_payloads: list[dict[str, object]] = []
            entity_payloads: list[dict[str, object]] = []
            for idx, a in enumerate(assertions, start=1):
                assertion_id = _make_assertion_id(
                    post_uid=post_uid,
                    idx=idx,
                    raw_assertion=a,
                )
                assertion_payloads.append(
                    {
                        "assertion_id": assertion_id,
                        "post_uid": post_uid,
                        "idx": int(idx),
                        "action": a["action"],
                        "action_strength": int(a["action_strength"]),
                        "summary": a["summary"],
                        "evidence": a["evidence"],
                        "created_at": str(a.get("created_at") or "").strip(),
                    }
                )
                raw_mentions = a.get("assertion_mentions")
                mentions = raw_mentions if isinstance(raw_mentions, list) else []
                for mention_seq, raw_mention in enumerate(mentions, start=1):
                    if not isinstance(raw_mention, dict):
                        continue
                    mention_payloads.append(
                        {
                            "assertion_id": assertion_id,
                            "mention_seq": int(mention_seq),
                            "mention_text": str(
                                raw_mention.get("mention_text") or ""
                            ).strip(),
                            "mention_norm": str(
                                raw_mention.get("mention_norm")
                                or raw_mention.get("mention_text")
                                or ""
                            ).strip(),
                            "mention_type": str(
                                raw_mention.get("mention_type") or ""
                            ).strip(),
                            "evidence": str(raw_mention.get("evidence") or "").strip(),
                            "confidence": float(raw_mention.get("confidence") or 0.0),
                        }
                    )
                raw_entities = a.get("assertion_entities")
                entities = raw_entities if isinstance(raw_entities, list) else []
                for raw_entity in entities:
                    if not isinstance(raw_entity, dict):
                        continue
                    entity_payloads.append(
                        {
                            "assertion_id": assertion_id,
                            "entity_key": str(
                                raw_entity.get("entity_key") or ""
                            ).strip(),
                            "entity_type": str(
                                raw_entity.get("entity_type") or ""
                            ).strip(),
                            "match_source": str(
                                raw_entity.get("match_source")
                                or raw_entity.get("source_mention_type")
                                or ""
                            ).strip(),
                            "is_primary": int(raw_entity.get("is_primary") or 0),
                        }
                    )
            if assertion_payloads:
                conn.execute(INSERT_ASSERTION, assertion_payloads)
            if mention_payloads:
                conn.execute(INSERT_ASSERTION_MENTION, mention_payloads)
            if entity_payloads:
                conn.execute(INSERT_ASSERTION_ENTITY, entity_payloads)
            for match_result in resolved_entity_match_results:
                persist_entity_match_followups(conn, match_result)
            conn.execute(
                UPDATE_POST_DONE,
                {
                    "post_uid": post_uid,
                    "final_status": final_status,
                    "invest_score": invest_score,
                    "processed_at": processed_at,
                    "model": model,
                    "prompt_version": prompt_version,
                    "archived_at": archived_at,
                },
            )
