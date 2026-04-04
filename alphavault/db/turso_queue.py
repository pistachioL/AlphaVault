"""
Turso queue helpers.

This module treats Turso (libsql) as the single source of truth for:
- RSS items (raw_text)
- AI processing state (ai_status / retry fields)
- AI outputs (assertions)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional

from alphavault.db.introspect import table_columns
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql.turso_queue import (
    CREATE_ASSERTION_OUTBOX_TABLE,
    CREATE_IDX_ASSERTION_OUTBOX_CREATED_AT,
    CREATE_IDX_POSTS_AI_STATUS_NEXT_RETRY_AT,
    DELETE_ASSERTION_ENTITIES_BY_POST_UID,
    DELETE_ASSERTION_MENTIONS_BY_POST_UID,
    DELETE_ASSERTIONS_BY_POST_UID,
    INSERT_ASSERTION,
    INSERT_ASSERTION_ENTITY,
    INSERT_ASSERTION_MENTION,
    INSERT_ASSERTION_OUTBOX,
    MARK_AI_ERROR,
    QUEUE_EXTRA_COLUMNS,
    RECOVER_DONE_WITHOUT_PROCESSED_AT,
    RECOVER_DONE_WITHOUT_PROCESSED_AT_BY_PLATFORM,
    RECOVER_STUCK_AI_TASKS,
    RECOVER_STUCK_AI_TASKS_BY_PLATFORM,
    RESET_AI_RESULTS_ALL,
    SELECT_CLOUD_POST,
    SELECT_ASSERTION_OUTBOX_AFTER_ID,
    SELECT_DUE_POST_UIDS,
    SELECT_DUE_POST_UIDS_BY_PLATFORM,
    SELECT_RECENT_POSTS_BY_AUTHOR,
    TRY_MARK_AI_RUNNING,
    UPDATE_POST_DONE,
    UPSERT_PENDING_POST,
    alter_posts_add_column,
    build_reset_ai_results_for_post_uids,
)
from alphavault.db.turso_schema import init_cloud_schema
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_fatal_base_exception,
    maybe_dispose_turso_engine_on_transient_error,
    turso_connect_autocommit,
    turso_savepoint,
)

if TYPE_CHECKING:
    from alphavault.domains.entity_match.resolve import EntityMatchResult


AI_STATUS_PENDING = "pending"


class TursoWriteError(RuntimeError):
    """Raised when Turso write fails with a non-fatal BaseException."""


BASE_POSTS_EXTRA_COLUMNS: list[tuple[str, str]] = [
    (
        "final_status",
        "final_status TEXT NOT NULL DEFAULT 'irrelevant' CHECK (final_status IN ('relevant','irrelevant'))",
    ),
    ("invest_score", "invest_score REAL"),
    ("processed_at", "processed_at TEXT"),
    ("model", "model TEXT"),
    ("prompt_version", "prompt_version TEXT"),
    ("archived_at", "archived_at TEXT NOT NULL DEFAULT ''"),
]


@dataclass(frozen=True)
class CloudPost:
    post_uid: str
    platform: str
    platform_post_id: str
    author: str
    created_at: str
    url: str
    raw_text: str
    display_md: str
    ai_retry_count: int


@dataclass(frozen=True)
class AssertionOutboxEvent:
    id: int
    source: str
    post_uid: str
    author: str
    event_json: str
    created_at: str


def persist_entity_match_followups(
    conn: TursoConnection, result: "EntityMatchResult"
) -> None:
    from alphavault.domains.entity_match.resolve import (
        persist_entity_match_followups as persist_followups,
    )

    persist_followups(conn, result)


def ensure_cloud_queue_schema(engine: TursoEngine, *, verbose: bool) -> None:
    """
    Ensure base schema exists (posts/assertions), then add queue columns to posts.
    """
    init_cloud_schema(engine)

    # Note: keep DDL (ALTER TABLE) outside the atomic write block on libsql/Turso.
    # Some builds may auto-break transactional state around DDL.
    with turso_connect_autocommit(engine) as conn:
        cols = table_columns(conn, "posts")
        for col_name, col_def in BASE_POSTS_EXTRA_COLUMNS:
            if col_name in cols:
                continue
            conn.execute(alter_posts_add_column(col_def))
            cols.add(col_name)
            if verbose:
                print(f"[turso] schema add_column posts.{col_name}", flush=True)
        for col_name, col_def in QUEUE_EXTRA_COLUMNS:
            if col_name in cols:
                continue
            conn.execute(alter_posts_add_column(col_def))
            cols.add(col_name)
            if verbose:
                print(f"[turso] schema add_column posts.{col_name}", flush=True)

        conn.execute(CREATE_IDX_POSTS_AI_STATUS_NEXT_RETRY_AT)
        conn.execute(CREATE_ASSERTION_OUTBOX_TABLE)
        conn.execute(CREATE_IDX_ASSERTION_OUTBOX_CREATED_AT)


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
    display_md: str,
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
            "display_md": display_md,
            "final_status": "irrelevant",
            "archived_at": archived_at,
            "ai_status": AI_STATUS_PENDING,
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
    display_md: str,
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
            display_md=display_md,
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
                display_md=display_md,
                archived_at=archived_at,
                ingested_at=ingested_at,
            )
    except BaseException as err:
        if is_fatal_base_exception(err):
            raise
        maybe_dispose_turso_engine_on_transient_error(engine, err)
        raise TursoWriteError("upsert_pending_post_failed") from err


def select_due_post_uids(
    engine: TursoEngine, *, now_epoch: int, limit: int, platform: Optional[str] = None
) -> list[str]:
    resolved_platform = str(platform or "").strip().lower() or None
    query = (
        SELECT_DUE_POST_UIDS_BY_PLATFORM if resolved_platform else SELECT_DUE_POST_UIDS
    )
    params: dict[str, object] = {
        "now": int(now_epoch),
        "limit": max(0, int(limit)),
    }
    if resolved_platform:
        params["platform"] = resolved_platform
    with turso_connect_autocommit(engine) as conn:
        rows = conn.execute(
            query,
            params,
        ).fetchall()
        return [str(r[0]) for r in rows if r and r[0]]


def try_mark_ai_running(
    engine: TursoEngine,
    *,
    post_uid: str,
    now_epoch: int,
) -> bool:
    with turso_connect_autocommit(engine) as conn:
        res = conn.execute(
            TRY_MARK_AI_RUNNING,
            {"post_uid": post_uid, "now": int(now_epoch)},
        )
        return int(res.rowcount or 0) > 0


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
            display_md=str(row.get("display_md") or ""),
            ai_retry_count=int(row.get("ai_retry_count") or 0),
        )


def load_recent_posts_by_author(
    engine: TursoEngine,
    *,
    author: str,
    limit: int,
) -> list[dict[str, object]]:
    """
    Load recent posts (both processed and unprocessed) for building a thread context.

    Returns mappings with keys:
    - post_uid, platform_post_id, author, created_at, url, raw_text, display_md
    - processed_at, ai_status, ai_retry_count
    """
    resolved_author = str(author or "").strip()
    if not resolved_author:
        return []
    with turso_connect_autocommit(engine) as conn:
        rows = (
            conn.execute(
                SELECT_RECENT_POSTS_BY_AUTHOR,
                {"author": resolved_author, "limit": max(0, int(limit))},
            )
            .mappings()
            .fetchall()
        )
        return [dict(r) for r in rows if r]


def load_assertion_outbox_events(
    engine: TursoEngine,
    *,
    after_id: int,
    limit: int,
) -> list[AssertionOutboxEvent]:
    with turso_connect_autocommit(engine) as conn:
        rows = (
            conn.execute(
                SELECT_ASSERTION_OUTBOX_AFTER_ID,
                {"after_id": max(0, int(after_id)), "limit": max(1, int(limit))},
            )
            .mappings()
            .fetchall()
        )
        out: list[AssertionOutboxEvent] = []
        for row in rows:
            if not row:
                continue
            out.append(
                AssertionOutboxEvent(
                    id=int(row.get("id") or 0),
                    source=str(row.get("source") or "").strip(),
                    post_uid=str(row.get("post_uid") or "").strip(),
                    author=str(row.get("author") or "").strip(),
                    event_json=str(row.get("event_json") or ""),
                    created_at=str(row.get("created_at") or "").strip(),
                )
            )
        return out


def reset_ai_results_all(
    engine: TursoEngine,
    *,
    archived_at: str,
) -> tuple[int, int]:
    """
    Reset all posts back to "pending" (do NOT delete assertions).

    Returns: (deleted_assertions, updated_posts)
    """
    with turso_connect_autocommit(engine) as conn:
        updated = conn.execute(
            RESET_AI_RESULTS_ALL,
            {
                "ai_status": AI_STATUS_PENDING,
                "archived_at": str(archived_at or "").strip(),
            },
        )
        return 0, int(updated.rowcount or 0)


def reset_ai_results_for_post_uids(
    engine: TursoEngine,
    *,
    post_uids: Iterable[str],
    archived_at: str,
    chunk_size: int = 200,
) -> tuple[int, int]:
    """
    Reset specific posts back to "pending" (do NOT delete assertions).

    Returns: (deleted_assertions, updated_posts)
    """
    resolved = []
    seen: set[str] = set()
    for uid in post_uids:
        s = str(uid or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        resolved.append(s)
    if not resolved:
        return 0, 0

    deleted_total = 0
    updated_total = 0
    n = max(1, int(chunk_size))
    for start in range(0, len(resolved), n):
        chunk = resolved[start : start + n]
        placeholders = make_in_placeholders(prefix="uid", count=len(chunk))
        params = make_in_params(prefix="uid", values=chunk)
        params["ai_status"] = AI_STATUS_PENDING
        params["archived_at"] = str(archived_at or "").strip()

        with turso_connect_autocommit(engine) as conn:
            upd_res = conn.execute(
                build_reset_ai_results_for_post_uids(placeholders),
                params,
            )

        updated_total += int(upd_res.rowcount or 0)

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
    ai_result_json: Optional[str],
    assertions: Iterable[Dict[str, Any]],
    entity_match_results: Iterable["EntityMatchResult"] | None = None,
    outbox_source: str = "",
    outbox_author: str = "",
    outbox_event_json: Optional[str] = None,
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
            conn.execute(DELETE_ASSERTION_ENTITIES_BY_POST_UID, {"post_uid": post_uid})
            conn.execute(DELETE_ASSERTION_MENTIONS_BY_POST_UID, {"post_uid": post_uid})
            conn.execute(DELETE_ASSERTIONS_BY_POST_UID, {"post_uid": post_uid})
            assertion_payloads: list[dict[str, object]] = []
            mention_payloads: list[dict[str, object]] = []
            entity_payloads: list[dict[str, object]] = []
            for idx, a in enumerate(assertions, start=1):
                assertion_payloads.append(
                    {
                        "post_uid": post_uid,
                        "idx": int(idx),
                        "speaker": str(a.get("speaker") or "").strip(),
                        "relation_to_topic": str(
                            a.get("relation_to_topic") or "new"
                        ).strip()
                        or "new",
                        "topic_key": a["topic_key"],
                        "action": a["action"],
                        "action_strength": int(a["action_strength"]),
                        "summary": a["summary"],
                        "evidence": a["evidence"],
                        "evidence_refs_json": a.get("evidence_refs_json", "[]"),
                        "confidence": float(a["confidence"]),
                        "stock_codes_json": a.get("stock_codes_json", "[]"),
                        "stock_names_json": a.get("stock_names_json", "[]"),
                        "industries_json": a.get("industries_json", "[]"),
                        "commodities_json": a.get("commodities_json", "[]"),
                        "indices_json": a.get("indices_json", "[]"),
                        "keywords_json": a.get("keywords_json", "[]"),
                    }
                )
                raw_mentions = a.get("assertion_mentions")
                mentions = raw_mentions if isinstance(raw_mentions, list) else []
                for mention_idx, raw_mention in enumerate(mentions, start=1):
                    if not isinstance(raw_mention, dict):
                        continue
                    mention_payloads.append(
                        {
                            "post_uid": post_uid,
                            "assertion_idx": int(idx),
                            "mention_idx": int(mention_idx),
                            "mention_text": str(
                                raw_mention.get("mention_text") or ""
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
                for entity_idx, raw_entity in enumerate(entities, start=1):
                    if not isinstance(raw_entity, dict):
                        continue
                    entity_payloads.append(
                        {
                            "post_uid": post_uid,
                            "assertion_idx": int(idx),
                            "entity_idx": int(entity_idx),
                            "entity_key": str(
                                raw_entity.get("entity_key") or ""
                            ).strip(),
                            "entity_type": str(
                                raw_entity.get("entity_type") or ""
                            ).strip(),
                            "source_mention_text": str(
                                raw_entity.get("source_mention_text") or ""
                            ).strip(),
                            "source_mention_type": str(
                                raw_entity.get("source_mention_type") or ""
                            ).strip(),
                            "confidence": float(raw_entity.get("confidence") or 0.0),
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

            resolved_event_json = str(outbox_event_json or "").strip()
            if resolved_event_json:
                conn.execute(
                    INSERT_ASSERTION_OUTBOX,
                    {
                        "source": str(outbox_source or "").strip(),
                        "post_uid": str(post_uid or "").strip(),
                        "author": str(outbox_author or "").strip(),
                        "event_json": resolved_event_json,
                        "created_at": str(processed_at or "").strip(),
                    },
                )
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
                    "ai_result_json": ai_result_json,
                },
            )


def mark_ai_error(
    engine: TursoEngine,
    *,
    post_uid: str,
    error: str,
    next_retry_at: int,
    archived_at: str,
) -> None:
    msg = (error or "")[:1000]
    with turso_connect_autocommit(engine) as conn:
        conn.execute(
            MARK_AI_ERROR,
            {
                "post_uid": post_uid,
                "error": msg,
                "next_retry_at": int(next_retry_at),
                "archived_at": archived_at,
            },
        )


def recover_stuck_ai_tasks(
    engine: TursoEngine,
    *,
    now_epoch: int,
    stuck_seconds: int,
    platform: Optional[str] = None,
    verbose: bool,
) -> int:
    threshold = int(now_epoch) - max(0, int(stuck_seconds))
    resolved_platform = str(platform or "").strip().lower() or None
    query = (
        RECOVER_STUCK_AI_TASKS_BY_PLATFORM
        if resolved_platform
        else RECOVER_STUCK_AI_TASKS
    )
    params: dict[str, object] = {
        "threshold": threshold,
        "next_retry_at": int(now_epoch) + 60,
    }
    if resolved_platform:
        params["platform"] = resolved_platform
    with turso_connect_autocommit(engine) as conn:
        res = conn.execute(
            query,
            params,
        )
        recovered = int(res.rowcount or 0)
        if recovered and verbose:
            if resolved_platform:
                print(
                    f"[ai] recovered_running={recovered} platform={resolved_platform}",
                    flush=True,
                )
            else:
                print(f"[ai] recovered_running={recovered}", flush=True)
        return recovered


def recover_done_without_processed_at(
    engine: TursoEngine, *, platform: Optional[str] = None, verbose: bool
) -> int:
    """
    Fix inconsistent rows:
    - ai_status='done' but processed_at is NULL/blank

    Such rows will never be picked by the AI scheduler, so we reset them to pending.
    """
    resolved_platform = str(platform or "").strip().lower() or None
    query = (
        RECOVER_DONE_WITHOUT_PROCESSED_AT_BY_PLATFORM
        if resolved_platform
        else RECOVER_DONE_WITHOUT_PROCESSED_AT
    )
    params = {"platform": resolved_platform} if resolved_platform else {}
    with turso_connect_autocommit(engine) as conn:
        res = conn.execute(query, params)
        fixed = int(res.rowcount or 0)
        if fixed and verbose:
            if resolved_platform:
                print(
                    f"[ai] recovered_done_without_processed_at={fixed} platform={resolved_platform}",
                    flush=True,
                )
            else:
                print(f"[ai] recovered_done_without_processed_at={fixed}", flush=True)
        return fixed
