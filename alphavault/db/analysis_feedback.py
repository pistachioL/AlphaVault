from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator
from uuid import uuid4

from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    ensure_postgres_engine,
    is_fatal_base_exception,
    postgres_connect_autocommit,
    qualify_postgres_table,
    require_postgres_schema_name,
    run_postgres_transaction,
)
from alphavault.db.postgres_env import (
    load_configured_postgres_sources_from_env,
    require_postgres_source_from_env,
    require_postgres_source_platform,
)
from alphavault.db.turso_queue import CloudPost, load_cloud_post
from alphavault.env import load_dotenv_if_present
from alphavault.rss.utils import now_str
from alphavault.worker.redis_queue import try_get_redis
from alphavault.worker.redis_stream_queue import (
    REDIS_PUSH_STATUS_DUPLICATE,
    REDIS_PUSH_STATUS_ERROR,
    REDIS_PUSH_STATUS_PUSHED,
    redis_try_push_ai_message_status,
    resolve_redis_ai_queue_maxlen,
    resolve_redis_dedup_ttl_seconds,
)
from alphavault.worker.source_runtime import build_source_redis_queue_key

POST_ANALYSIS_FEEDBACK_TABLE_NAME = "post_analysis_feedback"

FEEDBACK_STATUS_PENDING = "pending"
FEEDBACK_STATUS_APPLIED = "applied"
FEEDBACK_STATUS_SUPERSEDED = "superseded"
FEEDBACK_STATUS_QUEUE_FAILED = "queue_failed"

ENTRYPOINT_STOCK_RESEARCH = "stock_research"
ENTRYPOINT_HOMEWORK_TREE = "homework_tree"

_VALID_ENTRYPOINTS = frozenset(
    (
        ENTRYPOINT_STOCK_RESEARCH,
        ENTRYPOINT_HOMEWORK_TREE,
    )
)
_SOURCE_NAMES = frozenset(("weibo", "xueqiu"))


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_entrypoint(value: object) -> str:
    entrypoint = _clean_text(value)
    if entrypoint in _VALID_ENTRYPOINTS:
        return entrypoint
    raise RuntimeError(f"invalid_feedback_entrypoint:{entrypoint}")


def _normalize_post_uid(value: object) -> str:
    post_uid = _clean_text(value)
    if post_uid:
        return post_uid
    raise RuntimeError("missing_feedback_post_uid")


def _feedback_table(engine_or_conn: object) -> str:
    return qualify_postgres_table(
        require_postgres_schema_name(engine_or_conn),
        POST_ANALYSIS_FEEDBACK_TABLE_NAME,
    )


@contextmanager
def _use_conn(
    engine_or_conn: PostgresEngine | PostgresConnection,
) -> Iterator[PostgresConnection]:
    if isinstance(engine_or_conn, PostgresConnection):
        yield engine_or_conn
        return
    with postgres_connect_autocommit(engine_or_conn) as conn:
        yield conn


def _row_to_feedback(row: dict[str, Any] | None) -> dict[str, str] | None:
    if not isinstance(row, dict):
        return None
    return {
        "feedback_id": _clean_text(row.get("feedback_id")),
        "post_uid": _clean_text(row.get("post_uid")),
        "feedback_tag": _clean_text(row.get("feedback_tag")),
        "feedback_note": _clean_text(row.get("feedback_note")),
        "feedback_status": _clean_text(row.get("feedback_status")),
        "entrypoint": _clean_text(row.get("entrypoint")),
        "submitted_at": _clean_text(row.get("submitted_at")),
        "applied_at": _clean_text(row.get("applied_at")),
    }


def _source_engine_for_post_uid(
    post_uid: str,
) -> PostgresEngine:
    load_dotenv_if_present()
    platform = require_postgres_source_platform(post_uid)
    source = require_postgres_source_from_env(platform)
    return ensure_postgres_engine(source.dsn, schema_name=source.schema)


def _load_feedback_redis_runtime(*, source_name: str) -> tuple[Any, str]:
    load_dotenv_if_present()
    redis_client, base_queue_key = try_get_redis()
    source_names = [
        str(source.name or "").strip()
        for source in load_configured_postgres_sources_from_env()
        if str(source.name or "").strip() in _SOURCE_NAMES
    ]
    queue_key = build_source_redis_queue_key(
        base_queue_key=base_queue_key,
        source_name=str(source_name or "").strip(),
        multi_source=len(source_names) > 1,
    )
    return redis_client, queue_key


def _cloud_post_payload(post: CloudPost) -> dict[str, object]:
    return {
        "post_uid": _clean_text(post.post_uid),
        "platform": _clean_text(post.platform),
        "platform_post_id": _clean_text(post.platform_post_id),
        "author": _clean_text(post.author),
        "created_at": _clean_text(post.created_at),
        "url": _clean_text(post.url),
        "raw_text": str(post.raw_text or ""),
        "skip_db_processed_guard": True,
    }


def _mark_pending_feedback_superseded(
    conn: PostgresConnection,
    *,
    post_uid: str,
) -> int:
    result = conn.execute(
        f"""
UPDATE {_feedback_table(conn)}
SET feedback_status = :next_status
WHERE post_uid = :post_uid
  AND feedback_status = :current_status
""",
        {
            "post_uid": _normalize_post_uid(post_uid),
            "current_status": FEEDBACK_STATUS_PENDING,
            "next_status": FEEDBACK_STATUS_SUPERSEDED,
        },
    )
    return int(result.rowcount or 0)


def _insert_feedback_row(
    conn: PostgresConnection,
    *,
    feedback_id: str,
    post_uid: str,
    feedback_tag: str,
    feedback_note: str,
    feedback_status: str,
    entrypoint: str,
    submitted_at: str,
) -> None:
    conn.execute(
        f"""
INSERT INTO {_feedback_table(conn)}(
  feedback_id,
  post_uid,
  feedback_tag,
  feedback_note,
  feedback_status,
  entrypoint,
  submitted_at,
  applied_at
)
VALUES (
  :feedback_id,
  :post_uid,
  :feedback_tag,
  :feedback_note,
  :feedback_status,
  :entrypoint,
  :submitted_at,
  ''
)
""",
        {
            "feedback_id": _clean_text(feedback_id),
            "post_uid": _normalize_post_uid(post_uid),
            "feedback_tag": _clean_text(feedback_tag),
            "feedback_note": _clean_text(feedback_note),
            "feedback_status": _clean_text(feedback_status),
            "entrypoint": _normalize_entrypoint(entrypoint),
            "submitted_at": _clean_text(submitted_at),
        },
    )


def _update_feedback_status(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    feedback_id: str,
    feedback_status: str,
) -> int:
    def _update(conn: PostgresConnection) -> int:
        result = conn.execute(
            f"""
UPDATE {_feedback_table(conn)}
SET feedback_status = :feedback_status
WHERE feedback_id = :feedback_id
""",
            {
                "feedback_id": _clean_text(feedback_id),
                "feedback_status": _clean_text(feedback_status),
            },
        )
        return int(result.rowcount or 0)

    return run_postgres_transaction(engine_or_conn, _update)


def load_latest_pending_feedback(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    post_uid: str,
) -> dict[str, str] | None:
    with _use_conn(engine_or_conn) as conn:
        row = (
            conn.execute(
                f"""
SELECT feedback_id, post_uid, feedback_tag, feedback_note, feedback_status,
       entrypoint, submitted_at, applied_at
FROM {_feedback_table(conn)}
WHERE post_uid = :post_uid
  AND feedback_status = :feedback_status
ORDER BY submitted_at DESC, feedback_id DESC
LIMIT 1
""",
                {
                    "post_uid": _normalize_post_uid(post_uid),
                    "feedback_status": FEEDBACK_STATUS_PENDING,
                },
            )
            .mappings()
            .fetchone()
        )
    return _row_to_feedback(row)


def mark_feedback_applied(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    feedback_id: str,
    applied_at: str,
) -> int:
    def _update(conn: PostgresConnection) -> int:
        result = conn.execute(
            f"""
UPDATE {_feedback_table(conn)}
SET feedback_status = :feedback_status,
    applied_at = :applied_at
WHERE feedback_id = :feedback_id
""",
            {
                "feedback_id": _clean_text(feedback_id),
                "feedback_status": FEEDBACK_STATUS_APPLIED,
                "applied_at": _clean_text(applied_at),
            },
        )
        return int(result.rowcount or 0)

    return run_postgres_transaction(engine_or_conn, _update)


def submit_post_analysis_feedback(
    *,
    post_uid: str,
    feedback_tag: str,
    feedback_note: str,
    entrypoint: str,
) -> dict[str, str]:
    resolved_post_uid = _normalize_post_uid(post_uid)
    resolved_tag = _clean_text(feedback_tag)
    if not resolved_tag:
        raise RuntimeError("missing_feedback_tag")
    resolved_note = _clean_text(feedback_note)
    resolved_entrypoint = _normalize_entrypoint(entrypoint)
    submitted_at = now_str()
    feedback_id = uuid4().hex
    engine_or_conn = _source_engine_for_post_uid(resolved_post_uid)

    def _write(conn: PostgresConnection) -> None:
        _mark_pending_feedback_superseded(conn, post_uid=resolved_post_uid)
        _insert_feedback_row(
            conn,
            feedback_id=feedback_id,
            post_uid=resolved_post_uid,
            feedback_tag=resolved_tag,
            feedback_note=resolved_note,
            feedback_status=FEEDBACK_STATUS_PENDING,
            entrypoint=resolved_entrypoint,
            submitted_at=submitted_at,
        )

    run_postgres_transaction(engine_or_conn, _write)

    queue_status = REDIS_PUSH_STATUS_ERROR
    try:
        post = load_cloud_post(engine_or_conn, resolved_post_uid)
        platform = require_postgres_source_platform(resolved_post_uid)
        redis_client, queue_key = _load_feedback_redis_runtime(source_name=platform)
        queue_status = redis_try_push_ai_message_status(
            redis_client,
            queue_key,
            post_uid=resolved_post_uid,
            payload=_cloud_post_payload(post),
            ttl_seconds=resolve_redis_dedup_ttl_seconds(),
            queue_maxlen=resolve_redis_ai_queue_maxlen(),
            verbose=False,
        )
    except BaseException as err:
        if is_fatal_base_exception(err):
            raise
        queue_status = REDIS_PUSH_STATUS_ERROR

    if queue_status == REDIS_PUSH_STATUS_ERROR:
        _update_feedback_status(
            engine_or_conn,
            feedback_id=feedback_id,
            feedback_status=FEEDBACK_STATUS_QUEUE_FAILED,
        )

    return {
        "feedback_id": feedback_id,
        "feedback_status": (
            FEEDBACK_STATUS_PENDING
            if queue_status in (REDIS_PUSH_STATUS_PUSHED, REDIS_PUSH_STATUS_DUPLICATE)
            else FEEDBACK_STATUS_QUEUE_FAILED
        ),
        "queue_status": queue_status,
    }


__all__ = [
    "ENTRYPOINT_HOMEWORK_TREE",
    "ENTRYPOINT_STOCK_RESEARCH",
    "FEEDBACK_STATUS_APPLIED",
    "FEEDBACK_STATUS_PENDING",
    "FEEDBACK_STATUS_QUEUE_FAILED",
    "FEEDBACK_STATUS_SUPERSEDED",
    "load_latest_pending_feedback",
    "mark_feedback_applied",
    "submit_post_analysis_feedback",
]
