"""
Source queue helpers.

This module stores source queue reads and writes in Postgres source schemas such
as `weibo` and `xueqiu`.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, Optional

from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    is_fatal_base_exception,
    postgres_connect_autocommit,
    qualify_postgres_table,
    require_postgres_schema_name,
    run_postgres_transaction,
)
from alphavault.db.sql import source_queue as source_queue_sql
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.rss.utils import now_str

if TYPE_CHECKING:
    from alphavault.domains.entity_match.resolve import EntityMatchResult


SELECT_POST_PROCESSED_AT = source_queue_sql.SELECT_POST_PROCESSED_AT
UPDATE_POST_DONE = source_queue_sql.UPDATE_POST_DONE

_POSTS_TABLE_NAME = "posts"
_ASSERTIONS_TABLE_NAME = "assertions"
_ASSERTION_MENTIONS_TABLE_NAME = "assertion_mentions"
_ASSERTION_ENTITIES_TABLE_NAME = "assertion_entities"
_POST_CONTEXT_RUNS_TABLE_NAME = "post_context_runs"
_POST_CONTEXT_MENTIONS_TABLE_NAME = "post_context_mentions"
_POST_CONTEXT_ENTITIES_TABLE_NAME = "post_context_entities"


class SourceQueueWriteError(RuntimeError):
    """Raised when source queue write fails with a non-fatal BaseException."""


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


@dataclass(frozen=True)
class PostContextWriteRow:
    post_uid: str
    model: str
    prompt_version: str
    processed_at: str
    mentions: list[dict[str, object]]
    entities: list[dict[str, object]]
    entity_match_result: EntityMatchResult | None = None


def _source_table(engine_or_conn: object, table_name: str) -> str:
    return qualify_postgres_table(
        require_postgres_schema_name(engine_or_conn),
        table_name,
    )


def _posts_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _POSTS_TABLE_NAME)


def _assertions_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _ASSERTIONS_TABLE_NAME)


def _assertion_mentions_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _ASSERTION_MENTIONS_TABLE_NAME)


def _assertion_entities_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _ASSERTION_ENTITIES_TABLE_NAME)


def _post_context_runs_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _POST_CONTEXT_RUNS_TABLE_NAME)


def _post_context_mentions_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _POST_CONTEXT_MENTIONS_TABLE_NAME)


def _post_context_entities_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _POST_CONTEXT_ENTITIES_TABLE_NAME)


@contextmanager
def _use_conn(
    engine_or_conn: PostgresConnection | PostgresEngine,
) -> Iterator[PostgresConnection]:
    if isinstance(engine_or_conn, PostgresConnection):
        yield engine_or_conn
        return
    with postgres_connect_autocommit(engine_or_conn) as conn:
        yield conn


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


def _coerce_float(value: object, *, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return float(default)
    try:
        return float(text)
    except Exception:
        return float(default)


def _coerce_int(value: object, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return int(default)
    try:
        return int(text)
    except Exception:
        return int(default)


def persist_entity_match_followups(
    engine_or_conn: PostgresConnection | PostgresEngine,
    result: "EntityMatchResult",
) -> None:
    from alphavault.domains.entity_match.resolve import (
        persist_entity_match_followups as persist_followups,
    )

    persist_followups(engine_or_conn, result)


def get_research_workbench_engine_from_env() -> PostgresEngine:
    from alphavault.research_workbench.service import (
        get_research_workbench_engine_from_env as load_engine,
    )

    return load_engine()


def persist_entity_match_followups_batch(
    engine_or_conn: PostgresConnection | PostgresEngine,
    results: Iterable["EntityMatchResult"],
) -> None:
    resolved_results = list(results)
    if not resolved_results:
        return

    def _write(conn: PostgresConnection) -> None:
        for result in resolved_results:
            persist_entity_match_followups(conn, result)

    run_postgres_transaction(engine_or_conn, _write)


def _execute_upsert_pending_post(
    conn: PostgresConnection,
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
    conn.execute(
        source_queue_sql.upsert_pending_post_sql(_posts_table(conn)),
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
    conn_or_engine: PostgresConnection | PostgresEngine,
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
    if isinstance(conn_or_engine, PostgresConnection):
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

    try:
        with postgres_connect_autocommit(conn_or_engine) as conn:
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
        raise SourceQueueWriteError("upsert_pending_post_failed") from err


def load_cloud_post(
    engine_or_conn: PostgresConnection | PostgresEngine,
    post_uid: str,
) -> CloudPost:
    with _use_conn(engine_or_conn) as conn:
        row = (
            conn.execute(
                source_queue_sql.select_cloud_post_sql(_posts_table(conn)),
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
            ai_retry_count=_coerce_int(row.get("ai_retry_count")),
        )


def load_post_processed_at(conn: PostgresConnection, *, post_uid: str) -> str | None:
    row = (
        conn.execute(
            source_queue_sql.select_post_processed_at_sql(_posts_table(conn)),
            {"post_uid": str(post_uid or "").strip()},
        )
        .mappings()
        .fetchone()
    )
    if not row:
        return None
    return str(row.get("processed_at") or "")


def is_post_already_processed_success(
    engine_or_conn: PostgresConnection | PostgresEngine,
    *,
    post_uid: str,
) -> bool:
    resolved_post_uid = str(post_uid or "").strip()
    if not resolved_post_uid:
        return False
    with _use_conn(engine_or_conn) as conn:
        row = conn.execute(
            source_queue_sql.select_post_processed_success_sql(_posts_table(conn)),
            {"post_uid": resolved_post_uid},
        ).fetchone()
        return row is not None


def load_unprocessed_post_queue_rows(
    engine: PostgresEngine,
    *,
    limit: int,
    platform: Optional[str] = None,
) -> list[dict[str, object]]:
    resolved_platform = str(platform or "").strip().lower() or None
    query = (
        source_queue_sql.select_unprocessed_post_queue_rows_by_platform_sql(
            _posts_table(engine)
        )
        if resolved_platform
        else source_queue_sql.select_unprocessed_post_queue_rows_sql(
            _posts_table(engine)
        )
    )
    params: dict[str, object] = {"limit": max(0, int(limit))}
    if resolved_platform:
        params["platform"] = resolved_platform
    with postgres_connect_autocommit(engine) as conn:
        rows = conn.execute(query, params).mappings().fetchall()
        return [dict(row) for row in rows if row]


def load_failed_post_queue_rows(
    engine: PostgresEngine,
    *,
    limit: int,
) -> list[dict[str, object]]:
    with postgres_connect_autocommit(engine) as conn:
        rows = (
            conn.execute(
                source_queue_sql.select_failed_post_queue_rows_sql(
                    _posts_table(engine)
                ),
                {"limit": max(0, int(limit))},
            )
            .mappings()
            .fetchall()
        )
        return [dict(row) for row in rows if row]


def _ensure_post_row_exists_for_done(
    conn: PostgresConnection,
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


def _build_assertion_storage_payloads(
    *,
    post_uid: str,
    assertions: Iterable[Dict[str, Any]],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    assertion_payloads: list[dict[str, object]] = []
    mention_payloads: list[dict[str, object]] = []
    entity_payloads: list[dict[str, object]] = []
    for idx, raw_assertion in enumerate(assertions, start=1):
        assertion_id = _make_assertion_id(
            post_uid=post_uid,
            idx=idx,
            raw_assertion=raw_assertion,
        )
        assertion_payloads.append(
            {
                "assertion_id": assertion_id,
                "post_uid": post_uid,
                "idx": int(idx),
                "action": raw_assertion["action"],
                "action_strength": int(raw_assertion["action_strength"]),
                "summary": raw_assertion["summary"],
                "evidence": raw_assertion["evidence"],
            }
        )
        raw_mentions = raw_assertion.get("assertion_mentions")
        mentions = raw_mentions if isinstance(raw_mentions, list) else []
        for mention_seq, raw_mention in enumerate(mentions, start=1):
            if not isinstance(raw_mention, dict):
                continue
            mention_payloads.append(
                {
                    "assertion_id": assertion_id,
                    "mention_seq": int(mention_seq),
                    "mention_text": str(raw_mention.get("mention_text") or "").strip(),
                    "mention_norm": str(
                        raw_mention.get("mention_norm")
                        or raw_mention.get("mention_text")
                        or ""
                    ).strip(),
                    "mention_type": str(raw_mention.get("mention_type") or "").strip(),
                    "evidence": str(raw_mention.get("evidence") or "").strip(),
                    "confidence": _coerce_float(raw_mention.get("confidence")),
                }
            )
        raw_entities = raw_assertion.get("assertion_entities")
        entities = raw_entities if isinstance(raw_entities, list) else []
        for raw_entity in entities:
            if not isinstance(raw_entity, dict):
                continue
            entity_payloads.append(
                {
                    "assertion_id": assertion_id,
                    "entity_key": str(raw_entity.get("entity_key") or "").strip(),
                    "entity_type": str(raw_entity.get("entity_type") or "").strip(),
                    "match_source": str(
                        raw_entity.get("match_source")
                        or raw_entity.get("source_mention_type")
                        or ""
                    ).strip(),
                    "is_primary": _coerce_int(raw_entity.get("is_primary")),
                }
            )
    return assertion_payloads, mention_payloads, entity_payloads


def _build_post_context_storage_payloads(
    *,
    post_uid: str,
    mentions: Iterable[dict[str, object]],
    entities: Iterable[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    mention_payloads: list[dict[str, object]] = []
    entity_payloads: list[dict[str, object]] = []
    for mention_seq, raw_mention in enumerate(mentions, start=1):
        if not isinstance(raw_mention, dict):
            continue
        mention_payloads.append(
            {
                "post_uid": post_uid,
                "mention_seq": int(mention_seq),
                "mention_text": str(raw_mention.get("mention_text") or "").strip(),
                "mention_norm": str(
                    raw_mention.get("mention_norm")
                    or raw_mention.get("mention_text")
                    or ""
                ).strip(),
                "mention_type": str(raw_mention.get("mention_type") or "").strip(),
                "evidence": str(raw_mention.get("evidence") or "").strip(),
                "confidence": _coerce_float(raw_mention.get("confidence")),
            }
        )
    for raw_entity in entities:
        if not isinstance(raw_entity, dict):
            continue
        entity_payloads.append(
            {
                "post_uid": post_uid,
                "entity_key": str(raw_entity.get("entity_key") or "").strip(),
                "entity_type": str(raw_entity.get("entity_type") or "").strip(),
                "match_source": str(
                    raw_entity.get("match_source")
                    or raw_entity.get("source_mention_type")
                    or ""
                ).strip(),
                "is_primary": _coerce_int(raw_entity.get("is_primary")),
            }
        )
    return mention_payloads, entity_payloads


def _replace_post_context_rows(
    conn: PostgresConnection,
    *,
    post_uid: str,
    context_run: dict[str, object] | None,
    context_mentions: Iterable[dict[str, object]] | None,
    context_entities: Iterable[dict[str, object]] | None,
) -> None:
    post_context_runs_table = _post_context_runs_table(conn)
    post_context_mentions_table = _post_context_mentions_table(conn)
    post_context_entities_table = _post_context_entities_table(conn)
    params = {"post_uid": str(post_uid or "").strip()}
    conn.execute(
        source_queue_sql.delete_post_context_entities_by_post_uid_sql(
            post_context_entities_table
        ),
        params,
    )
    conn.execute(
        source_queue_sql.delete_post_context_mentions_by_post_uid_sql(
            post_context_mentions_table
        ),
        params,
    )
    conn.execute(
        source_queue_sql.delete_post_context_runs_by_post_uid_sql(
            post_context_runs_table
        ),
        params,
    )
    if not isinstance(context_run, dict):
        return
    run_payload = {
        "post_uid": params["post_uid"],
        "model": str(context_run.get("model") or "").strip(),
        "prompt_version": str(context_run.get("prompt_version") or "").strip(),
        "processed_at": str(context_run.get("processed_at") or "").strip(),
    }
    conn.execute(
        source_queue_sql.insert_post_context_run_sql(post_context_runs_table),
        run_payload,
    )
    mention_payloads, entity_payloads = _build_post_context_storage_payloads(
        post_uid=params["post_uid"],
        mentions=list(context_mentions or []),
        entities=list(context_entities or []),
    )
    if mention_payloads:
        conn.execute(
            source_queue_sql.insert_post_context_mention_sql(
                post_context_mentions_table
            ),
            mention_payloads,
        )
    if entity_payloads:
        conn.execute(
            source_queue_sql.insert_post_context_entity_sql(
                post_context_entities_table
            ),
            entity_payloads,
        )


def reset_ai_results_all(
    engine: PostgresEngine,
    *,
    archived_at: str,
) -> tuple[int, int]:
    def _reset(conn: PostgresConnection) -> tuple[int, int]:
        posts_table = _posts_table(conn)
        assertions_table = _assertions_table(conn)
        deleted = int(
            conn.execute(
                source_queue_sql.select_assertion_count_all_sql(assertions_table)
            ).scalar()
            or 0
        )
        updated = int(
            conn.execute(
                source_queue_sql.select_post_count_all_sql(posts_table)
            ).scalar()
            or 0
        )
        conn.execute(
            source_queue_sql.delete_assertion_entities_all_sql(
                _assertion_entities_table(conn)
            )
        )
        conn.execute(
            source_queue_sql.delete_assertion_mentions_all_sql(
                _assertion_mentions_table(conn)
            )
        )
        conn.execute(
            source_queue_sql.delete_post_context_entities_all_sql(
                _post_context_entities_table(conn)
            )
        )
        conn.execute(
            source_queue_sql.delete_post_context_mentions_all_sql(
                _post_context_mentions_table(conn)
            )
        )
        conn.execute(
            source_queue_sql.delete_post_context_runs_all_sql(
                _post_context_runs_table(conn)
            )
        )
        conn.execute(source_queue_sql.delete_assertions_all_sql(assertions_table))
        conn.execute(
            source_queue_sql.reset_all_posts_to_pending_sql(posts_table),
            {"archived_at": str(archived_at or "").strip()},
        )
        return deleted, updated

    return run_postgres_transaction(engine, _reset)


def reset_ai_results_for_post_uids(
    engine: PostgresEngine,
    *,
    post_uids: Iterable[str],
    archived_at: str,
    chunk_size: int,
) -> tuple[int, int]:
    chunks = _chunk_post_uids(post_uids, chunk_size=max(1, int(chunk_size)))
    if not chunks:
        return 0, 0
    resolved_archived_at = str(archived_at or "").strip()

    def _reset(conn: PostgresConnection) -> tuple[int, int]:
        posts_table = _posts_table(conn)
        assertions_table = _assertions_table(conn)
        assertion_mentions_table = _assertion_mentions_table(conn)
        assertion_entities_table = _assertion_entities_table(conn)
        post_context_runs_table = _post_context_runs_table(conn)
        post_context_mentions_table = _post_context_mentions_table(conn)
        post_context_entities_table = _post_context_entities_table(conn)
        deleted_total = 0
        updated_total = 0
        for chunk in chunks:
            placeholders = make_in_placeholders(prefix="uid", count=len(chunk))
            params = make_in_params(prefix="uid", values=chunk)
            deleted_total += int(
                conn.execute(
                    source_queue_sql.select_assertion_count_by_post_uids_sql(
                        assertions_table,
                        placeholders,
                    ),
                    params,
                ).scalar()
                or 0
            )
            updated_total += int(
                conn.execute(
                    source_queue_sql.select_post_count_by_post_uids_sql(
                        posts_table,
                        placeholders,
                    ),
                    params,
                ).scalar()
                or 0
            )
            conn.execute(
                source_queue_sql.delete_assertion_entities_by_post_uids_sql(
                    assertion_entities_table,
                    assertions_table,
                    placeholders,
                ),
                params,
            )
            conn.execute(
                source_queue_sql.delete_assertion_mentions_by_post_uids_sql(
                    assertion_mentions_table,
                    assertions_table,
                    placeholders,
                ),
                params,
            )
            conn.execute(
                source_queue_sql.delete_assertions_by_post_uids_sql(
                    assertions_table,
                    placeholders,
                ),
                params,
            )
            conn.execute(
                source_queue_sql.delete_post_context_entities_by_post_uids_sql(
                    post_context_entities_table,
                    placeholders,
                ),
                params,
            )
            conn.execute(
                source_queue_sql.delete_post_context_mentions_by_post_uids_sql(
                    post_context_mentions_table,
                    placeholders,
                ),
                params,
            )
            conn.execute(
                source_queue_sql.delete_post_context_runs_by_post_uids_sql(
                    post_context_runs_table,
                    placeholders,
                ),
                params,
            )
            conn.execute(
                source_queue_sql.reset_posts_to_pending_by_post_uids_sql(
                    posts_table,
                    placeholders,
                ),
                {
                    **params,
                    "archived_at": resolved_archived_at,
                },
            )
        return deleted_total, updated_total

    return run_postgres_transaction(engine, _reset)


def write_post_context_result(
    engine: PostgresConnection | PostgresEngine,
    *,
    post_uid: str,
    model: str,
    prompt_version: str,
    processed_at: str,
    mentions: Iterable[dict[str, object]],
    entities: Iterable[dict[str, object]],
    entity_match_result: "EntityMatchResult" | None = None,
    persist_entity_match_followups: bool = True,
) -> None:
    resolved_entity_match_results = (
        [entity_match_result] if entity_match_result is not None else []
    )

    def _write(conn: PostgresConnection) -> None:
        _replace_post_context_rows(
            conn,
            post_uid=str(post_uid or "").strip(),
            context_run={
                "model": str(model or "").strip(),
                "prompt_version": str(prompt_version or "").strip(),
                "processed_at": str(processed_at or "").strip(),
            },
            context_mentions=list(mentions),
            context_entities=list(entities),
        )

    run_postgres_transaction(engine, _write)
    if persist_entity_match_followups and resolved_entity_match_results:
        persist_entity_match_followups_batch(
            get_research_workbench_engine_from_env(),
            resolved_entity_match_results,
        )


def write_post_context_results_batch(
    engine: PostgresConnection | PostgresEngine,
    *,
    rows: Iterable[PostContextWriteRow],
    persist_entity_match_followups: bool = True,
) -> None:
    resolved_rows = [
        PostContextWriteRow(
            post_uid=str(row.post_uid or "").strip(),
            model=str(row.model or "").strip(),
            prompt_version=str(row.prompt_version or "").strip(),
            processed_at=str(row.processed_at or "").strip(),
            mentions=list(row.mentions),
            entities=list(row.entities),
            entity_match_result=row.entity_match_result,
        )
        for row in rows
        if str(row.post_uid or "").strip()
    ]
    if not resolved_rows:
        return
    resolved_entity_match_results = [
        row.entity_match_result
        for row in resolved_rows
        if row.entity_match_result is not None
    ]

    def _write(conn: PostgresConnection) -> None:
        for row in resolved_rows:
            _replace_post_context_rows(
                conn,
                post_uid=row.post_uid,
                context_run={
                    "model": row.model,
                    "prompt_version": row.prompt_version,
                    "processed_at": row.processed_at,
                },
                context_mentions=row.mentions,
                context_entities=row.entities,
            )

    run_postgres_transaction(engine, _write)
    if persist_entity_match_followups and resolved_entity_match_results:
        persist_entity_match_followups_batch(
            get_research_workbench_engine_from_env(),
            resolved_entity_match_results,
        )


def write_assertions_and_mark_done(
    engine: PostgresConnection | PostgresEngine,
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
    context_run: dict[str, object] | None = None,
    context_mentions: Iterable[dict[str, object]] | None = None,
    context_entities: Iterable[dict[str, object]] | None = None,
    prefetched_post: CloudPost | None = None,
    prefetched_ingested_at: int = 0,
) -> None:
    resolved_entity_match_results = list(entity_match_results or [])

    def _write(conn: PostgresConnection) -> None:
        posts_table = _posts_table(conn)
        assertions_table = _assertions_table(conn)
        assertion_mentions_table = _assertion_mentions_table(conn)
        assertion_entities_table = _assertion_entities_table(conn)
        _ensure_post_row_exists_for_done(
            conn,
            post_uid=str(post_uid or "").strip(),
            archived_at=str(archived_at or "").strip(),
            prefetched_post=prefetched_post,
            prefetched_ingested_at=int(prefetched_ingested_at),
        )
        _replace_post_context_rows(
            conn,
            post_uid=str(post_uid or "").strip(),
            context_run=context_run,
            context_mentions=context_mentions,
            context_entities=context_entities,
        )
        conn.execute(
            source_queue_sql.delete_assertion_entities_by_post_uid_sql(
                assertion_entities_table,
                assertions_table,
            ),
            {"post_uid": post_uid},
        )
        conn.execute(
            source_queue_sql.delete_assertion_mentions_by_post_uid_sql(
                assertion_mentions_table,
                assertions_table,
            ),
            {"post_uid": post_uid},
        )
        conn.execute(
            source_queue_sql.delete_assertions_by_post_uid_sql(assertions_table),
            {"post_uid": post_uid},
        )
        assertion_payloads, mention_payloads, entity_payloads = (
            _build_assertion_storage_payloads(
                post_uid=str(post_uid or "").strip(),
                assertions=assertions,
            )
        )
        if assertion_payloads:
            conn.execute(
                source_queue_sql.insert_assertion_sql(assertions_table),
                assertion_payloads,
            )
        if mention_payloads:
            conn.execute(
                source_queue_sql.insert_assertion_mention_sql(assertion_mentions_table),
                mention_payloads,
            )
        if entity_payloads:
            conn.execute(
                source_queue_sql.insert_assertion_entity_sql(assertion_entities_table),
                entity_payloads,
            )
        conn.execute(
            source_queue_sql.update_post_done_sql(posts_table),
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

    run_postgres_transaction(engine, _write)
    if resolved_entity_match_results:
        persist_entity_match_followups_batch(
            get_research_workbench_engine_from_env(),
            resolved_entity_match_results,
        )


def mark_post_failed(
    engine: PostgresConnection | PostgresEngine,
    *,
    post_uid: str,
    model: str,
    prompt_version: str,
    processed_at: str,
    archived_at: str,
    prefetched_post: CloudPost | None = None,
    prefetched_ingested_at: int = 0,
) -> None:
    write_assertions_and_mark_done(
        engine,
        post_uid=str(post_uid or "").strip(),
        final_status="failed",
        invest_score=None,
        processed_at=str(processed_at or now_str()).strip(),
        model=str(model or "").strip(),
        prompt_version=str(prompt_version or "").strip(),
        archived_at=str(archived_at or now_str()).strip(),
        assertions=[],
        prefetched_post=prefetched_post,
        prefetched_ingested_at=int(prefetched_ingested_at),
    )


__all__ = [
    "CloudPost",
    "PostContextWriteRow",
    "SourceQueueWriteError",
    "get_research_workbench_engine_from_env",
    "is_post_already_processed_success",
    "load_cloud_post",
    "load_failed_post_queue_rows",
    "load_post_processed_at",
    "load_unprocessed_post_queue_rows",
    "mark_post_failed",
    "persist_entity_match_followups",
    "persist_entity_match_followups_batch",
    "reset_ai_results_all",
    "reset_ai_results_for_post_uids",
    "upsert_pending_post",
    "write_assertions_and_mark_done",
    "write_post_context_result",
    "write_post_context_results_batch",
]
