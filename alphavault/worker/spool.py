from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from alphavault.logging_config import get_logger
from alphavault.constants import (
    DEFAULT_SPOOL_DIR,
    ENV_SPOOL_DIR,
    PLATFORM_WEIBO,
)
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    postgres_connect_autocommit,
)
from alphavault.db.turso_queue import load_post_processed_at, upsert_pending_post
from alphavault.rss.utils import now_str

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
SPOOL_JSON_SUFFIX = ".json"
SPOOL_PROCESSING_SUFFIX = ".processing"
SPOOL_RETRY_MARKER = ".retry."
SPOOL_FILE_GLOB = f"*{SPOOL_JSON_SUFFIX}"
SPOOL_PROCESSING_GLOB = f"*{SPOOL_JSON_SUFFIX}{SPOOL_PROCESSING_SUFFIX}"
SPOOL_PROCESSING_STALE_SECONDS = 300
SPOOL_RETRY_LINK_ATTEMPTS = 16
logger = get_logger(__name__)


def sha1_short(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:20]


def ensure_spool_dir() -> Path:
    value = os.getenv(ENV_SPOOL_DIR, "").strip() or DEFAULT_SPOOL_DIR
    path = Path(value)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error("[spool] dir_error %s %s: %s", path, type(e).__name__, e)
    return path


def _spool_path(spool_dir: Path, post_uid: str) -> Path:
    return spool_dir / f"{sha1_short(post_uid)}{SPOOL_JSON_SUFFIX}"


def spool_write(spool_dir: Path, post_uid: str, payload: Dict[str, Any]) -> Path:
    path = _spool_path(spool_dir, post_uid)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)
    return path


def spool_delete(spool_dir: Path, post_uid: str) -> None:
    path = _spool_path(spool_dir, post_uid)
    try:
        path.unlink(missing_ok=True)
    except Exception:
        return


def _spool_processing_path(path: Path) -> Path:
    return Path(f"{path}{SPOOL_PROCESSING_SUFFIX}")


def _spool_path_from_processing(path: Path) -> Optional[Path]:
    name = path.name
    expected_suffix = f"{SPOOL_JSON_SUFFIX}{SPOOL_PROCESSING_SUFFIX}"
    if not name.endswith(expected_suffix):
        return None
    return path.with_name(name[: -len(SPOOL_PROCESSING_SUFFIX)])


def _cleanup_spool_file(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except Exception:
        return


def _build_retry_spool_path(*, target_path: Path, seed: str, attempt: int) -> Path:
    name = target_path.name
    if name.endswith(SPOOL_JSON_SUFFIX):
        stem = name[: -len(SPOOL_JSON_SUFFIX)]
    else:
        stem = name
    retry_name = f"{stem}{SPOOL_RETRY_MARKER}{seed}.{attempt}{SPOOL_JSON_SUFFIX}"
    return target_path.with_name(retry_name)


def _claim_spool_file(path: Path) -> Optional[Path]:
    claimed_path = _spool_processing_path(path)
    try:
        path.rename(claimed_path)
    except FileNotFoundError:
        return None
    try:
        os.utime(claimed_path, None)
    except Exception:
        pass
    return claimed_path


def _restore_claimed_file_to_retry_slot(
    *, claimed_path: Path, target_path: Path
) -> None:
    seed = f"{os.getpid()}.{time.time_ns()}"
    for attempt in range(int(SPOOL_RETRY_LINK_ATTEMPTS)):
        retry_path = _build_retry_spool_path(
            target_path=target_path,
            seed=seed,
            attempt=attempt,
        )
        try:
            os.link(claimed_path, retry_path)
        except FileExistsError:
            continue
        except FileNotFoundError:
            return
        except Exception:
            return
        _cleanup_spool_file(claimed_path)
        return


def _restore_claimed_file_for_retry(*, claimed_path: Path, target_path: Path) -> None:
    try:
        os.link(claimed_path, target_path)
    except FileExistsError:
        _restore_claimed_file_to_retry_slot(
            claimed_path=claimed_path,
            target_path=target_path,
        )
    except FileNotFoundError:
        return
    except Exception:
        if target_path.exists():
            _restore_claimed_file_to_retry_slot(
                claimed_path=claimed_path,
                target_path=target_path,
            )
        return
    _cleanup_spool_file(claimed_path)


def _recover_stale_processing_files(*, spool_dir: Path) -> int:
    now_epoch = int(time.time())
    stale_before = now_epoch - max(1, int(SPOOL_PROCESSING_STALE_SECONDS))
    recovered = 0
    for processing_path in sorted(spool_dir.glob(SPOOL_PROCESSING_GLOB)):
        target_path = _spool_path_from_processing(processing_path)
        if target_path is None:
            continue
        try:
            st = processing_path.stat()
        except FileNotFoundError:
            continue
        except Exception:
            raise
        if int(st.st_mtime) > stale_before:
            continue
        if target_path.exists():
            continue
        try:
            processing_path.rename(target_path)
            recovered += 1
        except FileNotFoundError:
            continue
        except Exception:
            raise
    if recovered > 0:
        logger.info("[spool] recover_stale_processing recovered=%s", recovered)
    return recovered


def _maybe_dispose_turso_engine_on_transient_error(
    *, engine: PostgresEngine, err: BaseException
) -> None:
    del engine, err


def _load_claimed_payload(*, claimed_path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(claimed_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.warning(
            "[spool] bad_file %s %s: %s",
            claimed_path.name,
            type(e).__name__,
            e,
        )
        _cleanup_spool_file(claimed_path)
        return None
    if not isinstance(payload, dict):
        _cleanup_spool_file(claimed_path)
        return None
    return payload


def _upsert_spool_payload(
    conn: PostgresConnection,
    *,
    payload: dict[str, Any],
) -> str:
    post_uid = str(payload.get("post_uid") or "")
    if not post_uid:
        return ""
    platform = str(payload.get("platform") or PLATFORM_WEIBO).strip().lower()
    platform = platform or PLATFORM_WEIBO
    raw_text = str(payload.get("raw_text") or "")
    author = str(payload.get("author") or "")
    upsert_pending_post(
        conn,
        post_uid=post_uid,
        platform=platform,
        platform_post_id=str(payload.get("platform_post_id") or ""),
        author=author,
        created_at=str(payload.get("created_at") or now_str()),
        url=str(payload.get("url") or ""),
        raw_text=raw_text,
        archived_at=now_str(),
        ingested_at=int(payload.get("ingested_at") or int(time.time())),
    )
    return post_uid


def _try_push_payload_to_ai_ready(
    *,
    redis_client: Any,
    redis_queue_key: str,
    post_uid: str,
    payload: dict[str, Any],
) -> tuple[int, bool]:
    status = _try_push_payload_to_ai_ready_status(
        redis_client=redis_client,
        redis_queue_key=redis_queue_key,
        post_uid=post_uid,
        payload=payload,
    )
    if status == "error":
        return 0, True
    if status == "pushed":
        return 1, False
    return 0, False


def _try_push_payload_to_ai_ready_status(
    *,
    redis_client: Any,
    redis_queue_key: str,
    post_uid: str,
    payload: dict[str, Any],
) -> str:
    if not redis_client or not str(redis_queue_key or "").strip() or not post_uid:
        return "error"
    try:
        from alphavault.worker.redis_stream_queue import (
            REDIS_PUSH_STATUS_DUPLICATE,
            REDIS_PUSH_STATUS_ERROR,
            REDIS_PUSH_STATUS_PUSHED,
            redis_try_push_ai_message_status,
            resolve_redis_ai_queue_maxlen,
            resolve_redis_dedup_ttl_seconds,
        )

        status = redis_try_push_ai_message_status(
            redis_client,
            str(redis_queue_key),
            post_uid=post_uid,
            payload=payload,
            ttl_seconds=resolve_redis_dedup_ttl_seconds(),
            queue_maxlen=resolve_redis_ai_queue_maxlen(),
        )
    except Exception as e:
        logger.warning("[redis] ai_requeue_error %s: %s", type(e).__name__, e)
        return "error"
    if status == REDIS_PUSH_STATUS_PUSHED:
        return REDIS_PUSH_STATUS_PUSHED
    if status == REDIS_PUSH_STATUS_DUPLICATE:
        return REDIS_PUSH_STATUS_DUPLICATE
    if status == REDIS_PUSH_STATUS_ERROR:
        return REDIS_PUSH_STATUS_ERROR
    return "error"


def flush_spool_to_turso(
    *,
    spool_dir: Path,
    engine: Optional[PostgresEngine],
    max_items: int,
    redis_client=None,
    redis_queue_key: str = "",
    delete_spool_on_redis_push: bool = False,
) -> Tuple[int, bool]:
    if engine is None:
        return 0, False
    max_batch = max(0, int(max_items))
    if max_batch <= 0:
        return 0, False
    processed = 0
    try:
        _recover_stale_processing_files(spool_dir=spool_dir)
        paths = sorted(spool_dir.glob(SPOOL_FILE_GLOB))
        if not paths:
            return 0, False
        with postgres_connect_autocommit(engine) as conn:
            for path in paths[:max_batch]:
                claimed_path = _claim_spool_file(path)
                if claimed_path is None:
                    continue
                payload = _load_claimed_payload(
                    claimed_path=claimed_path,
                )
                if payload is None:
                    continue

                post_uid = str(payload.get("post_uid") or "")
                if not post_uid:
                    _cleanup_spool_file(claimed_path)
                    continue

                try:
                    _upsert_spool_payload(conn, payload=payload)
                except BaseException as e:
                    if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                        raise
                    _maybe_dispose_turso_engine_on_transient_error(engine=engine, err=e)
                    pushed = False
                    if redis_client and redis_queue_key:
                        try:
                            from alphavault.constants import (
                                DEFAULT_REDIS_DEDUP_TTL_SECONDS,
                            )
                            from alphavault.worker.redis_stream_queue import (
                                REDIS_PUSH_STATUS_PUSHED,
                                redis_try_push_ai_message_status,
                                resolve_redis_ai_queue_maxlen,
                            )

                            status = redis_try_push_ai_message_status(
                                redis_client,
                                redis_queue_key,
                                post_uid=post_uid,
                                payload=payload,
                                ttl_seconds=DEFAULT_REDIS_DEDUP_TTL_SECONDS,
                                queue_maxlen=resolve_redis_ai_queue_maxlen(),
                            )
                            pushed = status == REDIS_PUSH_STATUS_PUSHED
                        except Exception:
                            pushed = False
                    if pushed:
                        if delete_spool_on_redis_push:
                            _cleanup_spool_file(claimed_path)
                        else:
                            _restore_claimed_file_for_retry(
                                claimed_path=claimed_path,
                                target_path=path,
                            )
                        processed += 1
                        logger.info("[spool] moved_to_redis %s", path.name)
                        continue
                    logger.warning(
                        "[spool] turso_write_error %s %s: %s",
                        path.name,
                        type(e).__name__,
                        e,
                    )
                    _restore_claimed_file_for_retry(
                        claimed_path=claimed_path,
                        target_path=path,
                    )
                    return processed, True

                _restore_claimed_file_for_retry(
                    claimed_path=claimed_path,
                    target_path=path,
                )
                processed += 1
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(engine=engine, err=e)
        logger.warning("[spool] turso_connect_error %s: %s", type(e).__name__, e)
        return processed, True

    return processed, False


def recover_spool_to_turso_and_redis(
    *,
    spool_dir: Path,
    engine: Optional[PostgresEngine],
    max_items: int,
    redis_client=None,
    redis_queue_key: str = "",
) -> tuple[int, int, int, bool]:
    if engine is None:
        return 0, 0, 0, False
    max_batch = max(0, int(max_items))
    if max_batch <= 0:
        return 0, 0, 0, False
    handled_posts = 0
    queued_redis = 0
    deleted_done = 0
    try:
        _recover_stale_processing_files(spool_dir=spool_dir)
        paths = sorted(spool_dir.glob(SPOOL_FILE_GLOB))
        if not paths:
            return 0, 0, 0, False
        with postgres_connect_autocommit(engine) as conn:
            for path in paths[:max_batch]:
                claimed_path = _claim_spool_file(path)
                if claimed_path is None:
                    continue
                payload = _load_claimed_payload(
                    claimed_path=claimed_path,
                )
                if payload is None:
                    continue

                post_uid = str(payload.get("post_uid") or "")
                if not post_uid:
                    _cleanup_spool_file(claimed_path)
                    continue

                try:
                    processed_at = load_post_processed_at(conn, post_uid=post_uid)
                except BaseException as e:
                    if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                        raise
                    _maybe_dispose_turso_engine_on_transient_error(engine=engine, err=e)
                    _restore_claimed_file_for_retry(
                        claimed_path=claimed_path,
                        target_path=path,
                    )
                    return handled_posts, queued_redis, deleted_done, True

                if str(processed_at or "").strip():
                    _cleanup_spool_file(claimed_path)
                    handled_posts += 1
                    deleted_done += 1
                    continue

                push_status = _try_push_payload_to_ai_ready_status(
                    redis_client=redis_client,
                    redis_queue_key=redis_queue_key,
                    post_uid=post_uid,
                    payload=payload,
                )
                if push_status == "error":
                    _restore_claimed_file_for_retry(
                        claimed_path=claimed_path,
                        target_path=path,
                    )
                    return handled_posts, queued_redis, deleted_done, True
                if push_status == "duplicate":
                    _restore_claimed_file_for_retry(
                        claimed_path=claimed_path,
                        target_path=path,
                    )
                    continue

                _cleanup_spool_file(claimed_path)
                handled_posts += 1
                if push_status == "pushed":
                    queued_redis += 1
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(engine=engine, err=e)
        logger.warning(
            "[spool] recover_connect_error %s: %s",
            type(e).__name__,
            e,
        )
        return handled_posts, queued_redis, deleted_done, True

    return handled_posts, queued_redis, deleted_done, False


__all__ = [
    "sha1_short",
    "ensure_spool_dir",
    "spool_write",
    "spool_delete",
    "flush_spool_to_turso",
    "recover_spool_to_turso_and_redis",
]
