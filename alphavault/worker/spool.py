from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import Engine

from alphavault.constants import DEFAULT_SPOOL_DIR, ENV_SPOOL_DIR
from alphavault.db.turso_db import (
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
)
from alphavault.db.turso_queue import upsert_pending_post
from alphavault.rss.utils import now_str
from alphavault.weibo.display import format_weibo_display_md

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def sha1_short(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:20]


def ensure_spool_dir() -> Path:
    value = os.getenv(ENV_SPOOL_DIR, "").strip() or DEFAULT_SPOOL_DIR
    path = Path(value)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[spool] dir_error {path} {type(e).__name__}: {e}", flush=True)
    return path


def _spool_path(spool_dir: Path, post_uid: str) -> Path:
    return spool_dir / f"{sha1_short(post_uid)}.json"


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


def _maybe_dispose_turso_engine_on_transient_error(
    *, engine: Engine, err: BaseException
) -> None:
    if not (is_turso_stream_not_found_error(err) or is_turso_libsql_panic_error(err)):
        return
    try:
        engine.dispose()
    except Exception:
        return


def flush_spool_to_turso(
    *,
    spool_dir: Path,
    engine: Optional[Engine],
    max_items: int,
    verbose: bool,
) -> Tuple[int, bool]:
    if engine is None:
        return 0, False
    paths = sorted(spool_dir.glob("*.json"))
    if not paths:
        return 0, False
    processed = 0
    try:
        with turso_connect_autocommit(engine) as conn:
            for path in paths[: max(0, int(max_items))]:
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except Exception as e:
                    if verbose:
                        print(
                            f"[spool] bad_file {path.name} {type(e).__name__}: {e}",
                            flush=True,
                        )
                    path.unlink(missing_ok=True)
                    continue

                post_uid = str(payload.get("post_uid") or "")
                if not post_uid:
                    path.unlink(missing_ok=True)
                    continue

                try:
                    raw_text = str(payload.get("raw_text") or "")
                    author = str(payload.get("author") or "")
                    display_md = str(payload.get("display_md") or "")
                    if not display_md.strip():
                        display_md = format_weibo_display_md(raw_text, author=author)
                    upsert_pending_post(
                        conn,
                        post_uid=post_uid,
                        platform=str(payload.get("platform") or "weibo"),
                        platform_post_id=str(payload.get("platform_post_id") or ""),
                        author=str(payload.get("author") or ""),
                        created_at=str(payload.get("created_at") or now_str()),
                        url=str(payload.get("url") or ""),
                        raw_text=raw_text,
                        display_md=display_md,
                        archived_at=now_str(),
                        ingested_at=int(payload.get("ingested_at") or int(time.time())),
                    )
                except BaseException as e:
                    if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                        raise
                    _maybe_dispose_turso_engine_on_transient_error(engine=engine, err=e)
                    if verbose:
                        print(
                            f"[spool] turso_write_error {path.name} {type(e).__name__}: {e}",
                            flush=True,
                        )
                    return processed, True

                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass
                processed += 1
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(engine=engine, err=e)
        if verbose:
            print(f"[spool] turso_connect_error {type(e).__name__}: {e}", flush=True)
        return processed, True

    return processed, False
