from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.constants import SCHEMA_WEIBO, SCHEMA_XUEQIU  # noqa: E402
from alphavault.db.postgres_db import (  # noqa: E402
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import (  # noqa: E402
    PostgresSource,
    load_configured_postgres_sources_from_env,
)
from alphavault.db.semantic_docs import (  # noqa: E402
    load_assertion_semantic_rows_by_post_uids,
    load_stored_semantic_doc_embeddings_by_post_uids,
    scan_relevant_post_uids,
)
from alphavault.db.source_queue import load_cloud_posts  # noqa: E402
from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.logging_config import (  # noqa: E402
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from alphavault.semantic_docs import (  # noqa: E402
    SemanticDocSyncResult,
    semantic_doc_embedding_is_configured,
    semantic_doc_embedding_runtime_from_env,
    sync_semantic_docs_for_post,
)

DEFAULT_BATCH_SIZE = 100
DEFAULT_LIMIT = 0
DEFAULT_PROGRESS_FILE = ROOT_DIR / ".tmp" / "semantic_docs_backfill_progress.json"
SOURCE_SCHEMAS = frozenset((SCHEMA_WEIBO, SCHEMA_XUEQIU))
logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回填 semantic_docs")
    parser.add_argument(
        "--schema",
        choices=(*sorted(SOURCE_SCHEMAS), "all"),
        default="all",
        help="只处理某个 source schema，默认 all",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"每批扫描多少条帖子，默认 {DEFAULT_BATCH_SIZE}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="最多处理多少条帖子；0 表示不限制；schema=all 时按总量计算",
    )
    parser.add_argument(
        "--from-post-uid",
        default="",
        help="从哪个 post_uid 之后开始扫，默认从头开始",
    )
    parser.add_argument(
        "--post-uids",
        nargs="+",
        help="只处理指定 post_uid，空格分隔",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="从进度文件记录的上次成功 post_uid 后继续跑",
    )
    parser.add_argument(
        "--progress-file",
        default=str(DEFAULT_PROGRESS_FILE),
        help=f"断点续跑进度文件，默认 {DEFAULT_PROGRESS_FILE}",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="真的调用 embedding 并写库；默认只做 dry-run",
    )
    add_log_level_argument(parser)
    return parser.parse_args(argv)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clean_post_uids(values: list[str] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw_value in values or []:
        post_uid = _clean_text(raw_value)
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        out.append(post_uid)
    return out


def _chunk_post_uids(post_uids: list[str], *, chunk_size: int) -> list[list[str]]:
    cleaned = _clean_post_uids(post_uids)
    if not cleaned:
        return []
    resolved_chunk_size = max(1, int(chunk_size))
    return [
        cleaned[idx : idx + resolved_chunk_size]
        for idx in range(0, len(cleaned), resolved_chunk_size)
    ]


def _resolve_worker_count(
    *,
    engine_max_connections: int,
    embedding_runtime_max_inflight: int,
    chunk_size: int,
    apply: bool,
) -> int:
    if not apply:
        return 1
    return max(
        1,
        min(
            max(1, int(engine_max_connections)),
            max(1, int(embedding_runtime_max_inflight)),
            max(1, int(chunk_size)),
        ),
    )


def _load_progress_state(progress_file: Path) -> dict[str, str]:
    if not progress_file.is_file():
        return {}
    try:
        payload = json.loads(progress_file.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, str] = {}
    for raw_schema, raw_post_uid in payload.items():
        schema_name = _clean_text(raw_schema)
        post_uid = _clean_text(raw_post_uid)
        if schema_name and post_uid:
            out[schema_name] = post_uid
    return out


def _save_progress_state(progress_file: Path, state: dict[str, str]) -> None:
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    progress_file.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _effective_from_post_uid(
    *,
    source: PostgresSource,
    from_post_uid: str,
    target_post_uids: list[str],
    resume: bool,
    progress_state: dict[str, str],
) -> str:
    explicit_from_post_uid = _clean_text(from_post_uid)
    if explicit_from_post_uid or target_post_uids or not resume:
        return explicit_from_post_uid
    return _clean_text(progress_state.get(source.schema))


def _configured_sources(target_schema: str) -> list[PostgresSource]:
    sources = [
        source
        for source in load_configured_postgres_sources_from_env()
        if source.schema in SOURCE_SCHEMAS
    ]
    if target_schema == "all":
        return sources
    return [source for source in sources if source.schema == target_schema]


def _post_uid_matches_source(*, source: PostgresSource, post_uid: str) -> bool:
    lowered_post_uid = _clean_text(post_uid).lower()
    return lowered_post_uid.startswith(f"{source.name}:")


def _iter_source_post_uids(
    *,
    source: PostgresSource,
    batch_size: int,
    from_post_uid: str,
    limit: int,
    target_post_uids: list[str],
) -> list[str]:
    if target_post_uids:
        return [
            post_uid
            for post_uid in target_post_uids
            if _post_uid_matches_source(source=source, post_uid=post_uid)
        ]
    engine = ensure_postgres_engine(source.url, schema_name=source.schema)
    collected: list[str] = []
    after_post_uid = _clean_text(from_post_uid)
    remaining = max(0, int(limit))
    while True:
        current_batch_size = max(1, int(batch_size))
        if remaining > 0:
            current_batch_size = min(current_batch_size, remaining)
        rows = scan_relevant_post_uids(
            engine,
            after_post_uid=after_post_uid,
            batch_size=current_batch_size,
        )
        if not rows:
            break
        collected.extend(rows)
        after_post_uid = rows[-1]
        if remaining > 0:
            remaining -= len(rows)
            if remaining <= 0:
                break
    return collected


def _backfill_source(
    source: PostgresSource,
    *,
    batch_size: int,
    limit: int,
    from_post_uid: str,
    target_post_uids: list[str],
    apply: bool,
    resume: bool,
    progress_file: Path,
    progress_state: dict[str, str],
) -> tuple[int, int, int]:
    engine = ensure_postgres_engine(source.url, schema_name=source.schema)
    embedding_runtime = semantic_doc_embedding_runtime_from_env() if apply else None
    effective_from_post_uid = _effective_from_post_uid(
        source=source,
        from_post_uid=from_post_uid,
        target_post_uids=target_post_uids,
        resume=resume,
        progress_state=progress_state,
    )
    if effective_from_post_uid:
        logger.info(
            "schema=%s resume_from_post_uid=%s",
            source.schema,
            effective_from_post_uid,
        )
    post_uids = _iter_source_post_uids(
        source=source,
        batch_size=batch_size,
        from_post_uid=effective_from_post_uid,
        limit=limit,
        target_post_uids=target_post_uids,
    )
    if limit > 0:
        post_uids = post_uids[:limit]
    scanned_posts = 0
    synced_docs = 0
    embedded_docs = 0
    worker_count = _resolve_worker_count(
        engine_max_connections=engine.max_connections,
        embedding_runtime_max_inflight=(
            embedding_runtime.config.max_inflight
            if embedding_runtime is not None
            else 1
        ),
        chunk_size=batch_size,
        apply=apply,
    )
    logger.info(
        "schema=%s batch_size=%s worker_count=%s dry_run=%s",
        source.schema,
        batch_size,
        worker_count,
        "0" if apply else "1",
    )
    for post_uid_chunk in _chunk_post_uids(post_uids, chunk_size=batch_size):
        try:
            with postgres_connect_autocommit(engine) as conn:
                posts_by_post_uid = load_cloud_posts(
                    conn,
                    post_uids=post_uid_chunk,
                )
                assertion_rows_by_post_uid = load_assertion_semantic_rows_by_post_uids(
                    conn,
                    post_uids=post_uid_chunk,
                )
                stored_rows_by_post_uid = (
                    load_stored_semantic_doc_embeddings_by_post_uids(
                        conn,
                        post_uids=post_uid_chunk,
                    )
                    if apply
                    else {}
                )
            futures: list[tuple[str, Future[SemanticDocSyncResult]]] = []
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                for post_uid in post_uid_chunk:
                    prefetched_post = posts_by_post_uid.get(post_uid)
                    if prefetched_post is None:
                        raise RuntimeError(f"cloud_post_not_found:{post_uid}")
                    future = executor.submit(
                        sync_semantic_docs_for_post,
                        engine,
                        post_uid=post_uid,
                        final_status="relevant",
                        prefetched_post=prefetched_post,
                        prefetched_assertion_rows=assertion_rows_by_post_uid.get(
                            post_uid,
                            [],
                        ),
                        prefetched_stored_rows=stored_rows_by_post_uid.get(
                            post_uid,
                            [],
                        ),
                        apply=apply,
                        embedding_runtime=embedding_runtime,
                    )
                    futures.append((post_uid, future))
                for post_uid, future in futures:
                    result = future.result()
                    scanned_posts += 1
                    synced_docs += int(getattr(result, "doc_count"))
                    embedded_docs += int(getattr(result, "embedded_count"))
                    if resume and not target_post_uids:
                        progress_state[source.schema] = post_uid
                        _save_progress_state(progress_file, progress_state)
                    logger.info(
                        "schema=%s scanned_posts=%s doc_count=%s embedded_count=%s dry_run=%s post_uid=%s",
                        source.schema,
                        scanned_posts,
                        synced_docs,
                        embedded_docs,
                        "0" if apply else "1",
                        post_uid,
                    )
        except BaseException:
            failed_post_uid = progress_state.get(source.schema, "")
            if scanned_posts < len(post_uids):
                next_index = min(scanned_posts, len(post_uids) - 1)
                failed_post_uid = post_uids[next_index]
            logger.exception(
                "schema=%s failed_post_uid=%s scanned_posts=%s",
                source.schema,
                failed_post_uid,
                scanned_posts,
            )
            raise
    return scanned_posts, synced_docs, embedded_docs


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(level=args.log_level)
    load_dotenv_if_present()
    sources = _configured_sources(args.schema)
    if not sources:
        logger.error("没有可用的 source schema，先检查 POSTGRES_DSN。")
        return 1
    if args.apply:
        configured, message = semantic_doc_embedding_is_configured()
        if not configured:
            logger.error(message)
            return 1
    target_post_uids = _clean_post_uids(args.post_uids)
    progress_file = Path(args.progress_file).expanduser().resolve()
    progress_state = (
        _load_progress_state(progress_file)
        if bool(args.resume) and not target_post_uids
        else {}
    )
    total_posts = 0
    total_docs = 0
    total_embedded_docs = 0
    remaining_limit = max(0, int(args.limit))
    for source in sources:
        if remaining_limit == 0 and int(args.limit) > 0:
            break
        scanned_posts, synced_docs, embedded_docs = _backfill_source(
            source,
            batch_size=max(1, int(args.batch_size)),
            limit=remaining_limit,
            from_post_uid=args.from_post_uid,
            target_post_uids=target_post_uids,
            apply=bool(args.apply),
            resume=bool(args.resume),
            progress_file=progress_file,
            progress_state=progress_state,
        )
        total_posts += scanned_posts
        total_docs += synced_docs
        total_embedded_docs += embedded_docs
        if int(args.limit) > 0:
            remaining_limit = max(0, remaining_limit - scanned_posts)
    logger.info(
        "finished posts=%s docs=%s embedded_docs=%s dry_run=%s",
        total_posts,
        total_docs,
        total_embedded_docs,
        "0" if args.apply else "1",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
