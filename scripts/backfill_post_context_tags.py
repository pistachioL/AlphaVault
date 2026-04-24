from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.ai.analyze import AI_MODE_COMPLETION, AI_MODE_RESPONSES  # noqa: E402
from alphavault.constants import PLATFORM_WEIBO  # noqa: E402
from alphavault.db.postgres_db import (  # noqa: E402
    PostgresConnection,
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import require_postgres_source_from_env  # noqa: E402
from alphavault.db.source_queue import (  # noqa: E402
    CloudPost,
    load_cloud_post,
    write_post_context_result,
)
from alphavault.db.sql.common import make_in_params, make_in_placeholders  # noqa: E402
from alphavault.db.sql_rows import read_sql_rows  # noqa: E402
from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.infra.ai.runtime_config import AiRuntimeConfig  # noqa: E402
from alphavault.logging_config import (  # noqa: E402
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from alphavault.research_workbench.service import (  # noqa: E402
    dispose_research_workbench_engine_from_env,
)
from alphavault.research_stock_cache import mark_entity_page_dirty  # noqa: E402
from alphavault.rss.utils import RateLimiter, now_str  # noqa: E402
from alphavault.worker.post_context_tags import (  # noqa: E402
    PostContextResult,
    extract_post_context_result,
    extract_stock_entity_keys_from_entities,
    post_context_ai_runtime_config_from_env,
)

DEFAULT_BATCH_SIZE = 100
DEFAULT_LIMIT = 0
DEFAULT_SOURCE = PLATFORM_WEIBO
DEFAULT_TIMEOUT_SECONDS = 1000.0
AI_REASONING_EFFORT_CHOICES = ["none", "minimal", "low", "medium", "high", "xhigh"]
STATE_DIR = ROOT_DIR / ".states" / "backfill_post_context_tags"
logger = get_logger(__name__)


class _PostContextExtractError(RuntimeError):
    def __init__(self, *, post: CloudPost) -> None:
        super().__init__(f"post_context_extract_failed:{post.post_uid}")
        self.post = post


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env_config = post_context_ai_runtime_config_from_env(
        timeout_seconds_default=DEFAULT_TIMEOUT_SECONDS
    )
    parser = argparse.ArgumentParser(description="回补帖子级上下文标签")
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="source schema，默认 weibo",
    )
    parser.add_argument(
        "--model",
        default=env_config.model,
        help="默认读 AI_CONTEXT_MODEL，再回退到 AI_MODEL",
    )
    parser.add_argument(
        "--base-url",
        default=env_config.base_url,
        help="默认读 AI_CONTEXT_BASE_URL，再回退到 AI_BASE_URL",
    )
    parser.add_argument(
        "--api-key",
        default=env_config.api_key,
        help="默认读 AI_CONTEXT_API_KEY，再回退到 AI_API_KEY",
    )
    parser.add_argument(
        "--api-mode",
        default=env_config.api_mode,
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
        help="默认读 AI_CONTEXT_API_MODE，再回退到 AI_API_MODE",
    )
    parser.add_argument(
        "--ai-timeout-sec",
        type=float,
        default=env_config.timeout_seconds,
        help="默认读 AI_CONTEXT_TIMEOUT_SEC，再回退到 AI_TIMEOUT_SEC",
    )
    parser.add_argument(
        "--ai-retries",
        type=int,
        default=env_config.retries,
        help="默认读 AI_CONTEXT_RETRIES，再回退到 AI_RETRIES",
    )
    parser.add_argument(
        "--ai-rpm",
        type=float,
        default=env_config.ai_rpm,
        help="默认读 AI_CONTEXT_RPM，再回退到 AI_RPM",
    )
    parser.add_argument(
        "--ai-max-inflight",
        type=int,
        default=env_config.ai_max_inflight,
        help="默认读 AI_CONTEXT_MAX_INFLIGHT，再回退到 AI_MAX_INFLIGHT",
    )
    parser.add_argument(
        "--ai-reasoning-effort",
        default=env_config.reasoning_effort,
        choices=AI_REASONING_EFFORT_CHOICES,
        help="默认读 AI_CONTEXT_REASONING_EFFORT，再回退到 AI_REASONING_EFFORT",
    )
    parser.add_argument(
        "--ai-temperature",
        type=float,
        default=env_config.temperature,
        help="默认读 AI_CONTEXT_TEMPERATURE，再回退到 AI_TEMPERATURE",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"每批读取多少条帖子，默认 {DEFAULT_BATCH_SIZE}",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="是否从本地游标继续跑，默认 false",
    )
    parser.add_argument(
        "--reset-cursor",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="是否清空本地游标后重跑，默认 false",
    )
    parser.add_argument(
        "--cursor-file",
        default="",
        help="本地游标文件路径，默认 .states/backfill_post_context_tags/<source>.json",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="最多处理多少条帖子；0 表示不限制",
    )
    parser.add_argument(
        "--print-result",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="测试模式：按 --post-uids 直接打印 AI 结果，不写库",
    )
    parser.add_argument(
        "--post-uids",
        nargs="+",
        help="只处理指定 post_uid，空格分隔；测试模式也复用这个参数",
    )
    parser.add_argument(
        "--from-created-at",
        default="",
        help="只处理 created_at 大于等于该值的帖子，格式按库里的 created_at 文本传入",
    )
    add_log_level_argument(parser)
    return parser.parse_args(argv)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_positive_int(value: object, *, default: int) -> int:
    try:
        resolved = int(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = int(default)
    if resolved > 0:
        return resolved
    return max(1, int(default))


def _normalize_non_negative_int(value: object, *, default: int) -> int:
    try:
        resolved = int(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = int(default)
    return max(0, resolved)


def _normalize_non_negative_float(value: object, *, default: float) -> float:
    try:
        resolved = float(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = float(default)
    return max(0.0, resolved)


def _normalize_positive_float(value: object, *, default: float) -> float:
    try:
        resolved = float(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = float(default)
    if resolved > 0:
        return resolved
    return max(1.0, float(default))


def _clean_post_uids(values: list[str] | None) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        post_uid = _clean_text(raw)
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        cleaned.append(post_uid)
    return cleaned


def _resolve_cursor_file(args: argparse.Namespace) -> Path:
    raw_path = _clean_text(args.cursor_file)
    if raw_path:
        cursor_file = Path(raw_path)
        return cursor_file if cursor_file.is_absolute() else ROOT_DIR / cursor_file
    source_name = _clean_text(args.source) or DEFAULT_SOURCE
    return STATE_DIR / f"{source_name}.json"


def _load_cursor_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as err:
        raise RuntimeError(f"invalid_cursor_file:{path}") from err
    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid_cursor_file:{path}")
    return payload


def _write_cursor_state(
    path: Path,
    *,
    source: str,
    last_created_at: str,
    last_post_uid: str,
    processed_count: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source": source,
        "last_created_at": _clean_text(last_created_at),
        "last_post_uid": _clean_text(last_post_uid),
        "processed_count": max(int(processed_count), 0),
        "updated_at": now_str(),
    }
    tmp = path.with_suffix(f"{path.suffix}.tmp" if path.suffix else ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def _build_runtime_config(args: argparse.Namespace) -> AiRuntimeConfig:
    env_config = post_context_ai_runtime_config_from_env(
        timeout_seconds_default=DEFAULT_TIMEOUT_SECONDS
    )
    return replace(
        env_config,
        api_key=_clean_text(args.api_key),
        model=_clean_text(args.model) or env_config.model,
        base_url=_clean_text(args.base_url),
        api_mode=_clean_text(args.api_mode) or env_config.api_mode,
        temperature=float(args.ai_temperature),
        reasoning_effort=_clean_text(args.ai_reasoning_effort)
        or env_config.reasoning_effort,
        timeout_seconds=_normalize_positive_float(
            args.ai_timeout_sec,
            default=env_config.timeout_seconds,
        ),
        retries=_normalize_non_negative_int(
            args.ai_retries,
            default=env_config.retries,
        ),
        ai_rpm=_normalize_non_negative_float(args.ai_rpm, default=env_config.ai_rpm),
        ai_max_inflight=_normalize_positive_int(
            args.ai_max_inflight,
            default=env_config.ai_max_inflight,
        ),
    )


def _extract_results_for_posts(
    *,
    source_engine,
    posts: list[CloudPost],
    runtime_config: AiRuntimeConfig,
    limiter: RateLimiter,
) -> list[PostContextResult]:
    if not posts:
        return []
    futures: list[tuple[CloudPost, Future[PostContextResult]]] = []
    with ThreadPoolExecutor(max_workers=runtime_config.ai_max_inflight) as executor:
        for post in posts:
            futures.append(
                (
                    post,
                    executor.submit(
                        extract_post_context_result,
                        source_engine,
                        post=post,
                        runtime_config=runtime_config,
                        request_gate=limiter.wait,
                    ),
                )
            )
        results: list[PostContextResult] = []
        for post, future in futures:
            try:
                results.append(future.result())
            except Exception as err:
                raise _PostContextExtractError(post=post) from err
    return results


def _result_payload(
    *,
    post: CloudPost,
    runtime_config: AiRuntimeConfig,
    result,
) -> dict[str, object]:
    return {
        "post_uid": post.post_uid,
        "author": post.author,
        "created_at": post.created_at,
        "url": post.url,
        "runtime": {
            "model": runtime_config.model,
            "api_mode": runtime_config.api_mode,
            "base_url": runtime_config.base_url,
            "temperature": runtime_config.temperature,
            "reasoning_effort": runtime_config.reasoning_effort,
            "timeout_seconds": runtime_config.timeout_seconds,
            "retries": runtime_config.retries,
            "ai_rpm": runtime_config.ai_rpm,
            "ai_max_inflight": runtime_config.ai_max_inflight,
        },
        "result": asdict(result),
    }


def _print_results_for_post_uids(
    *,
    source_engine,
    post_uids: list[str],
    runtime_config: AiRuntimeConfig,
    limiter: RateLimiter,
) -> int:
    if not post_uids:
        raise RuntimeError("print_result_requires_post_uids")
    total = len(post_uids)
    posts: list[CloudPost] = []
    for post_uid in post_uids:
        try:
            posts.append(load_cloud_post(source_engine, post_uid))
        except Exception:
            logger.exception(
                "[post_context_backfill] print_failed post_uid=%s", post_uid
            )
            return 1
    try:
        results = _extract_results_for_posts(
            source_engine=source_engine,
            posts=posts,
            runtime_config=runtime_config,
            limiter=limiter,
        )
    except _PostContextExtractError as err:
        logger.exception(
            "[post_context_backfill] print_failed post_uid=%s",
            err.post.post_uid,
        )
        return 1
    for index, (post, result) in enumerate(zip(posts, results, strict=True), start=1):
        print(
            json.dumps(
                _result_payload(
                    post=post,
                    runtime_config=runtime_config,
                    result=result,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        if index < total:
            print()
    return 0


def _load_target_posts(
    conn: PostgresConnection,
    *,
    fetch_limit: int,
    post_uids: list[str],
    from_created_at: str,
    last_created_at: str,
    last_post_uid: str,
) -> list[CloudPost]:
    posts_table = f"{conn.schema_name}.posts"
    assertions_table = f"{conn.schema_name}.assertions"
    post_context_runs_table = f"{conn.schema_name}.post_context_runs"
    params: dict[str, object] = {"limit": max(1, int(fetch_limit))}
    where_clauses = [
        "p.processed_at IS NOT NULL",
        f"EXISTS (SELECT 1 FROM {assertions_table} a WHERE a.post_uid = p.post_uid)",
        (
            f"NOT EXISTS (SELECT 1 FROM {post_context_runs_table} r "
            "WHERE r.post_uid = p.post_uid)"
        ),
    ]
    cleaned_from_created_at = _clean_text(from_created_at)
    if cleaned_from_created_at:
        params["from_created_at"] = cleaned_from_created_at
        where_clauses.append("p.created_at >= :from_created_at")
    if post_uids:
        placeholders = make_in_placeholders(prefix="uid", count=len(post_uids))
        params.update(make_in_params(prefix="uid", values=post_uids))
        where_clauses.append(f"p.post_uid IN ({placeholders})")
    if _clean_text(last_created_at):
        params["last_created_at"] = _clean_text(last_created_at)
        params["last_post_uid"] = _clean_text(last_post_uid)
        where_clauses.append(
            "("
            "p.created_at > :last_created_at "
            "OR (p.created_at = :last_created_at AND p.post_uid > :last_post_uid)"
            ")"
        )
    query = f"""
SELECT
  p.post_uid,
  p.platform,
  p.platform_post_id,
  p.author,
  p.created_at,
  p.url,
  p.raw_text
FROM {posts_table} p
WHERE {" AND ".join(where_clauses)}
ORDER BY p.created_at ASC, p.post_uid ASC
LIMIT :limit
"""
    rows = read_sql_rows(conn, query, params=params)
    return [
        CloudPost(
            post_uid=_clean_text(row.get("post_uid")),
            platform=_clean_text(row.get("platform")),
            platform_post_id=_clean_text(row.get("platform_post_id")),
            author=_clean_text(row.get("author")),
            created_at=_clean_text(row.get("created_at")),
            url=_clean_text(row.get("url")),
            raw_text=_clean_text(row.get("raw_text")),
            ai_retry_count=0,
        )
        for row in rows
        if _clean_text(row.get("post_uid"))
    ]


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_present()
    args = parse_args(argv)
    configure_logging(level=args.log_level)

    cleaned_post_uids = _clean_post_uids(args.post_uids)
    batch_size = _normalize_positive_int(args.batch_size, default=DEFAULT_BATCH_SIZE)
    limit = _normalize_non_negative_int(args.limit, default=DEFAULT_LIMIT)
    from_created_at = _clean_text(args.from_created_at)
    print_result = bool(args.print_result)
    runtime_config = _build_runtime_config(args)
    limiter = RateLimiter(float(runtime_config.ai_rpm))
    cursor_file = _resolve_cursor_file(args)
    use_cursor = not cleaned_post_uids
    cursor_state: dict[str, object] = {}
    if print_result and not cleaned_post_uids:
        raise RuntimeError("print_result_requires_post_uids")
    if (not print_result) and bool(args.reset_cursor) and cursor_file.exists():
        cursor_file.unlink()
    if (not print_result) and use_cursor and bool(args.resume):
        cursor_state = _load_cursor_state(cursor_file)
    elif (not print_result) and cleaned_post_uids and bool(args.resume):
        logger.info("[post_context_backfill] ignore_resume_for_post_uids=1")
    scan_created_at = _clean_text(cursor_state.get("last_created_at"))
    scan_post_uid = _clean_text(cursor_state.get("last_post_uid"))
    cursor_processed_count = (
        _normalize_non_negative_int(cursor_state.get("processed_count"), default=0)
        if bool(args.resume)
        else 0
    )
    source = require_postgres_source_from_env(args.source)
    source_engine = ensure_postgres_engine(source.dsn, schema_name=source.schema)

    logger.info(
        "[post_context_backfill] start source=%s batch_size=%s limit=%s resume=%s "
        "cursor_file=%s model=%s api_mode=%s from_created_at=%s post_uids=%s "
        "print_result=%s ai_rpm=%s ai_max_inflight=%s",
        source.name,
        batch_size,
        limit,
        int(bool(args.resume) and use_cursor),
        str(cursor_file),
        runtime_config.model,
        runtime_config.api_mode,
        from_created_at or "(empty)",
        len(cleaned_post_uids),
        int(print_result),
        runtime_config.ai_rpm,
        runtime_config.ai_max_inflight,
    )

    try:
        if print_result:
            return _print_results_for_post_uids(
                source_engine=source_engine,
                post_uids=cleaned_post_uids,
                runtime_config=runtime_config,
                limiter=limiter,
            )

        processed_this_run = 0
        while True:
            if limit > 0 and processed_this_run >= limit:
                break
            fetch_limit = (
                min(batch_size, limit - processed_this_run) if limit > 0 else batch_size
            )
            with postgres_connect_autocommit(source_engine) as conn:
                posts = _load_target_posts(
                    conn,
                    fetch_limit=fetch_limit,
                    post_uids=cleaned_post_uids,
                    from_created_at=from_created_at,
                    last_created_at=scan_created_at if use_cursor else "",
                    last_post_uid=scan_post_uid if use_cursor else "",
                )
            if not posts:
                break
            try:
                results = _extract_results_for_posts(
                    source_engine=source_engine,
                    posts=posts,
                    runtime_config=runtime_config,
                    limiter=limiter,
                )
            except _PostContextExtractError as err:
                logger.exception(
                    "[post_context_backfill] failed post_uid=%s created_at=%s",
                    err.post.post_uid,
                    err.post.created_at,
                )
                return 1
            for post, result in zip(posts, results, strict=True):
                write_post_context_result(
                    source_engine,
                    post_uid=post.post_uid,
                    model=result.model,
                    prompt_version=result.prompt_version,
                    processed_at=result.processed_at,
                    mentions=result.mentions,
                    entities=result.entities,
                    entity_match_result=result.entity_match_result,
                )
                stock_keys = extract_stock_entity_keys_from_entities(result.entities)
                for stock_key in stock_keys:
                    try:
                        mark_entity_page_dirty(
                            source_engine,
                            stock_key=stock_key,
                            reason="ai_done",
                        )
                    except BaseException:
                        logger.warning(
                            "[post_context_backfill] mark_dirty_failed post_uid=%s stock_key=%s",
                            post.post_uid,
                            stock_key,
                        )

                processed_this_run += 1
                scan_created_at = post.created_at
                scan_post_uid = post.post_uid
                if use_cursor:
                    cursor_processed_count += 1
                    _write_cursor_state(
                        cursor_file,
                        source=source.name,
                        last_created_at=scan_created_at,
                        last_post_uid=scan_post_uid,
                        processed_count=cursor_processed_count,
                    )
                logger.info(
                    "[post_context_backfill] done post_uid=%s created_at=%s mentions=%s entities=%s processed=%s",
                    post.post_uid,
                    post.created_at,
                    len(result.mentions),
                    len(result.entities),
                    processed_this_run,
                )
                if limit > 0 and processed_this_run >= limit:
                    break
        logger.info(
            "[post_context_backfill] finish source=%s processed=%s last_created_at=%s last_post_uid=%s",
            source.name,
            processed_this_run,
            scan_created_at or "(empty)",
            scan_post_uid or "(empty)",
        )
        return 0
    finally:
        dispose_research_workbench_engine_from_env()
        source_engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
