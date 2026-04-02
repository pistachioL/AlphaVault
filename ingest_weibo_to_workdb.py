from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from alphavault.ai.analyze import (
    AI_MODE_COMPLETION,
    AI_MODE_RESPONSES,
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL as DEFAULT_ANALYZE_MODEL,
    DEFAULT_PROMPT_VERSION,
    AnalyzeResult,
    analyze_with_litellm,
    clean_text,
    validate_and_adjust_assertions,
)
from alphavault.constants import (
    ENV_AI_API_KEY,
    ENV_AI_API_MODE,
    ENV_AI_BASE_URL,
    ENV_AI_MODEL,
    ENV_AI_PROMPT_VERSION,
)


def _env_first(*keys: str, default: str = "") -> str:
    for key in keys:
        value = str(os.getenv(key, "") or "").strip()
        if value:
            return value
    return default


DEFAULT_CSV_PATH = Path("weibo/挖地瓜的超级鹿鼎公/3962719063_new.csv")
DEFAULT_DB_PATH = Path("workdb.sqlite")
DEFAULT_AUTHOR = "挖地瓜的超级鹿鼎公"
DEFAULT_USER_ID = "3962719063"

DEFAULT_MODEL = _env_first(ENV_AI_MODEL, "GEMINI_MODEL", default=DEFAULT_ANALYZE_MODEL)
DEFAULT_BASE_URL = _env_first(ENV_AI_BASE_URL, "GEMINI_BASE_URL", default="")
DEFAULT_API_MODE = _env_first(ENV_AI_API_MODE, default=DEFAULT_AI_MODE)
DEFAULT_PROMPT_VERSION_LOCAL = _env_first(
    ENV_AI_PROMPT_VERSION,
    default=DEFAULT_PROMPT_VERSION,
)
DEFAULT_AI_MAX_INFLIGHT = int(os.getenv("AI_MAX_INFLIGHT", "32"))
DEFAULT_INGEST_AI_RETRIES = int(os.getenv("INGEST_AI_RETRIES", "1"))
DEFAULT_HEARTBEAT_SECONDS = float(os.getenv("INGEST_HEARTBEAT_SECONDS", "15"))


def now_str() -> str:
    cst = timezone(timedelta(hours=8))
    return datetime.now(cst).strftime("%Y-%m-%d %H:%M:%S")


def init_workdb(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                post_uid TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                platform_post_id TEXT NOT NULL,
                author TEXT NOT NULL,
                created_at TEXT NOT NULL,
                url TEXT NOT NULL,
                raw_text TEXT NOT NULL,
                ingested_at TEXT NOT NULL,

                status TEXT NOT NULL DEFAULT 'new'
                    CHECK (status IN ('new', 'processing', 'relevant', 'irrelevant', 'error')),
                invest_score REAL,
                processed_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_retry_at TEXT,
                last_error TEXT,
                model TEXT,
                prompt_version TEXT,

                sync_status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (sync_status IN ('pending', 'synced', 'error')),
                sync_attempts INTEGER NOT NULL DEFAULT 0,
                next_sync_at TEXT,
                synced_at TEXT,
                sync_error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assertions (
                post_uid TEXT NOT NULL,
                idx INTEGER NOT NULL CHECK (idx >= 1),
                topic_key TEXT NOT NULL,
                action TEXT NOT NULL,
                action_strength INTEGER NOT NULL CHECK (action_strength BETWEEN 0 AND 3),
                summary TEXT NOT NULL,
                evidence TEXT NOT NULL,
                confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
                stock_codes_json TEXT NOT NULL DEFAULT '[]',
                stock_names_json TEXT NOT NULL DEFAULT '[]',
                industries_json TEXT NOT NULL DEFAULT '[]',
                commodities_json TEXT NOT NULL DEFAULT '[]',
                indices_json TEXT NOT NULL DEFAULT '[]',
                UNIQUE(post_uid, idx),
                FOREIGN KEY (post_uid) REFERENCES posts(post_uid) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_status_next_retry ON posts(status, next_retry_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_sync_status_next_sync ON posts(sync_status, next_sync_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_author_created_at ON posts(author, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_platform_post_id ON posts(platform, platform_post_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_ingested_at ON posts(ingested_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_assertions_topic_key ON assertions(topic_key)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_assertions_action ON assertions(action)"
        )
        conn.commit()
    finally:
        conn.close()


def normalize_datetime(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return now_str()
    return v.replace("T", " ")


def split_commentary_and_quoted(body: str) -> tuple[str, str]:
    text = clean_text(body)
    marker = "//@"
    idx = text.find(marker)
    if idx < 0:
        return text, ""
    commentary = text[:idx].strip()
    quoted = text[idx:].strip()
    return commentary, quoted


def build_raw_text(row: Dict[str, str]) -> str:
    body = clean_text(row.get("正文", ""))
    source_body = clean_text(row.get("源微博正文", ""))
    topic = clean_text(row.get("话题", ""))
    at_users = clean_text(row.get("@用户", ""))

    parts: List[str] = []
    if body:
        parts.append(body)

    meta_lines: List[str] = []
    if topic:
        meta_lines.append(f"话题: {topic}")
    if at_users:
        meta_lines.append(f"@用户: {at_users}")
    if meta_lines:
        parts.append("[微博元信息]\n" + "\n".join(meta_lines))

    if source_body:
        parts.append("[转发原文]\n" + source_body)

    parts.append("[CSV原始字段]\n" + json.dumps(row, ensure_ascii=False))
    return "\n\n".join(parts).strip()


def build_analysis_context(row: Dict[str, str]) -> Dict[str, str]:
    body = clean_text(row.get("正文", ""))
    source_body = clean_text(row.get("源微博正文", ""))
    commentary_text, quoted_inline = split_commentary_and_quoted(body)

    quoted_parts: List[str] = []
    if quoted_inline:
        quoted_parts.append(quoted_inline)
    if source_body:
        quoted_parts.append(source_body)
    quoted_text = "\n".join([x for x in quoted_parts if x]).strip()

    return {
        "commentary_text": commentary_text,
        "quoted_text": quoted_text,
        "full_text": body,
        "source_text": source_body,
    }


def build_url(row: Dict[str, str], user_id: str) -> str:
    article_url = clean_text(row.get("头条文章url", ""))
    if article_url:
        return article_url
    bid = clean_text(row.get("bid", ""))
    mid = clean_text(row.get("id", "")).strip()
    if bid:
        return f"https://weibo.com/{user_id}/{bid}"
    return f"https://weibo.com/{user_id}/{mid}"


class GlobalAiRateLimiter:
    def __init__(self, *, ai_rpm: float, verbose: bool) -> None:
        self._min_interval = 60.0 / ai_rpm if ai_rpm and ai_rpm > 0 else 0.0
        self._last_call_ts: Optional[float] = None
        self._lock = threading.Lock()
        self._verbose = bool(verbose)

    def wait(self, *, row_index: int, post_uid: str) -> None:
        if self._min_interval <= 0:
            return
        with self._lock:
            if self._last_call_ts is None:
                self._last_call_ts = time.time()
                return
            since_last = time.time() - self._last_call_ts
            if since_last < self._min_interval:
                wait_sec = self._min_interval - since_last
                if self._verbose:
                    print(
                        f"[{row_index}] wait_rpm {post_uid} sleep={wait_sec:.2f}s",
                        flush=True,
                    )
                time.sleep(wait_sec)
            self._last_call_ts = time.time()


def estimate_ai_call_max_seconds(timeout_seconds: float, ai_retries: int) -> float:
    retries = max(0, int(ai_retries))
    total = 0.0
    backoff = 2.0
    for attempt in range(retries + 1):
        total += max(0.0, float(timeout_seconds))
        if attempt < retries:
            total += min(backoff, 32.0)
            backoff = min(backoff * 2.0, 32.0)
    return total


def analyze_with_heartbeat(
    *,
    row_index: int,
    post_uid: str,
    heartbeat_seconds: float,
    timeout_seconds: float,
    ai_retries: int,
    verbose: bool,
    api_key: str,
    model: str,
    analysis_context: Dict[str, str],
    row: Dict[str, str],
    base_url: str,
    api_mode: str,
    ai_stream: bool,
    ai_temperature: float,
    ai_reasoning_effort: str,
    trace_out: Optional[Path],
) -> AnalyzeResult:
    if heartbeat_seconds <= 0:
        return analyze_with_litellm(
            api_key=api_key,
            model=model,
            analysis_context=analysis_context,
            row=row,
            base_url=base_url,
            api_mode=api_mode,
            ai_stream=ai_stream,
            ai_retries=ai_retries,
            ai_temperature=ai_temperature,
            ai_reasoning_effort=ai_reasoning_effort,
            trace_out=trace_out,
            timeout_seconds=timeout_seconds,
        )

    result_holder: Dict[str, Any] = {"result": None, "error": None}

    def _run() -> None:
        try:
            result_holder["result"] = analyze_with_litellm(
                api_key=api_key,
                model=model,
                analysis_context=analysis_context,
                row=row,
                base_url=base_url,
                api_mode=api_mode,
                ai_stream=ai_stream,
                ai_retries=ai_retries,
                ai_temperature=ai_temperature,
                ai_reasoning_effort=ai_reasoning_effort,
                trace_out=trace_out,
                timeout_seconds=timeout_seconds,
            )
        except BaseException as exc:
            result_holder["error"] = exc

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    started_at = time.time()
    next_log_at = started_at + heartbeat_seconds

    while worker.is_alive():
        worker.join(timeout=0.5)
        if not verbose:
            continue
        now = time.time()
        if now < next_log_at:
            continue
        elapsed = now - started_at
        print(
            f"[{row_index}] wait_api {post_uid} elapsed={elapsed:.1f}s "
            f"timeout={timeout_seconds:.1f}s retries={ai_retries}",
            flush=True,
        )
        next_log_at = now + heartbeat_seconds

    error = result_holder.get("error")
    if isinstance(error, BaseException):
        raise error

    result = result_holder.get("result")
    if not isinstance(result, AnalyzeResult):
        raise RuntimeError("ai_call_no_result")
    return result


@dataclass
class AiTaskContext:
    row_index: int
    post_uid: str
    attempts: int
    analysis_context: Dict[str, str]
    ai_row_meta: Dict[str, str]


@dataclass
class AiTaskOutput:
    result: AnalyzeResult
    cost_seconds: float


def run_one_ai_task(
    *,
    task: AiTaskContext,
    limiter: GlobalAiRateLimiter,
    heartbeat_seconds: float,
    timeout_seconds: float,
    ai_retries: int,
    verbose: bool,
    api_key: str,
    model: str,
    base_url: str,
    api_mode: str,
    ai_stream: bool,
    ai_temperature: float,
    ai_reasoning_effort: str,
    trace_out: Optional[Path],
) -> AiTaskOutput:
    limiter.wait(row_index=task.row_index, post_uid=task.post_uid)
    start_ts = time.time()
    result = analyze_with_heartbeat(
        row_index=task.row_index,
        post_uid=task.post_uid,
        heartbeat_seconds=heartbeat_seconds,
        timeout_seconds=timeout_seconds,
        ai_retries=ai_retries,
        verbose=verbose,
        api_key=api_key,
        model=model,
        analysis_context=task.analysis_context,
        row=task.ai_row_meta,
        base_url=base_url,
        api_mode=api_mode,
        ai_stream=ai_stream,
        ai_temperature=ai_temperature,
        ai_reasoning_effort=ai_reasoning_effort,
        trace_out=trace_out,
    )
    return AiTaskOutput(result=result, cost_seconds=(time.time() - start_ts))


def upsert_post_base(
    conn: sqlite3.Connection,
    *,
    post_uid: str,
    platform_post_id: str,
    author: str,
    created_at: str,
    url: str,
    raw_text: str,
) -> None:
    conn.execute(
        """
        INSERT INTO posts (
            post_uid, platform, platform_post_id, author, created_at, url, raw_text, ingested_at
        ) VALUES (?, 'weibo', ?, ?, ?, ?, ?, ?)
        ON CONFLICT(post_uid) DO UPDATE SET
            platform_post_id=excluded.platform_post_id,
            author=excluded.author,
            created_at=excluded.created_at,
            url=excluded.url,
            raw_text=excluded.raw_text
        """,
        (post_uid, platform_post_id, author, created_at, url, raw_text, now_str()),
    )


def mark_processing(
    conn: sqlite3.Connection,
    post_uid: str,
    model: str,
    prompt_version: str,
) -> int:
    row = conn.execute(
        "SELECT attempts FROM posts WHERE post_uid = ?",
        (post_uid,),
    ).fetchone()
    attempts = int(row[0] or 0) + 1 if row else 1
    conn.execute(
        """
        UPDATE posts
        SET status='processing',
            attempts=?,
            processed_at=NULL,
            last_error=NULL,
            next_retry_at=NULL,
            model=?,
            prompt_version=?
        WHERE post_uid=?
        """,
        (attempts, model, prompt_version, post_uid),
    )
    return attempts


def mark_error(
    conn: sqlite3.Connection, post_uid: str, error: str, attempts: int
) -> None:
    retry_minutes = min(60, 2 ** min(attempts, 6))
    next_retry = (
        datetime.now(timezone.utc) + timedelta(minutes=retry_minutes)
    ).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        UPDATE posts
        SET status='error',
            last_error=?,
            next_retry_at=?,
            processed_at=?
        WHERE post_uid=?
        """,
        (error[:1000], next_retry, now_str(), post_uid),
    )


def write_assertions(
    conn: sqlite3.Connection, post_uid: str, assertions: List[Dict[str, Any]]
) -> None:
    conn.execute("DELETE FROM assertions WHERE post_uid = ?", (post_uid,))
    for idx, a in enumerate(assertions, start=1):
        conn.execute(
            """
            INSERT INTO assertions (
                post_uid, idx, topic_key, action, action_strength, summary, evidence, confidence,
                stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                post_uid,
                idx,
                a["topic_key"],
                a["action"],
                a["action_strength"],
                a["summary"],
                a["evidence"],
                a["confidence"],
                a["stock_codes_json"],
                a["stock_names_json"],
                a["industries_json"],
                a["commodities_json"],
                a["indices_json"],
            ),
        )


def mark_done(
    conn: sqlite3.Connection,
    post_uid: str,
    result: AnalyzeResult,
    model: str,
    prompt_version: str,
) -> None:
    conn.execute(
        """
        UPDATE posts
        SET status=?,
            invest_score=?,
            processed_at=?,
            last_error=NULL,
            next_retry_at=NULL,
            model=?,
            prompt_version=?
        WHERE post_uid=?
        """,
        (
            result.status,
            result.invest_score,
            now_str(),
            model,
            prompt_version,
            post_uid,
        ),
    )


def should_skip(conn: sqlite3.Connection, post_uid: str, reprocess: bool) -> bool:
    if reprocess:
        return False
    row = conn.execute(
        "SELECT status FROM posts WHERE post_uid = ?",
        (post_uid,),
    ).fetchone()
    if not row:
        return False
    return row[0] in ("relevant", "irrelevant")


def _build_ai_row_meta(
    row: Dict[str, str],
    *,
    url: str,
    author: str,
    created_at: str,
) -> Dict[str, str]:
    meta = dict(row)
    meta.setdefault("link", url)
    meta.setdefault("author", author)
    meta.setdefault("published_at", created_at)
    return meta


def build_analysis_context_from_raw_text(raw_text: str) -> Dict[str, str]:
    text = clean_text(raw_text)
    commentary_text, quoted_text = split_commentary_and_quoted(text)
    return {
        "commentary_text": commentary_text,
        "quoted_text": quoted_text,
        "full_text": text,
        "source_text": "",
    }


def process_csv(
    csv_path: Path,
    db_path: Path,
    api_key: str,
    model: str,
    prompt_version: str,
    author: str,
    user_id: str,
    limit: Optional[int],
    sleep_seconds: float,
    reprocess: bool,
    relevant_threshold: float,
    base_url: str,
    api_mode: str,
    ai_stream: bool,
    ai_retries: int,
    ai_temperature: float,
    ai_reasoning_effort: str,
    ai_rpm: float,
    ai_max_inflight: int,
    heartbeat_seconds: float,
    trace_out: Optional[Path],
    timeout_seconds: float,
    progress_every: int,
    verbose: bool,
) -> None:
    init_workdb(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        processed = 0
        skipped = 0
        failed = 0
        total = 0
        max_inflight = max(1, int(ai_max_inflight))
        limiter = GlobalAiRateLimiter(ai_rpm=ai_rpm, verbose=verbose)
        estimated_ai_max = estimate_ai_call_max_seconds(timeout_seconds, ai_retries)
        if verbose:
            print(
                "[config] "
                f"csv={csv_path} db={db_path} model={model} api_mode={api_mode} "
                f"timeout={timeout_seconds:.1f}s retries={ai_retries} "
                f"heartbeat={heartbeat_seconds:.1f}s max_inflight={max_inflight} "
                f"ai_call_max≈{estimated_ai_max:.1f}s",
                flush=True,
            )
            if limit is not None:
                print(
                    f"[config] limit={limit} (counts processed success only)",
                    flush=True,
                )

        inflight: Dict[Future[AiTaskOutput], AiTaskContext] = {}

        def handle_done_futures(done_futures: set[Future[AiTaskOutput]]) -> None:
            nonlocal processed, failed
            for done in done_futures:
                task = inflight.pop(done, None)
                if task is None:
                    continue
                try:
                    out = done.result()
                    result = out.result
                    if verbose:
                        print(
                            f"[{task.row_index}] done {task.post_uid} status={result.status} "
                            f"score={result.invest_score:.3f} cost={out.cost_seconds:.1f}s",
                            flush=True,
                        )

                    if result.invest_score < relevant_threshold:
                        result = AnalyzeResult(
                            status="irrelevant",
                            invest_score=result.invest_score,
                            assertions=[],
                        )
                    else:
                        result.assertions = validate_and_adjust_assertions(
                            result.assertions,
                            commentary_text=task.analysis_context["commentary_text"],
                            quoted_text=task.analysis_context["quoted_text"],
                        )

                    assertions = (
                        result.assertions if result.status == "relevant" else []
                    )
                    write_assertions(conn, task.post_uid, assertions)
                    mark_done(
                        conn,
                        task.post_uid,
                        result,
                        model=model,
                        prompt_version=prompt_version,
                    )
                    conn.commit()
                    processed += 1
                except BaseException as e:
                    if isinstance(e, (KeyboardInterrupt, SystemExit)):
                        raise
                    failed += 1
                    print(
                        f"[{task.row_index}] error {task.post_uid} {type(e).__name__}: {e}",
                        flush=True,
                    )
                    mark_error(
                        conn,
                        task.post_uid,
                        f"{type(e).__name__}: {e}",
                        task.attempts,
                    )
                    conn.commit()

                if sleep_seconds > 0:
                    if verbose:
                        print(
                            f"[{task.row_index}] wait_row sleep={sleep_seconds:.2f}s",
                            flush=True,
                        )
                    time.sleep(sleep_seconds)

        with ThreadPoolExecutor(max_workers=max_inflight) as executor:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if limit is not None and processed >= limit:
                        break

                    if inflight:
                        done, _ = wait(
                            set(inflight.keys()),
                            timeout=0.0,
                            return_when=FIRST_COMPLETED,
                        )
                        if done:
                            handle_done_futures(done)
                            if limit is not None and processed >= limit:
                                break

                    total += 1
                    mid = clean_text(row.get("id", "")).strip()
                    if not mid:
                        skipped += 1
                        continue

                    post_uid = f"weibo:{mid}"
                    if should_skip(conn, post_uid, reprocess):
                        skipped += 1
                        continue

                    platform_post_id = mid
                    created_at = normalize_datetime(
                        clean_text(row.get("完整日期", ""))
                        or clean_text(row.get("日期", ""))
                    )
                    url = build_url(row, user_id=user_id)
                    raw_text = build_raw_text(row)
                    analysis_context = build_analysis_context(row)
                    ai_row_meta = _build_ai_row_meta(
                        row,
                        url=url,
                        author=author,
                        created_at=created_at,
                    )
                    if not raw_text:
                        skipped += 1
                        continue

                    upsert_post_base(
                        conn,
                        post_uid=post_uid,
                        platform_post_id=platform_post_id,
                        author=author,
                        created_at=created_at,
                        url=url,
                        raw_text=raw_text,
                    )
                    attempts = mark_processing(
                        conn,
                        post_uid,
                        model=model,
                        prompt_version=prompt_version,
                    )
                    conn.commit()

                    task = AiTaskContext(
                        row_index=total,
                        post_uid=post_uid,
                        attempts=attempts,
                        analysis_context=analysis_context,
                        ai_row_meta=ai_row_meta,
                    )
                    if verbose:
                        print(f"[{total}] call_api {post_uid}", flush=True)
                    future = executor.submit(
                        run_one_ai_task,
                        task=task,
                        limiter=limiter,
                        heartbeat_seconds=heartbeat_seconds,
                        timeout_seconds=timeout_seconds,
                        ai_retries=ai_retries,
                        verbose=verbose,
                        api_key=api_key,
                        model=model,
                        base_url=base_url,
                        api_mode=api_mode,
                        ai_stream=ai_stream,
                        ai_temperature=ai_temperature,
                        ai_reasoning_effort=ai_reasoning_effort,
                        trace_out=trace_out,
                    )
                    inflight[future] = task

                    while len(inflight) >= max_inflight:
                        done, _ = wait(
                            set(inflight.keys()),
                            return_when=FIRST_COMPLETED,
                        )
                        handle_done_futures(done)
                        if limit is not None and processed >= limit:
                            break
                    if limit is not None and processed >= limit:
                        break

                    if progress_every > 0 and total % progress_every == 0:
                        print(
                            f"[{total}] progress processed={processed} skipped={skipped} failed={failed} inflight={len(inflight)}",
                            flush=True,
                        )

            while inflight:
                done, _ = wait(
                    set(inflight.keys()),
                    return_when=FIRST_COMPLETED,
                )
                handle_done_futures(done)
                if limit is not None and processed >= limit:
                    # 仅停止继续提交新任务；已在跑的任务仍会收尾并写库。
                    continue

        print(
            json.dumps(
                {
                    "mode": "csv",
                    "csv_total_rows": total,
                    "processed": processed,
                    "skipped": skipped,
                    "failed": failed,
                    "db": str(db_path.resolve()),
                    "model": model,
                    "prompt_version": prompt_version,
                    "base_url": base_url,
                    "api_mode": api_mode,
                    "ai_stream": ai_stream,
                    "ai_retries": ai_retries,
                    "ai_reasoning_effort": ai_reasoning_effort,
                    "ai_rpm": ai_rpm,
                    "trace_out": str(trace_out) if trace_out else "",
                },
                ensure_ascii=False,
            )
        )
    finally:
        conn.close()


def process_db_status(
    *,
    db_path: Path,
    api_key: str,
    model: str,
    prompt_version: str,
    limit: Optional[int],
    sleep_seconds: float,
    only_status: str,
    relevant_threshold: float,
    base_url: str,
    api_mode: str,
    ai_stream: bool,
    ai_retries: int,
    ai_temperature: float,
    ai_reasoning_effort: str,
    ai_rpm: float,
    ai_max_inflight: int,
    heartbeat_seconds: float,
    trace_out: Optional[Path],
    timeout_seconds: float,
    progress_every: int,
    verbose: bool,
) -> None:
    init_workdb(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        status_value = clean_text(only_status).lower()
        if status_value not in {"error", "processing", "new"}:
            raise ValueError("only_status_must_be_error_processing_new")

        processed = 0
        skipped = 0
        failed = 0
        total = 0
        max_inflight = max(1, int(ai_max_inflight))
        limiter = GlobalAiRateLimiter(ai_rpm=ai_rpm, verbose=verbose)
        estimated_ai_max = estimate_ai_call_max_seconds(timeout_seconds, ai_retries)
        if verbose:
            print(
                "[config] "
                f"mode=db_status status={status_value} db={db_path} model={model} "
                f"api_mode={api_mode} timeout={timeout_seconds:.1f}s retries={ai_retries} "
                f"heartbeat={heartbeat_seconds:.1f}s max_inflight={max_inflight} "
                f"ai_call_max≈{estimated_ai_max:.1f}s",
                flush=True,
            )
            if limit is not None:
                print(
                    f"[config] limit={limit} (counts processed success only)",
                    flush=True,
                )

        selected_rows = conn.execute(
            """
            SELECT post_uid, platform_post_id, author, created_at, url, raw_text
            FROM posts
            WHERE platform='weibo' AND status=?
            ORDER BY created_at ASC, post_uid ASC
            """,
            (status_value,),
        ).fetchall()
        selected_total = len(selected_rows)
        if verbose:
            print(
                f"[config] selected_rows={selected_total}",
                flush=True,
            )

        inflight: Dict[Future[AiTaskOutput], AiTaskContext] = {}

        def handle_done_futures(done_futures: set[Future[AiTaskOutput]]) -> None:
            nonlocal processed, failed
            for done in done_futures:
                task = inflight.pop(done, None)
                if task is None:
                    continue
                try:
                    out = done.result()
                    result = out.result
                    if verbose:
                        print(
                            f"[{task.row_index}] done {task.post_uid} status={result.status} "
                            f"score={result.invest_score:.3f} cost={out.cost_seconds:.1f}s",
                            flush=True,
                        )

                    if result.invest_score < relevant_threshold:
                        result = AnalyzeResult(
                            status="irrelevant",
                            invest_score=result.invest_score,
                            assertions=[],
                        )
                    else:
                        result.assertions = validate_and_adjust_assertions(
                            result.assertions,
                            commentary_text=task.analysis_context["commentary_text"],
                            quoted_text=task.analysis_context["quoted_text"],
                        )

                    assertions = (
                        result.assertions if result.status == "relevant" else []
                    )
                    write_assertions(conn, task.post_uid, assertions)
                    mark_done(
                        conn,
                        task.post_uid,
                        result,
                        model=model,
                        prompt_version=prompt_version,
                    )
                    conn.commit()
                    processed += 1
                except BaseException as e:
                    if isinstance(e, (KeyboardInterrupt, SystemExit)):
                        raise
                    failed += 1
                    print(
                        f"[{task.row_index}] error {task.post_uid} {type(e).__name__}: {e}",
                        flush=True,
                    )
                    mark_error(
                        conn,
                        task.post_uid,
                        f"{type(e).__name__}: {e}",
                        task.attempts,
                    )
                    conn.commit()

                if sleep_seconds > 0:
                    if verbose:
                        print(
                            f"[{task.row_index}] wait_row sleep={sleep_seconds:.2f}s",
                            flush=True,
                        )
                    time.sleep(sleep_seconds)

        with ThreadPoolExecutor(max_workers=max_inflight) as executor:
            for db_row in selected_rows:
                if limit is not None and processed >= limit:
                    break

                if inflight:
                    done, _ = wait(
                        set(inflight.keys()),
                        timeout=0.0,
                        return_when=FIRST_COMPLETED,
                    )
                    if done:
                        handle_done_futures(done)
                        if limit is not None and processed >= limit:
                            break

                post_uid = clean_text(db_row[0])
                platform_post_id = clean_text(db_row[1])
                author = clean_text(db_row[2])
                created_at = clean_text(db_row[3])
                url = clean_text(db_row[4])
                raw_text = clean_text(db_row[5])

                total += 1
                if not post_uid or not raw_text:
                    skipped += 1
                    continue

                analysis_context = build_analysis_context_from_raw_text(raw_text)
                ai_row_meta = {
                    "id": platform_post_id,
                    "link": url,
                    "author": author,
                    "published_at": created_at,
                    "正文": raw_text,
                }

                attempts = mark_processing(
                    conn,
                    post_uid,
                    model=model,
                    prompt_version=prompt_version,
                )
                conn.commit()

                task = AiTaskContext(
                    row_index=total,
                    post_uid=post_uid,
                    attempts=attempts,
                    analysis_context=analysis_context,
                    ai_row_meta=ai_row_meta,
                )
                if verbose:
                    print(f"[{total}] call_api {post_uid}", flush=True)
                future = executor.submit(
                    run_one_ai_task,
                    task=task,
                    limiter=limiter,
                    heartbeat_seconds=heartbeat_seconds,
                    timeout_seconds=timeout_seconds,
                    ai_retries=ai_retries,
                    verbose=verbose,
                    api_key=api_key,
                    model=model,
                    base_url=base_url,
                    api_mode=api_mode,
                    ai_stream=ai_stream,
                    ai_temperature=ai_temperature,
                    ai_reasoning_effort=ai_reasoning_effort,
                    trace_out=trace_out,
                )
                inflight[future] = task

                while len(inflight) >= max_inflight:
                    done, _ = wait(
                        set(inflight.keys()),
                        return_when=FIRST_COMPLETED,
                    )
                    handle_done_futures(done)
                    if limit is not None and processed >= limit:
                        break
                if limit is not None and processed >= limit:
                    break

                if progress_every > 0 and total % progress_every == 0:
                    print(
                        f"[{total}] progress processed={processed} skipped={skipped} failed={failed} inflight={len(inflight)}",
                        flush=True,
                    )

            while inflight:
                done, _ = wait(
                    set(inflight.keys()),
                    return_when=FIRST_COMPLETED,
                )
                handle_done_futures(done)
                if limit is not None and processed >= limit:
                    continue

        print(
            json.dumps(
                {
                    "mode": "db_status",
                    "status_filter": status_value,
                    "selected_rows": selected_total,
                    "processed_rows": total,
                    "processed": processed,
                    "skipped": skipped,
                    "failed": failed,
                    "db": str(db_path.resolve()),
                    "model": model,
                    "prompt_version": prompt_version,
                    "base_url": base_url,
                    "api_mode": api_mode,
                    "ai_stream": ai_stream,
                    "ai_retries": ai_retries,
                    "ai_reasoning_effort": ai_reasoning_effort,
                    "ai_rpm": ai_rpm,
                    "trace_out": str(trace_out) if trace_out else "",
                },
                ensure_ascii=False,
            )
        )
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse Weibo CSV, call shared AI tagging logic, write posts/assertions into SQLite workdb."
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_PATH)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--author", default=DEFAULT_AUTHOR)
    parser.add_argument("--user-id", default=DEFAULT_USER_ID)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--api-key-env", default=None)
    parser.add_argument(
        "--api-mode",
        default=DEFAULT_API_MODE,
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
    )
    parser.add_argument("--ai-stream", action="store_true")
    parser.add_argument("--prompt-version", default=DEFAULT_PROMPT_VERSION_LOCAL)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--relevant-threshold", type=float, default=0.35)
    parser.add_argument("--reprocess", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--ai-timeout-sec", type=float, default=None)
    parser.add_argument("--ai-retries", type=int, default=DEFAULT_INGEST_AI_RETRIES)
    parser.add_argument("--ai-temperature", type=float, default=DEFAULT_AI_TEMPERATURE)
    parser.add_argument(
        "--ai-reasoning-effort",
        default=DEFAULT_AI_REASONING_EFFORT,
        choices=["none", "minimal", "low", "medium", "high", "xhigh"],
    )
    parser.add_argument("--ai-rpm", type=float, default=0.0)
    parser.add_argument(
        "--heartbeat-seconds", type=float, default=DEFAULT_HEARTBEAT_SECONDS
    )
    parser.add_argument("--ai-max-inflight", type=int, default=DEFAULT_AI_MAX_INFLIGHT)
    parser.add_argument("--trace-out", type=Path, default=None)
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--only-status",
        default=None,
        choices=["error", "processing", "new"],
        help="可选：仅从 DB 重跑指定状态，不读取 CSV。",
    )

    # Backward-compatible no-op args kept to avoid breaking old command snippets.
    parser.add_argument("--api-style", default="litellm")
    parser.add_argument("--api-type", default=None)
    parser.add_argument("--litellm-provider", default="")
    parser.add_argument("--gemini-auth", default="query")

    args = parser.parse_args()

    api_key = ""
    if args.api_key:
        api_key = str(args.api_key).strip()
    else:
        key_env = str(args.api_key_env or ENV_AI_API_KEY).strip()
        if key_env:
            api_key = os.getenv(key_env, "").strip()
        if not api_key and key_env != "GEMINI_API_KEY":
            api_key = os.getenv("GEMINI_API_KEY", "").strip()

    if not api_key:
        raise RuntimeError(
            f"Missing API key. Provide --api-key or set {ENV_AI_API_KEY} (fallback GEMINI_API_KEY)."
        )
    timeout_seconds = (
        args.ai_timeout_sec if args.ai_timeout_sec is not None else args.timeout_seconds
    )

    if args.only_status:
        process_db_status(
            db_path=args.db,
            api_key=api_key,
            model=args.model,
            prompt_version=args.prompt_version,
            limit=args.limit,
            sleep_seconds=args.sleep_seconds,
            only_status=str(args.only_status),
            relevant_threshold=max(0.0, min(1.0, args.relevant_threshold)),
            base_url=args.base_url,
            api_mode=args.api_mode,
            ai_stream=args.ai_stream,
            ai_retries=max(0, args.ai_retries),
            ai_temperature=args.ai_temperature,
            ai_reasoning_effort=args.ai_reasoning_effort,
            ai_rpm=max(0.0, args.ai_rpm),
            ai_max_inflight=max(1, args.ai_max_inflight),
            heartbeat_seconds=max(0.0, args.heartbeat_seconds),
            trace_out=args.trace_out,
            timeout_seconds=max(1.0, timeout_seconds),
            progress_every=max(0, args.progress_every),
            verbose=args.verbose,
        )
        return

    if not args.csv.exists():
        raise FileNotFoundError(f"CSV not found: {args.csv}")

    process_csv(
        csv_path=args.csv,
        db_path=args.db,
        api_key=api_key,
        model=args.model,
        prompt_version=args.prompt_version,
        author=args.author,
        user_id=args.user_id,
        limit=args.limit,
        sleep_seconds=args.sleep_seconds,
        reprocess=args.reprocess,
        relevant_threshold=max(0.0, min(1.0, args.relevant_threshold)),
        base_url=args.base_url,
        api_mode=args.api_mode,
        ai_stream=args.ai_stream,
        ai_retries=max(0, args.ai_retries),
        ai_temperature=args.ai_temperature,
        ai_reasoning_effort=args.ai_reasoning_effort,
        ai_rpm=max(0.0, args.ai_rpm),
        ai_max_inflight=max(1, args.ai_max_inflight),
        heartbeat_seconds=max(0.0, args.heartbeat_seconds),
        trace_out=args.trace_out,
        timeout_seconds=max(1.0, timeout_seconds),
        progress_every=max(0, args.progress_every),
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
