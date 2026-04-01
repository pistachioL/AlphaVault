from __future__ import annotations

from concurrent.futures import Future
import json
import threading
from pathlib import Path
from typing import Any, Callable, Sequence


BACKFILL_MAX_STOCKS_PER_RUN_CAP = 32


def compute_low_priority_budget(*, ai_cap: int, rss_inflight_now: int) -> int:
    return max(0, int(ai_cap) - max(0, int(rss_inflight_now)))


def compute_backfill_max_stocks_per_run(*, low_budget: int) -> int:
    return max(
        1,
        min(BACKFILL_MAX_STOCKS_PER_RUN_CAP, max(0, int(low_budget))),
    )


def compute_rss_available_slots(
    *,
    ai_cap: int,
    rss_inflight_now: int,
    low_inflight_now: int,
) -> int:
    remaining = (
        int(ai_cap) - max(0, int(rss_inflight_now)) - max(0, int(low_inflight_now))
    )
    return max(0, remaining)


def build_low_priority_should_continue(
    *,
    ai_cap: int,
    rss_inflight_now_get: Callable[[], int],
    low_inflight_now_get: Callable[[], int] | None = None,
    has_due_ai_pending_get: Callable[[], bool] | None = None,
) -> Callable[[], bool]:
    def _should_continue() -> bool:
        rss_now = max(0, int(rss_inflight_now_get()))
        low_now = (
            max(0, int(low_inflight_now_get()))
            if low_inflight_now_get is not None
            else 0
        )
        available = compute_rss_available_slots(
            ai_cap=int(ai_cap),
            rss_inflight_now=int(rss_now),
            low_inflight_now=int(low_now),
        )
        if available > 0:
            return True
        if has_due_ai_pending_get is None:
            return False
        return not bool(has_due_ai_pending_get())

    return _should_continue


class LowPriorityAiSlotGate:
    def __init__(self, *, cap_getter: Callable[[], int]) -> None:
        self._cap_getter = cap_getter
        self._lock = threading.Lock()
        self._inflight = 0

    def try_acquire(self) -> bool:
        try:
            cap_now = max(0, int(self._cap_getter()))
        except Exception:
            cap_now = 0
        with self._lock:
            if cap_now <= 0 or int(self._inflight) >= int(cap_now):
                return False
            self._inflight += 1
            return True

    def release(self) -> None:
        with self._lock:
            if self._inflight <= 0:
                self._inflight = 0
                return
            self._inflight -= 1

    def inflight(self) -> int:
        with self._lock:
            return int(self._inflight)


def dedup_post_uids(post_uids: Sequence[object]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in post_uids:
        post_uid = str(value or "").strip()
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        deduped.append(post_uid)
    return deduped


def schedule_ai_from_redis(
    *,
    executor: Any,
    engine: Any,
    ai_cap: int,
    low_inflight_now_get: Callable[[], int],
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
    inflight_owner: str,
    wakeup_event: Any,
    config: Any,
    limiter: Any,
    verbose: bool,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    spool_dir: Path | str,
    prune_inflight_futures_fn: Callable[[set[Future], dict[Future, str]], None],
    compute_rss_available_slots_fn: Callable[..., int],
    pop_to_processing_fn: Callable[..., str | None],
    ack_processing_fn: Callable[..., None],
    process_one_redis_payload_fn: Callable[..., None],
    fatal_exceptions: tuple[type[BaseException], ...],
) -> tuple[int, bool]:
    prune_inflight_futures_fn(inflight_futures, inflight_owner_by_future)
    rss_inflight_now = int(len(inflight_futures))
    try:
        low_inflight_now = max(0, int(low_inflight_now_get()))
    except Exception:
        low_inflight_now = 0
    available = compute_rss_available_slots_fn(
        ai_cap=int(ai_cap),
        rss_inflight_now=int(rss_inflight_now),
        low_inflight_now=int(low_inflight_now),
    )
    if available <= 0:
        return 0, False

    scheduled = 0
    for _ in range(max(1, int(available))):
        if scheduled >= available:
            break
        try:
            msg = pop_to_processing_fn(redis_client, redis_queue_key)
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            if verbose:
                print(
                    f"[ai] redis_pop_error owner={inflight_owner} {type(err).__name__}: {err}",
                    flush=True,
                )
            return scheduled, True
        if not msg:
            break
        try:
            payload = json.loads(msg)
        except Exception as err:
            if verbose:
                print(f"[ai] redis_bad_payload {type(err).__name__}: {err}", flush=True)
            try:
                ack_processing_fn(redis_client, redis_queue_key, str(msg))
            except Exception:
                pass
            continue
        if not isinstance(payload, dict):
            try:
                ack_processing_fn(redis_client, redis_queue_key, str(msg))
            except Exception:
                pass
            continue
        if not str(payload.get("post_uid") or "").strip():
            try:
                ack_processing_fn(redis_client, redis_queue_key, str(msg))
            except Exception:
                pass
            continue

        fut = executor.submit(
            process_one_redis_payload_fn,
            engine=engine,
            payload=payload,
            processing_msg=str(msg),
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            source_name=str(source_name or "").strip(),
            spool_dir=spool_dir,
            config=config,
            limiter=limiter,
            verbose=bool(verbose),
        )
        fut.add_done_callback(lambda _f: wakeup_event.set())
        inflight_futures.add(fut)
        inflight_owner_by_future[fut] = str(inflight_owner or "").strip()
        scheduled += 1
    return scheduled, False


def schedule_ai(
    *,
    executor: Any,
    engine: Any,
    platform: str,
    ai_cap: int,
    low_inflight_now_get: Callable[[], int],
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
    inflight_owner: str,
    wakeup_event: Any,
    config: Any,
    limiter: Any,
    verbose: bool,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    spool_dir: Path | None,
    schedule_ai_from_redis_fn: Callable[..., tuple[int, bool]],
    prune_inflight_futures_fn: Callable[[set[Future], dict[Future, str]], None],
    compute_rss_available_slots_fn: Callable[..., int],
    select_due_post_uids_fn: Callable[..., Sequence[object]],
    dedup_post_uids_fn: Callable[[Sequence[object]], list[str]],
    try_mark_ai_running_fn: Callable[..., bool],
    process_one_post_uid_fn: Callable[..., bool],
    maybe_dispose_turso_engine_on_transient_error_fn: Callable[..., None],
    now_epoch_fn: Callable[[], int],
    fatal_exceptions: tuple[type[BaseException], ...],
) -> tuple[int, bool]:
    if engine is None:
        return 0, False
    if redis_client and str(redis_queue_key or "").strip() and spool_dir is not None:
        return schedule_ai_from_redis_fn(
            executor=executor,
            engine=engine,
            ai_cap=ai_cap,
            low_inflight_now_get=low_inflight_now_get,
            inflight_futures=inflight_futures,
            inflight_owner_by_future=inflight_owner_by_future,
            inflight_owner=inflight_owner,
            wakeup_event=wakeup_event,
            config=config,
            limiter=limiter,
            verbose=bool(verbose),
            redis_client=redis_client,
            redis_queue_key=str(redis_queue_key),
            source_name=str(source_name or "").strip(),
            spool_dir=spool_dir,
        )

    prune_inflight_futures_fn(inflight_futures, inflight_owner_by_future)
    rss_inflight_now = int(len(inflight_futures))
    try:
        low_inflight_now = max(0, int(low_inflight_now_get()))
    except Exception:
        low_inflight_now = 0
    available = compute_rss_available_slots_fn(
        ai_cap=int(ai_cap),
        rss_inflight_now=int(rss_inflight_now),
        low_inflight_now=int(low_inflight_now),
    )
    if available <= 0:
        return 0, False
    now_epoch = int(now_epoch_fn())
    try:
        due = select_due_post_uids_fn(
            engine,
            now_epoch=now_epoch,
            limit=max(1, int(available) * 2),
            platform=str(platform or "").strip().lower() or None,
        )
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        maybe_dispose_turso_engine_on_transient_error_fn(
            engine=engine, err=err, verbose=bool(verbose)
        )
        if verbose:
            print(
                f"[ai] select_due_error owner={inflight_owner} platform={platform} "
                f"{type(err).__name__}: {err}",
                flush=True,
            )
        return 0, True

    raw_due = list(due or [])
    due_list = dedup_post_uids_fn(raw_due)
    if verbose and len(due_list) < len(raw_due):
        print(
            f"[ai] due_dedup owner={inflight_owner} platform={platform} "
            f"before={len(raw_due)} after={len(due_list)}",
            flush=True,
        )

    scheduled = 0
    for post_uid in due_list:
        if scheduled >= available:
            break
        try:
            ok = try_mark_ai_running_fn(engine, post_uid=post_uid, now_epoch=now_epoch)
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            maybe_dispose_turso_engine_on_transient_error_fn(
                engine=engine, err=err, verbose=bool(verbose)
            )
            if verbose:
                print(
                    f"[ai] mark_running_error owner={inflight_owner} platform={platform} "
                    f"{type(err).__name__}: {err}",
                    flush=True,
                )
            return scheduled, True
        if not ok:
            continue
        fut = executor.submit(
            process_one_post_uid_fn,
            engine=engine,
            post_uid=post_uid,
            config=config,
            limiter=limiter,
            source_name=str(source_name or "").strip(),
        )
        fut.add_done_callback(lambda _f: wakeup_event.set())
        inflight_futures.add(fut)
        inflight_owner_by_future[fut] = str(inflight_owner or "").strip()
        scheduled += 1
    return scheduled, False


__all__ = [
    "BACKFILL_MAX_STOCKS_PER_RUN_CAP",
    "LowPriorityAiSlotGate",
    "build_low_priority_should_continue",
    "compute_backfill_max_stocks_per_run",
    "compute_low_priority_budget",
    "compute_rss_available_slots",
    "dedup_post_uids",
    "schedule_ai",
    "schedule_ai_from_redis",
]
