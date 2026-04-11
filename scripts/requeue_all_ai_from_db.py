from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# This script supports direct execution from `scripts/`, so it must expose the
# project root before importing local packages.
import requests  # noqa: E402

from alphavault.logging_config import (  # noqa: E402
    add_log_level_argument,
    configure_logging,
    get_logger,
)

MANUAL_DB_REQUEUE_API_PATH = "/api/admin/requeue-from-db"
DEFAULT_BASE_URL = "http://127.0.0.1:8080"
DEFAULT_LIMIT = 200
DEFAULT_SLEEP_SECONDS = 5.0
DEFAULT_MAX_ROUNDS = 200
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MODE_ORDER = "failed,legacy_unprocessed"
VALID_MODES = ("failed", "legacy_unprocessed")
logger = get_logger(__name__)


def load_dotenv_if_present() -> None:
    from alphavault.env import load_dotenv_if_present as _load_dotenv_if_present

    _load_dotenv_if_present()


def _worker_admin_trigger_key_env_name() -> str:
    from alphavault.constants import ENV_WORKER_ADMIN_TRIGGER_KEY

    return ENV_WORKER_ADMIN_TRIGGER_KEY


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="循环补送 AI DB 重排任务")
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="服务地址，默认 http://127.0.0.1:8080",
    )
    parser.add_argument(
        "--key",
        default="",
        help=(f"admin key；为空时读环境变量 {_worker_admin_trigger_key_env_name()}"),
    )
    parser.add_argument(
        "--platform",
        default="",
        help="可选：只补某个平台，比如 weibo 或 xueqiu；为空表示都跑",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="每轮最多补多少条",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="每轮没跑完时，下一轮前等几秒",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=DEFAULT_MAX_ROUNDS,
        help="每个 mode 最多跑几轮，防止一直转",
    )
    parser.add_argument(
        "--mode-order",
        default=DEFAULT_MODE_ORDER,
        help="mode 顺序，逗号分隔；默认 failed,legacy_unprocessed",
    )
    add_log_level_argument(parser)
    return parser.parse_args()


def _normalize_base_url(base_url: str) -> str:
    resolved = str(base_url or "").strip()
    if not resolved:
        raise RuntimeError("missing_base_url")
    return resolved.rstrip("/")


def _resolve_key(cli_key: str) -> str:
    resolved = str(cli_key or "").strip()
    if resolved:
        return resolved
    env_name = _worker_admin_trigger_key_env_name()
    resolved = os.getenv(env_name, "").strip()
    if resolved:
        return resolved
    raise RuntimeError(f"missing_admin_key env={env_name}")


def _normalize_modes(raw_mode_order: str) -> list[str]:
    seen: set[str] = set()
    resolved: list[str] = []
    for raw_mode in str(raw_mode_order or "").split(","):
        mode = str(raw_mode or "").strip().lower()
        if not mode:
            continue
        if mode not in VALID_MODES:
            raise RuntimeError(f"invalid_mode:{mode}")
        if mode in seen:
            continue
        seen.add(mode)
        resolved.append(mode)
    if not resolved:
        raise RuntimeError("missing_mode_order")
    return resolved


def _payload_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(text)
    except Exception:
        return 0


def _sum_queue_backlog(payload: dict[str, Any]) -> int:
    sources = payload.get("sources")
    if not isinstance(sources, list):
        return 0
    total = 0
    for source in sources:
        if not isinstance(source, dict):
            continue
        total += _payload_int(source, "queue_backlog")
    return total


def _source_error(payload: dict[str, Any]) -> str:
    sources = payload.get("sources")
    if not isinstance(sources, list):
        return ""
    for source in sources:
        if not isinstance(source, dict):
            continue
        text = str(source.get("error") or "").strip()
        if text:
            source_name = str(source.get("source") or "").strip() or "unknown"
            return f"source_error source={source_name} error={text}"
    return ""


def _ensure_non_empty_sources(payload: dict[str, Any], *, mode: str) -> None:
    sources = payload.get("sources")
    if not isinstance(sources, list) or not sources:
        raise RuntimeError(f"empty_sources mode={mode}")


def _request_requeue(
    *,
    base_url: str,
    key: str,
    platform: str | None,
    limit: int,
    dry_run: bool,
    mode: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "key": str(key),
        "mode": str(mode),
        "limit": max(1, int(limit)),
    }
    if platform:
        params["platform"] = str(platform)
    if dry_run:
        params["dry_run"] = "1"

    response = requests.get(
        f"{_normalize_base_url(base_url)}{MANUAL_DB_REQUEUE_API_PATH}",
        params=params,
        timeout=max(1.0, float(timeout_seconds)),
    )
    if response.status_code != 200:
        body = str(getattr(response, "text", "") or "").strip()
        body = body[:300]
        raise RuntimeError(
            f"http_error status={response.status_code} mode={mode} body={body}"
        )

    try:
        payload = response.json()
    except Exception as err:
        raise RuntimeError(f"invalid_json mode={mode} {type(err).__name__}") from err
    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid_payload mode={mode}")
    if payload.get("ok") is not True:
        raise RuntimeError(
            f"api_not_ok mode={mode} error={str(payload.get('error') or '').strip()}"
        )
    return payload


def _run_mode(
    *,
    base_url: str,
    key: str,
    platform: str | None,
    limit: int,
    sleep_seconds: float,
    max_rounds: int,
    mode: str,
    timeout_seconds: float,
) -> None:
    for round_no in range(1, max(1, int(max_rounds)) + 1):
        logger.info(
            "[requeue] start mode=%s round=%s limit=%s",
            mode,
            round_no,
            limit,
        )
        result = _request_requeue(
            base_url=base_url,
            key=key,
            platform=platform,
            limit=limit,
            dry_run=False,
            mode=mode,
            timeout_seconds=timeout_seconds,
        )
        _ensure_non_empty_sources(result, mode=mode)
        apply_error = _source_error(result)
        if apply_error:
            raise RuntimeError(f"{apply_error} phase=apply mode={mode}")
        logger.info(
            "[requeue] applied mode=%s round=%s scanned_total=%s enqueued_total=%s",
            mode,
            round_no,
            _payload_int(result, "scanned_total"),
            _payload_int(result, "enqueued_total"),
        )

        dry_run_result = _request_requeue(
            base_url=base_url,
            key=key,
            platform=platform,
            limit=limit,
            dry_run=True,
            mode=mode,
            timeout_seconds=timeout_seconds,
        )
        _ensure_non_empty_sources(dry_run_result, mode=mode)
        dry_run_error = _source_error(dry_run_result)
        if dry_run_error:
            raise RuntimeError(f"{dry_run_error} phase=dry_run mode={mode}")
        scanned_total = _payload_int(dry_run_result, "scanned_total")
        queue_backlog = _sum_queue_backlog(dry_run_result)
        logger.info(
            "[requeue] check mode=%s round=%s scanned_total=%s queue_backlog=%s",
            mode,
            round_no,
            scanned_total,
            queue_backlog,
        )
        if scanned_total == 0:
            logger.info("[requeue] done mode=%s rounds=%s", mode, round_no)
            return
        if round_no >= max(1, int(max_rounds)):
            raise RuntimeError(
                f"max_rounds_reached mode={mode} scanned_total={scanned_total} "
                f"queue_backlog={queue_backlog}"
            )
        if float(sleep_seconds) > 0:
            time.sleep(float(sleep_seconds))


def main() -> int:
    load_dotenv_if_present()
    args = parse_args()
    configure_logging(level=getattr(args, "log_level", ""))
    base_url = _normalize_base_url(str(args.base_url or ""))
    key = _resolve_key(str(args.key or ""))
    platform_text = str(args.platform or "").strip().lower()
    platform = platform_text or None
    limit = max(1, int(args.limit))
    sleep_seconds = max(0.0, float(args.sleep_seconds))
    max_rounds = max(1, int(args.max_rounds))
    timeout_seconds = max(
        1.0, float(getattr(args, "timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    )
    mode_order = _normalize_modes(str(args.mode_order or ""))

    logger.info(
        "[requeue] config base_url=%s platform=%s limit=%s sleep_seconds=%s "
        "max_rounds=%s mode_order=%s",
        base_url,
        platform or "all",
        limit,
        sleep_seconds,
        max_rounds,
        ",".join(mode_order),
    )

    for mode in mode_order:
        _run_mode(
            base_url=base_url,
            key=key,
            platform=platform,
            limit=limit,
            sleep_seconds=sleep_seconds,
            max_rounds=max_rounds,
            mode=mode,
            timeout_seconds=timeout_seconds,
        )

    logger.info("[requeue] all_done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
