from __future__ import annotations

from dataclasses import dataclass
import hashlib
import logging
import os
import re
import sys
import threading
import time
from urllib.parse import quote

import requests

from alphavault.constants import (
    DEFAULT_NTFY_ALERT_DEDUP_WINDOW_SECONDS,
    DEFAULT_NTFY_ALERT_TIMEOUT_SECONDS,
    DEFAULT_NTFY_URL,
    ENV_NTFY_ALERT_DEDUP_WINDOW_SECONDS,
    ENV_NTFY_ALERT_TIMEOUT_SECONDS,
    ENV_NTFY_TOKEN,
    ENV_NTFY_TOPIC,
    ENV_NTFY_URL,
)

_ALERT_TITLE_PREFIX = "AlphaVault"
_WARNING_ALERT_MARKERS = (
    "_error",
    " error ",
    "error=",
    "failed",
    "crash",
    "exception",
)
_INTERNAL_LOGGER_PREFIXES = ("urllib3", "requests")
_MAX_BODY_MESSAGE_CHARS = 1200
_MAX_STACK_CHARS = 1800
_MAX_SUMMARY_MESSAGE_CHARS = 500
_URL_PATTERN = re.compile(r"https?://\S+")
_UUID_PATTERN = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_DATETIME_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\b")
_LONG_NUMBER_PATTERN = re.compile(r"\b\d{4,}\b")
_LONG_TOKEN_PATTERN = re.compile(r"\b[a-z0-9_-]{16,}\b", re.IGNORECASE)
_KEY_VALUE_PATTERNS = (
    re.compile(r"\b(post_uid|message_id|trace_id|consumer|queue|source)=[^\s]+"),
    re.compile(r"\b(author|root_key|base_url|url|path|alias_key)=[^\s]+"),
)
_EXCEPTION_FORMATTER = logging.Formatter()


@dataclass(frozen=True)
class NtfyAlertConfig:
    service_name: str
    server_url: str
    topic: str
    token: str
    dedup_window_seconds: int
    timeout_seconds: float


@dataclass(frozen=True)
class _AlertEvent:
    dedup_key: str
    fingerprint_id: str
    logger_name: str
    level_name: str
    levelno: int
    first_title: str
    first_body: str
    summary_message: str


@dataclass
class _AlertWindowState:
    fingerprint_id: str
    logger_name: str
    level_name: str
    count: int
    first_seen_at: float
    last_seen_at: float
    first_message: str
    last_message: str


def _limit_text(value: str, max_chars: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3]}..."


def _parse_positive_int(value: str, default: int) -> int:
    try:
        parsed = int(str(value or "").strip())
    except Exception:
        return int(default)
    if parsed <= 0:
        return int(default)
    return int(parsed)


def _parse_positive_float(value: str, default: float) -> float:
    try:
        parsed = float(str(value or "").strip())
    except Exception:
        return float(default)
    if parsed <= 0:
        return float(default)
    return float(parsed)


def _load_ntfy_alert_config(*, service_name: str) -> NtfyAlertConfig | None:
    topic = str(os.getenv(ENV_NTFY_TOPIC, "") or "").strip()
    if not topic:
        return None
    server_url = str(os.getenv(ENV_NTFY_URL, "") or "").strip() or DEFAULT_NTFY_URL
    server_url = server_url.rstrip("/")
    token = str(os.getenv(ENV_NTFY_TOKEN, "") or "").strip()
    dedup_window_seconds = _parse_positive_int(
        os.getenv(ENV_NTFY_ALERT_DEDUP_WINDOW_SECONDS, ""),
        DEFAULT_NTFY_ALERT_DEDUP_WINDOW_SECONDS,
    )
    timeout_seconds = _parse_positive_float(
        os.getenv(ENV_NTFY_ALERT_TIMEOUT_SECONDS, ""),
        DEFAULT_NTFY_ALERT_TIMEOUT_SECONDS,
    )
    resolved_service_name = str(service_name or "").strip() or "backend"
    return NtfyAlertConfig(
        service_name=resolved_service_name,
        server_url=server_url,
        topic=topic,
        token=token,
        dedup_window_seconds=dedup_window_seconds,
        timeout_seconds=timeout_seconds,
    )


def _should_alert_for_record(record: logging.LogRecord) -> bool:
    if record.name.startswith(_INTERNAL_LOGGER_PREFIXES):
        return False
    if record.levelno >= logging.ERROR:
        return True
    if record.levelno < logging.WARNING:
        return False
    lowered = f"{record.name} {record.getMessage()}".lower()
    return any(marker in lowered for marker in _WARNING_ALERT_MARKERS)


def _normalize_message_for_fingerprint(message: str) -> str:
    normalized = " ".join(str(message or "").split())
    for pattern in _KEY_VALUE_PATTERNS:
        normalized = pattern.sub(
            lambda match: f"{match.group(0).split('=', 1)[0]}=<value>",
            normalized,
        )
    normalized = _URL_PATTERN.sub("<url>", normalized)
    normalized = _UUID_PATTERN.sub("<uuid>", normalized)
    normalized = _DATETIME_PATTERN.sub("<time>", normalized)
    normalized = _LONG_NUMBER_PATTERN.sub("<number>", normalized)
    normalized = _LONG_TOKEN_PATTERN.sub("<token>", normalized)
    return _limit_text(normalized, 500)


def _format_exception_name(record: logging.LogRecord) -> str:
    if record.exc_info and record.exc_info[0] is not None:
        return str(record.exc_info[0].__name__ or "").strip()
    return ""


def _format_exception_stack(record: logging.LogRecord) -> str:
    if not record.exc_info:
        return ""
    try:
        return _limit_text(
            _EXCEPTION_FORMATTER.formatException(record.exc_info),
            _MAX_STACK_CHARS,
        )
    except Exception:
        return ""


def _build_fingerprint(
    *,
    config: NtfyAlertConfig,
    record: logging.LogRecord,
    message: str,
    exception_name: str,
) -> tuple[str, str]:
    raw = "\n".join(
        [
            config.service_name,
            record.name,
            str(record.lineno),
            _normalize_message_for_fingerprint(message),
            exception_name,
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return raw, digest


def _format_local_time(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _build_alert_event(
    *,
    config: NtfyAlertConfig,
    record: logging.LogRecord,
) -> _AlertEvent:
    message = _limit_text(record.getMessage(), _MAX_BODY_MESSAGE_CHARS)
    exception_name = _format_exception_name(record)
    fingerprint_raw, fingerprint_id = _build_fingerprint(
        config=config,
        record=record,
        message=message,
        exception_name=exception_name,
    )
    title = (
        f"{_ALERT_TITLE_PREFIX}/{config.service_name} {record.levelname} {record.name}"
    )
    if exception_name:
        title = f"{title} {exception_name}"
    body_lines = [
        f"service={config.service_name}",
        f"logger={record.name}",
        f"level={record.levelname}",
        f"fingerprint={fingerprint_id}",
        f"time={_format_local_time(time.time())}",
        "",
        message,
    ]
    stack = _format_exception_stack(record)
    if stack:
        body_lines.extend(["", stack])
    return _AlertEvent(
        dedup_key=fingerprint_raw,
        fingerprint_id=fingerprint_id,
        logger_name=record.name,
        level_name=record.levelname,
        levelno=record.levelno,
        first_title=title,
        first_body="\n".join(body_lines),
        summary_message=_limit_text(message, _MAX_SUMMARY_MESSAGE_CHARS),
    )


def _priority_for_level(levelno: int) -> str:
    if levelno >= logging.CRITICAL:
        return "urgent"
    if levelno >= logging.ERROR:
        return "high"
    return "default"


def _publish_alert(
    *,
    config: NtfyAlertConfig,
    title: str,
    body: str,
    priority: str,
) -> None:
    headers = {
        "Title": title,
        "Priority": priority,
        "Tags": "warning,rotating_light",
    }
    if config.token:
        headers["Authorization"] = f"Bearer {config.token}"
    url = f"{config.server_url}/{quote(config.topic, safe='')}"
    response = requests.post(
        url,
        data=body.encode("utf-8"),
        headers=headers,
        timeout=float(config.timeout_seconds),
    )
    response.raise_for_status()


class NtfyErrorAlertHandler(logging.Handler):
    def __init__(self, *, config: NtfyAlertConfig) -> None:
        super().__init__(level=logging.WARNING)
        self.config = config
        self._lock = threading.Lock()
        self._windows: dict[str, _AlertWindowState] = {}

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if not _should_alert_for_record(record):
                return
            event = _build_alert_event(config=self.config, record=record)
            self._record_event(event)
        except Exception as err:
            self._write_internal_error(err)

    def _record_event(self, event: _AlertEvent) -> None:
        timer_to_start: threading.Timer | None = None
        should_publish_first = False
        now = time.time()
        with self._lock:
            state = self._windows.get(event.dedup_key)
            if state is None:
                timer = threading.Timer(
                    float(self.config.dedup_window_seconds),
                    self._flush_window,
                    args=(event.dedup_key,),
                )
                timer.daemon = True
                self._windows[event.dedup_key] = _AlertWindowState(
                    fingerprint_id=event.fingerprint_id,
                    logger_name=event.logger_name,
                    level_name=event.level_name,
                    count=1,
                    first_seen_at=now,
                    last_seen_at=now,
                    first_message=event.summary_message,
                    last_message=event.summary_message,
                )
                timer_to_start = timer
                should_publish_first = True
            else:
                state.count += 1
                state.last_seen_at = now
                state.last_message = event.summary_message
        if timer_to_start is not None:
            timer_to_start.start()
        if should_publish_first:
            publish_thread = threading.Thread(
                target=self._publish_alert_safe,
                kwargs={
                    "title": event.first_title,
                    "body": event.first_body,
                    "priority": _priority_for_level(event.levelno),
                },
                daemon=True,
            )
            publish_thread.start()

    def _flush_window(self, dedup_key: str) -> None:
        with self._lock:
            state = self._windows.pop(dedup_key, None)
        if state is None or state.count <= 1:
            return
        title = (
            f"{_ALERT_TITLE_PREFIX}/{self.config.service_name} repeated "
            f"{state.level_name} {state.logger_name} x{state.count}"
        )
        body = "\n".join(
            [
                f"service={self.config.service_name}",
                f"logger={state.logger_name}",
                f"level={state.level_name}",
                f"fingerprint={state.fingerprint_id}",
                f"count={state.count}",
                f"window_seconds={self.config.dedup_window_seconds}",
                f"first_seen={_format_local_time(state.first_seen_at)}",
                f"last_seen={_format_local_time(state.last_seen_at)}",
                "",
                f"first_message={state.first_message}",
                f"last_message={state.last_message}",
            ]
        )
        try:
            self._publish_alert_safe(
                title=title,
                body=body,
                priority="default",
            )
        except Exception as err:
            self._write_internal_error(err)

    def _publish_alert_safe(
        self,
        *,
        title: str,
        body: str,
        priority: str,
    ) -> None:
        try:
            _publish_alert(
                config=self.config,
                title=title,
                body=body,
                priority=priority,
            )
        except Exception as err:
            self._write_internal_error(err)

    def _write_internal_error(self, err: Exception) -> None:
        try:
            sys.stderr.write(f"[ntfy] alert_send_error {type(err).__name__}: {err}\n")
        except Exception:
            return


def install_ntfy_error_alerting(*, service_name: str) -> bool:
    config = _load_ntfy_alert_config(service_name=service_name)
    if config is None:
        return False
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, NtfyErrorAlertHandler):
            if handler.config.service_name == config.service_name:
                return True
    root_logger.addHandler(NtfyErrorAlertHandler(config=config))
    return True


__all__ = [
    "NtfyAlertConfig",
    "NtfyErrorAlertHandler",
    "install_ntfy_error_alerting",
]
