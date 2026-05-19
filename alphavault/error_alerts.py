from __future__ import annotations

from dataclasses import dataclass
import hashlib
import logging
import os
import re
import sys
import threading
import time
from urllib.parse import quote, urlsplit

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
_LLM_ALERT_LOGGER_NAME = "alphavault.ai._client"
_LLM_NTFY_MESSAGE_PREFIXES = (
    "[llm] request_retry label=",
    "[llm] request_failed label=",
    "[llm] request_defer label=",
)
_SUPPRESSED_LLM_NTFY_MESSAGE_MARKERS = (
    "aiinvalidjsonerror",
    "ai_invalid_json",
    "ratelimiterror",
    "status_code=429",
    "code=model_cooldown",
)
_SUSTAINED_LLM_NTFY_MESSAGE_MARKERS = (
    "auth_unavailable",
    "no auth available",
    "status_code=503",
)
_SUSTAINED_LLM_UNAVAILABLE_GROUP_LABEL = "llm_auth_unavailable_503"
_SUSTAINED_LLM_UNAVAILABLE_MIN_DURATION_SECONDS = 6 * 60 * 60
_SUSTAINED_LLM_UNAVAILABLE_REPEAT_COOLDOWN_SECONDS = 24 * 60 * 60
_SUSTAINED_LLM_UNAVAILABLE_RESET_AFTER_SECONDS = 2 * 60 * 60
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
_EXCEPTION_SIGNATURE_PATTERN = re.compile(
    r"(?P<exc>[A-Za-z_][\w.]*(?:Error|Exception)):\s*"
    r"(?P<msg>.+?)(?:\s+[A-Za-z_][A-Za-z0-9_]*=[^\s]+|$)"
)
_SOURCE_SCOPE_PATTERNS = (
    re.compile(r"\b(?:owner|source)=([^\s]+)"),
    re.compile(r"^\[[^:\]]+:([^\]]+)\]"),
)
_EXCEPTION_FORMATTER = logging.Formatter()


@dataclass(frozen=True)
class NtfyAlertConfig:
    publish_url: str
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
    group_label: str
    logger_name: str
    level_name: str
    levelno: int
    first_title: str
    first_body: str
    source_scope: str
    summary_message: str


@dataclass
class _AlertWindowState:
    fingerprint_id: str
    group_label: str
    logger_name: str
    level_name: str
    count: int
    first_seen_at: float
    last_seen_at: float
    first_message: str
    last_message: str
    source_scope: str


@dataclass
class _SustainedAlertState:
    fingerprint_id: str
    group_label: str
    logger_name: str
    level_name: str
    count: int
    first_seen_at: float
    last_seen_at: float
    last_notified_at: float
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


def _mask_long_token(match: re.Match[str]) -> str:
    value = str(match.group(0) or "")
    if any(char.isdigit() for char in value):
        return "<token>"
    return value


def _load_ntfy_alert_config(*, service_name: str) -> NtfyAlertConfig | None:
    raw_topic = str(os.getenv(ENV_NTFY_TOPIC, "") or "").strip()
    if not raw_topic:
        return None
    raw_server_url = str(os.getenv(ENV_NTFY_URL, "") or "").strip() or DEFAULT_NTFY_URL
    server_url, topic, publish_url = _resolve_ntfy_target(
        raw_server_url=raw_server_url,
        raw_topic=raw_topic,
    )
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
        publish_url=publish_url,
        service_name=resolved_service_name,
        server_url=server_url,
        topic=topic,
        token=token,
        dedup_window_seconds=dedup_window_seconds,
        timeout_seconds=timeout_seconds,
    )


def _resolve_ntfy_target(
    *,
    raw_server_url: str,
    raw_topic: str,
) -> tuple[str, str, str]:
    server_url = str(raw_server_url or "").strip() or DEFAULT_NTFY_URL
    server_url = server_url.rstrip("/")
    topic = str(raw_topic or "").strip().strip("/")
    if topic.startswith(("http://", "https://")):
        parsed = urlsplit(topic)
        resolved_server_url = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        resolved_topic = parsed.path.strip("/") or parsed.netloc
        return resolved_server_url, resolved_topic, topic

    parsed_server = urlsplit(server_url)
    server_scheme = parsed_server.scheme or "https"
    server_host = parsed_server.netloc or parsed_server.path
    topic_head, slash, topic_tail = topic.partition("/")
    if slash and "." in topic_head and topic_tail:
        resolved_server_url = f"{server_scheme}://{topic_head}".rstrip("/")
        resolved_topic = topic_tail.strip("/")
        publish_url = f"{resolved_server_url}/{quote(resolved_topic, safe='/')}"
        return resolved_server_url, resolved_topic, publish_url

    publish_url = f"{server_url}/{quote(topic, safe='/')}"
    if slash and server_host and topic_head == server_host and topic_tail:
        resolved_topic = topic_tail.strip("/")
        publish_url = f"{server_url}/{quote(resolved_topic, safe='/')}"
        return server_url, resolved_topic, publish_url
    return server_url, topic, publish_url


def _should_alert_for_record(record: logging.LogRecord) -> bool:
    if record.name.startswith(_INTERNAL_LOGGER_PREFIXES):
        return False
    message = record.getMessage()
    if _should_suppress_llm_ntfy_alert(record=record, message=message):
        return False
    if record.levelno >= logging.ERROR:
        return True
    if record.levelno < logging.WARNING:
        return False
    lowered = f"{record.name} {message}".lower()
    return any(marker in lowered for marker in _WARNING_ALERT_MARKERS)


def _should_suppress_llm_ntfy_alert(
    *,
    record: logging.LogRecord,
    message: str,
) -> bool:
    return _matches_llm_ntfy_message(
        record=record,
        message=message,
        markers=_SUPPRESSED_LLM_NTFY_MESSAGE_MARKERS,
    )


def _should_use_sustained_llm_ntfy_alert(
    *,
    record: logging.LogRecord,
    message: str,
) -> bool:
    return _matches_llm_ntfy_message(
        record=record,
        message=message,
        markers=_SUSTAINED_LLM_NTFY_MESSAGE_MARKERS,
    )


def _matches_llm_ntfy_message(
    *,
    record: logging.LogRecord,
    message: str,
    markers: tuple[str, ...],
) -> bool:
    if record.name != _LLM_ALERT_LOGGER_NAME:
        return False
    if not any(message.startswith(prefix) for prefix in _LLM_NTFY_MESSAGE_PREFIXES):
        return False
    lowered_message = message.lower()
    return any(marker in lowered_message for marker in markers)


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
    normalized = _LONG_TOKEN_PATTERN.sub(_mask_long_token, normalized)
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


def _extract_exception_signature(
    *,
    message: str,
    record: logging.LogRecord,
    exception_name: str,
) -> str:
    if record.exc_info and len(record.exc_info) >= 2 and record.exc_info[1] is not None:
        exc_message = _limit_text(
            _normalize_message_for_fingerprint(str(record.exc_info[1])),
            240,
        )
        if exception_name and exc_message:
            return f"{exception_name}: {exc_message}"
        if exception_name:
            return exception_name

    matches = list(_EXCEPTION_SIGNATURE_PATTERN.finditer(message))
    if not matches:
        return exception_name
    matched = matches[-1]
    matched_name = str(matched.group("exc") or "").strip()
    matched_message = _limit_text(
        _normalize_message_for_fingerprint(str(matched.group("msg") or "")),
        240,
    )
    if matched_name and matched_message:
        return f"{matched_name}: {matched_message}"
    return matched_name or exception_name


def _extract_source_scope(message: str) -> str:
    for pattern in _SOURCE_SCOPE_PATTERNS:
        matched = pattern.search(message)
        if matched is None:
            continue
        value = _limit_text(str(matched.group(1) or "").strip(), 80)
        if value:
            return value
    return ""


def _build_fingerprint(
    *,
    config: NtfyAlertConfig,
    record: logging.LogRecord,
    message: str,
    exception_name: str,
) -> tuple[str, str, str, str]:
    root_signature = _extract_exception_signature(
        message=message,
        record=record,
        exception_name=exception_name,
    )
    source_scope = _extract_source_scope(message)
    if root_signature:
        raw_parts = [config.service_name, source_scope, root_signature]
        group_label = root_signature
    else:
        raw_parts = [
            config.service_name,
            record.name,
            str(record.lineno),
            _normalize_message_for_fingerprint(message),
            source_scope,
            exception_name,
        ]
        group_label = record.name
    raw = "\n".join(raw_parts)
    digest = _fingerprint_digest(raw)
    return raw, digest, group_label, source_scope


def _format_local_time(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _fingerprint_digest(raw: str) -> str:
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _build_alert_event(
    *,
    config: NtfyAlertConfig,
    record: logging.LogRecord,
) -> _AlertEvent:
    message = _limit_text(record.getMessage(), _MAX_BODY_MESSAGE_CHARS)
    exception_name = _format_exception_name(record)
    fingerprint_raw, fingerprint_id, group_label, source_scope = _build_fingerprint(
        config=config,
        record=record,
        message=message,
        exception_name=exception_name,
    )
    title = (
        f"{_ALERT_TITLE_PREFIX}/{config.service_name} {record.levelname} {group_label}"
    )
    if source_scope:
        title = f"{title} ({source_scope})"
    body_lines = [
        f"service={config.service_name}",
        f"logger={record.name}",
        f"level={record.levelname}",
        f"fingerprint={fingerprint_id}",
        f"group={group_label}",
        f"source_scope={source_scope or '(empty)'}",
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
        group_label=group_label,
        logger_name=record.name,
        level_name=record.levelname,
        levelno=record.levelno,
        first_title=title,
        first_body="\n".join(body_lines),
        source_scope=source_scope,
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
    response = requests.post(
        config.publish_url,
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
        self._sustained_windows: dict[str, _SustainedAlertState] = {}

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
            if _should_use_sustained_llm_ntfy_alert(record=record, message=message):
                self._record_sustained_llm_alert(record=record, message=message)
                return
            if not _should_alert_for_record(record):
                return
            event = _build_alert_event(config=self.config, record=record)
            self._record_event(event)
        except Exception as err:
            self._write_internal_error(err)

    def _record_sustained_llm_alert(
        self,
        *,
        record: logging.LogRecord,
        message: str,
    ) -> None:
        summary_message = _limit_text(message, _MAX_SUMMARY_MESSAGE_CHARS)
        dedup_key = "\n".join(
            (
                self.config.service_name,
                record.name,
                _SUSTAINED_LLM_UNAVAILABLE_GROUP_LABEL,
            )
        )
        now = time.time()
        state_to_publish: _SustainedAlertState | None = None
        with self._lock:
            state = self._sustained_windows.get(dedup_key)
            if (
                state is None
                or now - state.last_seen_at
                > _SUSTAINED_LLM_UNAVAILABLE_RESET_AFTER_SECONDS
            ):
                self._sustained_windows[dedup_key] = _SustainedAlertState(
                    fingerprint_id=_fingerprint_digest(dedup_key),
                    group_label=_SUSTAINED_LLM_UNAVAILABLE_GROUP_LABEL,
                    logger_name=record.name,
                    level_name=record.levelname,
                    count=1,
                    first_seen_at=now,
                    last_seen_at=now,
                    last_notified_at=0.0,
                    first_message=summary_message,
                    last_message=summary_message,
                )
                return
            state.count += 1
            state.last_seen_at = now
            state.last_message = summary_message
            if (
                now - state.first_seen_at
                < _SUSTAINED_LLM_UNAVAILABLE_MIN_DURATION_SECONDS
            ):
                return
            if (
                state.last_notified_at > 0
                and now - state.last_notified_at
                < _SUSTAINED_LLM_UNAVAILABLE_REPEAT_COOLDOWN_SECONDS
            ):
                return
            state.last_notified_at = now
            state_to_publish = _SustainedAlertState(
                fingerprint_id=state.fingerprint_id,
                group_label=state.group_label,
                logger_name=state.logger_name,
                level_name=state.level_name,
                count=state.count,
                first_seen_at=state.first_seen_at,
                last_seen_at=state.last_seen_at,
                last_notified_at=state.last_notified_at,
                first_message=state.first_message,
                last_message=state.last_message,
            )
        if state_to_publish is None:
            return
        publish_thread = threading.Thread(
            target=self._publish_sustained_llm_alert_safe,
            kwargs={"state": state_to_publish},
            daemon=True,
        )
        publish_thread.start()

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
                    group_label=event.group_label,
                    logger_name=event.logger_name,
                    level_name=event.level_name,
                    count=1,
                    first_seen_at=now,
                    last_seen_at=now,
                    first_message=event.summary_message,
                    last_message=event.summary_message,
                    source_scope=event.source_scope,
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
        title = f"{_ALERT_TITLE_PREFIX}/{self.config.service_name} repeated {state.group_label} x{state.count}"
        if state.source_scope:
            title = f"{title} ({state.source_scope})"
        body = "\n".join(
            [
                f"service={self.config.service_name}",
                f"logger={state.logger_name}",
                f"level={state.level_name}",
                f"fingerprint={state.fingerprint_id}",
                f"group={state.group_label}",
                f"source_scope={state.source_scope or '(empty)'}",
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

    def _publish_sustained_llm_alert_safe(
        self,
        *,
        state: _SustainedAlertState,
    ) -> None:
        duration_seconds = max(1, int(state.last_seen_at - state.first_seen_at))
        title = (
            f"{_ALERT_TITLE_PREFIX}/{self.config.service_name} "
            f"sustained {state.group_label} x{state.count}"
        )
        body = "\n".join(
            [
                f"service={self.config.service_name}",
                f"logger={state.logger_name}",
                f"level={state.level_name}",
                f"fingerprint={state.fingerprint_id}",
                f"group={state.group_label}",
                f"count={state.count}",
                f"duration_seconds={duration_seconds}",
                (
                    "notify_after_seconds="
                    f"{_SUSTAINED_LLM_UNAVAILABLE_MIN_DURATION_SECONDS}"
                ),
                (
                    "repeat_cooldown_seconds="
                    f"{_SUSTAINED_LLM_UNAVAILABLE_REPEAT_COOLDOWN_SECONDS}"
                ),
                f"first_seen={_format_local_time(state.first_seen_at)}",
                f"last_seen={_format_local_time(state.last_seen_at)}",
                "",
                f"first_message={state.first_message}",
                f"last_message={state.last_message}",
            ]
        )
        self._publish_alert_safe(title=title, body=body, priority="high")

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
