from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from alphavault.ai._errors import (
    _append_trace,
    extract_retry_after_seconds,
    _mask_secret,
    extract_llm_error_details,
    format_llm_error_one_line,
)
from alphavault.ai._extract import _collect_streamed_ai_text, _extract_ai_text
from alphavault.ai._openai import _build_openai_client, _resolve_openai_model_name
from alphavault.ai._text import parse_json_text
from alphavault.logging_config import get_logger


DEFAULT_AI_RETRY_BACKOFF_SEC = 2.0
DEFAULT_AI_RETRY_MAX_BACKOFF_SEC = 32.0
DEFAULT_AI_RETRY_LOG_ERROR_LIMIT = 300
DEFAULT_AI_INLINE_RETRY_AFTER_SEC = 60
DEFAULT_AI_RETRY_AFTER_MAX_SECONDS = 24 * 60 * 60
logger = get_logger(__name__)


class AiInvalidJsonError(RuntimeError):
    """
    Raised when the model returns non-JSON text (or JSON parsing fails).

    Keep raw_ai_text for downstream logging (but callers should print only a short tail).
    """

    def __init__(self, message: str, *, raw_ai_text: str) -> None:
        super().__init__(message)
        self.raw_ai_text = str(raw_ai_text or "")


class AiValidationError(RuntimeError):
    """
    Raised when parsed JSON does not pass a caller-provided validator.

    Keep raw_ai_text for downstream logging (but callers should print only a short tail).
    """

    def __init__(self, message: str, *, raw_ai_text: str) -> None:
        super().__init__(message)
        self.raw_ai_text = str(raw_ai_text or "")


class AiRetryLaterError(RuntimeError):
    """
    Raised when the provider asks the caller to retry much later.

    The worker should hand the job back to the delayed retry queue instead of
    holding the consumer thread for a long cooldown window.
    """

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: int,
        raw_error: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = max(1, int(retry_after_seconds))
        if raw_error is None:
            return
        for attr_name in (
            "status_code",
            "provider",
            "llm_provider",
            "model",
            "request_id",
            "code",
            "type",
            "param",
            "response",
            "request",
        ):
            attr_value = getattr(raw_error, attr_name, None)
            if attr_value in (None, ""):
                continue
            setattr(self, attr_name, attr_value)


def _to_one_line_tail(text: str, *, max_chars: int) -> str:
    s = str(text or "").replace("\r", " ").replace("\n", " ").strip()
    if not s:
        return ""
    if len(s) <= int(max_chars):
        return s
    return s[-int(max_chars) :]


def _close_if_possible(resource: Any) -> None:
    close_fn = getattr(resource, "close", None)
    if callable(close_fn):
        close_fn()


def _load_raw_ai_text(response: Any, *, ai_stream: bool, api_mode: str) -> str:
    if not ai_stream:
        return _extract_ai_text(response)
    try:
        return _collect_streamed_ai_text(response, api_mode=api_mode)
    finally:
        _close_if_possible(response)


def _clamp_retry_after_seconds(retry_after_seconds: int | float) -> int:
    return max(
        1,
        min(int(retry_after_seconds), int(DEFAULT_AI_RETRY_AFTER_MAX_SECONDS)),
    )


def _call_ai_with_openai(
    *,
    prompt: str,
    api_mode: str,
    ai_stream: bool,
    model_name: str,
    base_url: str,
    api_key: str,
    timeout_seconds: float,
    retry_count: int,
    temperature: float,
    reasoning_effort: str,
    trace_out: Optional[Path],
    trace_label: str,
    validator: Optional[Callable[[Dict[str, Any]], None]] = None,
    request_gate: Optional[Callable[[], None]] = None,
) -> Dict[str, Any]:
    request_model_name = _resolve_openai_model_name(model_name)
    clean_reasoning_effort = str(reasoning_effort or "").strip()
    last_error: Optional[Exception] = None
    retries = max(0, int(retry_count))
    backoff_sec = DEFAULT_AI_RETRY_BACKOFF_SEC

    for attempt in range(retries + 1):
        raw_text = ""
        try:
            if request_gate is not None:
                request_gate()
            client = _build_openai_client(
                api_key=api_key,
                base_url=base_url,
                timeout_seconds=float(timeout_seconds),
            )
            try:
                response: Any
                if api_mode == "responses":
                    call_kwargs: Dict[str, Any] = {
                        "model": request_model_name,
                        "input": prompt,
                        "temperature": float(temperature),
                        "stream": bool(ai_stream),
                    }
                    if clean_reasoning_effort:
                        call_kwargs["reasoning"] = {"effort": clean_reasoning_effort}
                    response = client.responses.create(**call_kwargs)
                else:
                    call_kwargs = {
                        "model": request_model_name,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": float(temperature),
                        "stream": bool(ai_stream),
                    }
                    if clean_reasoning_effort:
                        call_kwargs["reasoning_effort"] = clean_reasoning_effort
                    response = client.chat.completions.create(**call_kwargs)
                raw_text = _load_raw_ai_text(
                    response,
                    ai_stream=bool(ai_stream),
                    api_mode=api_mode,
                )
            finally:
                _close_if_possible(client)

            try:
                parsed = parse_json_text(raw_text)
            except Exception as parse_exc:
                raise AiInvalidJsonError(
                    f"ai_invalid_json:{type(parse_exc).__name__}",
                    raw_ai_text=raw_text,
                ) from parse_exc

            if validator is not None:
                try:
                    validator(parsed)
                except Exception as val_exc:
                    raise AiValidationError(
                        f"ai_invalid_output:{type(val_exc).__name__}:{val_exc}",
                        raw_ai_text=raw_text,
                    ) from val_exc
            _append_trace(
                trace_out,
                {
                    "label": trace_label,
                    "attempt": attempt + 1,
                    "api_mode": api_mode,
                    "model": request_model_name,
                    "base_url": base_url,
                    "stream": bool(ai_stream),
                    "temperature": float(temperature),
                    "reasoning_effort": clean_reasoning_effort,
                    "timeout_seconds": float(timeout_seconds),
                    "api_key": _mask_secret(api_key),
                    "prompt_chars": len(prompt),
                    "raw_ai_text": raw_text,
                    "error": "",
                },
            )
            return parsed
        except Exception as exc:
            last_error = exc
            if isinstance(exc, AiValidationError):
                tail = _to_one_line_tail(getattr(exc, "raw_ai_text", ""), max_chars=240)
                msg = str(exc)[:400]
                logger.warning(
                    " ".join(
                        [
                            "[llm] validate_failed",
                            f"label={trace_label}",
                            f"attempt={attempt + 1}",
                            f"error={msg}",
                            f"raw_ai_tail={tail}",
                        ]
                    )
                )
            _append_trace(
                trace_out,
                {
                    "label": trace_label,
                    "attempt": attempt + 1,
                    "api_mode": api_mode,
                    "model": request_model_name,
                    "base_url": base_url,
                    "stream": bool(ai_stream),
                    "temperature": float(temperature),
                    "reasoning_effort": clean_reasoning_effort,
                    "timeout_seconds": float(timeout_seconds),
                    "api_key": _mask_secret(api_key),
                    "prompt_chars": len(prompt),
                    "raw_ai_text": raw_text,
                    "error": f"{type(exc).__name__}: {exc}",
                    "error_details": extract_llm_error_details(exc),
                },
            )
            retry_after_seconds = extract_retry_after_seconds(exc)
            if retry_after_seconds is not None:
                retry_after_seconds = _clamp_retry_after_seconds(retry_after_seconds)
                formatted_error = format_llm_error_one_line(
                    exc,
                    limit=DEFAULT_AI_RETRY_LOG_ERROR_LIMIT,
                )
                if (
                    retry_after_seconds > DEFAULT_AI_INLINE_RETRY_AFTER_SEC
                    or attempt >= retries
                ):
                    logger.info(
                        "[llm] request_defer label=%s attempt=%s retry_after_seconds=%s error=%s",
                        trace_label,
                        attempt + 1,
                        retry_after_seconds,
                        formatted_error,
                    )
                    raise AiRetryLaterError(
                        formatted_error,
                        retry_after_seconds=retry_after_seconds,
                        raw_error=exc,
                    ) from exc
            if attempt >= retries:
                logger.warning(
                    "[llm] request_failed label=%s attempts=%s error=%s",
                    trace_label,
                    attempt + 1,
                    format_llm_error_one_line(
                        exc,
                        limit=DEFAULT_AI_RETRY_LOG_ERROR_LIMIT,
                    ),
                )
                break
            if retry_after_seconds is not None:
                wait_seconds = float(retry_after_seconds)
            else:
                wait_seconds = min(backoff_sec, DEFAULT_AI_RETRY_MAX_BACKOFF_SEC)
            logger.warning(
                "[llm] request_retry label=%s attempt=%s next_attempt=%s wait_seconds=%s error=%s",
                trace_label,
                attempt + 1,
                attempt + 2,
                wait_seconds,
                format_llm_error_one_line(
                    exc,
                    limit=DEFAULT_AI_RETRY_LOG_ERROR_LIMIT,
                ),
            )
            time.sleep(wait_seconds)
            backoff_sec = min(backoff_sec * 2, DEFAULT_AI_RETRY_MAX_BACKOFF_SEC)

    assert last_error is not None
    raise last_error
