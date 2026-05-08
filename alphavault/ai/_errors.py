from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from alphavault.ai._text import clean_text

_RETRY_AFTER_SECONDS_PATTERNS = (
    re.compile(r"""["']reset_seconds["']\s*[:=]\s*(\d+)""", re.IGNORECASE),
    re.compile(r"""["']retry_after_seconds["']\s*[:=]\s*(\d+)""", re.IGNORECASE),
    re.compile(r"""\bretry_after_seconds\s*[:=]\s*(\d+)""", re.IGNORECASE),
    re.compile(r"""\bretry[-_ ]after\s*[:=]?\s*(\d+)""", re.IGNORECASE),
)
_ERROR_CODE_PATTERNS = (
    re.compile(r"""["']code["']\s*[:=]\s*["']([^"']+)["']""", re.IGNORECASE),
    re.compile(r"""\bcode=([A-Za-z0-9_.-]+)""", re.IGNORECASE),
)


def _mask_secret(value: Any) -> str:
    secret = str(value or "").strip()
    if not secret:
        return ""
    if len(secret) <= 8:
        return "*" * len(secret)
    return f"{secret[:4]}***{secret[-4:]}"


def _compact_text(value: Any) -> str:
    # Keep it one line for logs / db.
    text = clean_text(value)
    return " ".join(text.split())


def _truncate_text(text: str, limit: int) -> str:
    n = max(1, int(limit))
    if len(text) <= n:
        return text
    if n <= 3:
        return text[:n]
    return text[: n - 3] + "..."


def _extract_retry_after_seconds_from_text(text: str) -> int | None:
    compact_text = _compact_text(text)
    if not compact_text:
        return None
    for pattern in _RETRY_AFTER_SECONDS_PATTERNS:
        matched = pattern.search(compact_text)
        if matched is None:
            continue
        try:
            parsed = int(str(matched.group(1) or "").strip())
        except Exception:
            continue
        if parsed > 0:
            return parsed
    return None


def _extract_error_code_from_text(text: str) -> str:
    compact_text = _compact_text(text)
    if not compact_text:
        return ""
    for pattern in _ERROR_CODE_PATTERNS:
        matched = pattern.search(compact_text)
        if matched is None:
            continue
        code = _compact_text(matched.group(1))
        if code:
            return code
    return ""


def extract_llm_error_details(exc: BaseException) -> Dict[str, Any]:
    """
    Extract JSON-safe error details for tracing.
    Keep this conservative: never raise, never include secrets.
    """
    details: Dict[str, Any] = {}
    try:
        details["type"] = type(exc).__name__
        details["message"] = _compact_text(str(exc))

        status_code = getattr(exc, "status_code", None)
        if status_code is None:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
        if status_code is not None:
            details["status_code"] = int(status_code)

        provider = getattr(exc, "llm_provider", None) or getattr(exc, "provider", None)
        if provider:
            details["provider"] = _compact_text(provider)

        model = getattr(exc, "model", None)
        if model:
            details["model"] = _compact_text(model)

        request_id = getattr(exc, "request_id", None)
        if not request_id:
            response = getattr(exc, "response", None)
            headers = getattr(response, "headers", None)
            if headers and hasattr(headers, "get"):
                request_id = headers.get("x-request-id") or headers.get("x-requestid")
        if request_id:
            details["request_id"] = _compact_text(request_id)

        code = getattr(exc, "code", None)
        if not code:
            code = _extract_error_code_from_text(str(exc))
        if code:
            details["code"] = _compact_text(code)
        error_type = getattr(exc, "type", None)
        if error_type:
            details["error_type"] = _compact_text(error_type)
        param = getattr(exc, "param", None)
        if param:
            details["param"] = _compact_text(param)

        retry_after_seconds = getattr(exc, "retry_after_seconds", None)
        if retry_after_seconds is None:
            retry_after_seconds = getattr(exc, "reset_seconds", None)
        if retry_after_seconds is None:
            response = getattr(exc, "response", None)
            headers = getattr(response, "headers", None)
            retry_after_header = None
            if headers and hasattr(headers, "get"):
                retry_after_header = headers.get("retry-after") or headers.get(
                    "Retry-After"
                )
            if retry_after_header not in (None, ""):
                try:
                    retry_after_seconds = int(float(str(retry_after_header).strip()))
                except Exception:
                    retry_after_seconds = None
        if retry_after_seconds is None:
            retry_after_seconds = _extract_retry_after_seconds_from_text(str(exc))
        if retry_after_seconds is not None:
            try:
                parsed_retry_after = int(float(str(retry_after_seconds).strip()))
            except Exception:
                parsed_retry_after = 0
            if parsed_retry_after > 0:
                details["retry_after_seconds"] = parsed_retry_after

        request = getattr(exc, "request", None)
        if request is not None:
            try:
                details["request_method"] = _compact_text(
                    getattr(request, "method", "")
                )
                details["request_url"] = _compact_text(str(getattr(request, "url", "")))
            except Exception:
                pass

        litellm_debug_info = getattr(exc, "litellm_debug_info", None)
        if litellm_debug_info:
            # This may contain prompt snippets depending on LiteLLM settings.
            # We only store a short, compact preview.
            details["litellm_debug_info"] = _truncate_text(
                _compact_text(litellm_debug_info), 400
            )

        cause_chain: List[Dict[str, str]] = []
        cur: Optional[BaseException] = exc
        seen: set[int] = set()
        for _ in range(4):
            if cur is None:
                break
            next_exc = getattr(cur, "__cause__", None) or getattr(
                cur, "__context__", None
            )
            if next_exc is None:
                break
            obj_id = id(next_exc)
            if obj_id in seen:
                break
            seen.add(obj_id)
            cause_chain.append(
                {
                    "type": type(next_exc).__name__,
                    "message": _truncate_text(_compact_text(str(next_exc)), 500),
                }
            )
            cur = next_exc
        if cause_chain:
            details["cause_chain"] = cause_chain
    except Exception:
        # Never block the caller due to debug extraction.
        return {"type": type(exc).__name__, "message": _compact_text(str(exc))}
    return details


def extract_retry_after_seconds(exc: BaseException) -> int | None:
    try:
        raw_value = extract_llm_error_details(exc).get("retry_after_seconds")
        if raw_value in (None, ""):
            return None
        parsed = int(float(str(raw_value).strip()))
    except Exception:
        return None
    if parsed <= 0:
        return None
    return parsed


def format_llm_error_one_line(exc: BaseException, *, limit: int = 900) -> str:
    """
    One-line error for log + DB. Example:
    APIError status_code=401 provider=openai model=gpt-5.2 message=...
    """
    try:
        details = extract_llm_error_details(exc)
        parts: List[str] = []
        parts.append(str(details.get("type") or type(exc).__name__))
        for key in (
            "status_code",
            "retry_after_seconds",
            "provider",
            "model",
            "request_id",
            "code",
            "error_type",
        ):
            value = details.get(key)
            if value is None or value == "":
                continue
            parts.append(f"{key}={value}")
        message = details.get("message") or _compact_text(str(exc))
        if message:
            parts.append(f"message={_truncate_text(_compact_text(message), 300)}")
        cause_chain = details.get("cause_chain") or []
        if isinstance(cause_chain, list) and cause_chain:
            cause_items = [item for item in cause_chain if isinstance(item, dict)]
            picked: List[Dict[str, str]] = []
            for item in cause_items:
                msg = _compact_text(item.get("message", ""))
                if msg:
                    picked.append(
                        {"type": _compact_text(item.get("type", "")), "message": msg}
                    )
            if not picked and cause_items:
                first = cause_items[0]
                picked.append(
                    {
                        "type": _compact_text(first.get("type", "")),
                        "message": _compact_text(first.get("message", "")),
                    }
                )
            if picked:
                parts.append(
                    f"cause={picked[0].get('type', '')}: {_truncate_text(_compact_text(picked[0].get('message', '')), 200)}"
                )
            if len(picked) >= 2:
                parts.append(
                    f"cause2={picked[1].get('type', '')}: {_truncate_text(_compact_text(picked[1].get('message', '')), 140)}"
                )
            for item in picked[:2]:
                msg = _compact_text(item.get("message", "")).lower()
                if "<!doctype html" in msg or "<html" in msg:
                    parts.append("hint=base_url_returns_html")
                    break
        return _truncate_text(" ".join(parts), limit)
    except Exception:
        return _truncate_text(
            f"{type(exc).__name__} message={_compact_text(str(exc))}", limit
        )


def _append_trace(trace_out: Optional[Path], payload: Dict[str, Any]) -> None:
    if trace_out is None:
        return
    trace_out.parent.mkdir(parents=True, exist_ok=True)
    with trace_out.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
