from __future__ import annotations

import time
from typing import Any, Callable

from alphavault.ai._errors import extract_retry_after_seconds
from alphavault.ai._openai import _build_openai_client, _resolve_openai_model_name
from alphavault.logging_config import get_logger

DEFAULT_EMBEDDING_RETRY_BACKOFF_SEC = 2.0
DEFAULT_EMBEDDING_RETRY_MAX_BACKOFF_SEC = 32.0
logger = get_logger(__name__)


def _close_if_possible(resource: Any) -> None:
    close_fn = getattr(resource, "close", None)
    if callable(close_fn):
        close_fn()


def _coerce_embedding_vector(value: object) -> list[float]:
    if isinstance(value, list):
        return [float(item) for item in value]
    raise RuntimeError("embedding_vector_missing")


def embed_texts_with_openai(
    *,
    texts: list[str],
    model_name: str,
    dimensions: int,
    base_url: str,
    api_key: str,
    timeout_seconds: float,
    retry_count: int,
    request_gate: Callable[[], None] | None = None,
) -> list[list[float]]:
    resolved_texts = [
        str(text or "").strip() for text in texts if str(text or "").strip()
    ]
    if not resolved_texts:
        return []
    request_model_name = _resolve_openai_model_name(model_name)
    retries = max(0, int(retry_count))
    backoff_seconds = DEFAULT_EMBEDDING_RETRY_BACKOFF_SEC
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            if request_gate is not None:
                request_gate()
            client = _build_openai_client(
                api_key=api_key,
                base_url=base_url,
                timeout_seconds=float(timeout_seconds),
            )
            try:
                response = client.embeddings.create(
                    model=request_model_name,
                    input=resolved_texts,
                    dimensions=max(1, int(dimensions)),
                )
            finally:
                _close_if_possible(client)
            response_data = list(getattr(response, "data", []) or [])
            embeddings = [
                _coerce_embedding_vector(getattr(item, "embedding", None))
                for item in response_data
            ]
            if len(embeddings) != len(resolved_texts):
                raise RuntimeError("embedding_count_mismatch")
            return embeddings
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                break
            retry_after_seconds = extract_retry_after_seconds(exc)
            wait_seconds = (
                float(retry_after_seconds)
                if retry_after_seconds is not None
                else min(
                    backoff_seconds,
                    DEFAULT_EMBEDDING_RETRY_MAX_BACKOFF_SEC,
                )
            )
            logger.warning(
                "[embedding] request_retry attempt=%s next_attempt=%s wait_seconds=%s error=%s",
                attempt + 1,
                attempt + 2,
                wait_seconds,
                type(exc).__name__,
            )
            time.sleep(wait_seconds)
            backoff_seconds = min(
                backoff_seconds * 2,
                DEFAULT_EMBEDDING_RETRY_MAX_BACKOFF_SEC,
            )
    assert last_error is not None
    raise last_error


__all__ = ["embed_texts_with_openai"]
