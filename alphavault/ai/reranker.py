from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable

import requests

from alphavault.logging_config import get_logger

DEFAULT_RERANKER_RETRY_BACKOFF_SEC = 2.0
DEFAULT_RERANKER_RETRY_MAX_BACKOFF_SEC = 32.0
logger = get_logger(__name__)


@dataclass(frozen=True)
class RerankResult:
    index: int
    score: float


def _coerce_result_index(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return int(str(value or "").strip())


def _coerce_result_score(item: dict[str, object]) -> float:
    raw_score = (
        item.get("relevance_score")
        if item.get("relevance_score") is not None
        else item.get("score")
    )
    if isinstance(raw_score, bool):
        return float(int(raw_score))
    if isinstance(raw_score, (int, float)):
        return float(raw_score)
    return float(str(raw_score or "").strip())


def _rerank_endpoint(base_url: str) -> str:
    resolved_base_url = str(base_url or "").strip().rstrip("/")
    if not resolved_base_url:
        raise RuntimeError("reranker_base_url_missing")
    if resolved_base_url.endswith("/rerank"):
        return resolved_base_url
    return f"{resolved_base_url}/rerank"


def _parse_rerank_results(payload: object) -> list[RerankResult]:
    if not isinstance(payload, dict):
        raise RuntimeError("reranker_invalid_response")
    raw_results = payload.get("results")
    if not isinstance(raw_results, list):
        raw_results = payload.get("data")
    if not isinstance(raw_results, list):
        raise RuntimeError("reranker_results_missing")
    out: list[RerankResult] = []
    for item in raw_results:
        if not isinstance(item, dict):
            continue
        try:
            out.append(
                RerankResult(
                    index=_coerce_result_index(item.get("index")),
                    score=_coerce_result_score(item),
                )
            )
        except Exception:
            continue
    return out


def rerank_texts(
    *,
    query: str,
    documents: list[str],
    model_name: str,
    base_url: str,
    api_key: str,
    timeout_seconds: float,
    retry_count: int,
    top_n: int,
    request_gate: Callable[[], None] | None = None,
) -> list[RerankResult]:
    clean_query = str(query or "").strip()
    clean_documents = [
        str(item or "").strip() for item in documents if str(item or "").strip()
    ]
    if not clean_query or not clean_documents:
        return []
    url = _rerank_endpoint(base_url)
    headers = {
        "Authorization": f"Bearer {str(api_key or '').strip()}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": str(model_name or "").strip(),
        "query": clean_query,
        "documents": clean_documents,
        "top_n": max(1, min(int(top_n), len(clean_documents))),
    }
    retries = max(0, int(retry_count))
    backoff_seconds = DEFAULT_RERANKER_RETRY_BACKOFF_SEC
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            if request_gate is not None:
                request_gate()
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=float(timeout_seconds),
            )
            response.raise_for_status()
            return _parse_rerank_results(response.json())
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                break
            wait_seconds = min(
                backoff_seconds,
                DEFAULT_RERANKER_RETRY_MAX_BACKOFF_SEC,
            )
            logger.warning(
                "[reranker] request_retry attempt=%s next_attempt=%s wait_seconds=%s error=%s",
                attempt + 1,
                attempt + 2,
                wait_seconds,
                type(exc).__name__,
            )
            time.sleep(wait_seconds)
            backoff_seconds = min(
                backoff_seconds * 2,
                DEFAULT_RERANKER_RETRY_MAX_BACKOFF_SEC,
            )
    assert last_error is not None
    raise last_error


__all__ = ["RerankResult", "rerank_texts"]
