from __future__ import annotations

from typing import Any


def _import_openai():
    try:
        import openai  # type: ignore
    except Exception as exc:
        raise RuntimeError("openai_not_installed") from exc
    return openai


def _build_openai_client(
    *,
    api_key: str,
    base_url: str,
    timeout_seconds: float,
) -> Any:
    openai = _import_openai()
    client_kwargs: dict[str, Any] = {
        "api_key": str(api_key or "").strip(),
        "timeout": float(timeout_seconds),
        "max_retries": 0,
    }
    resolved_base_url = str(base_url or "").strip().rstrip("/")
    if resolved_base_url:
        client_kwargs["base_url"] = resolved_base_url
    return openai.OpenAI(**client_kwargs)


def _resolve_openai_model_name(model_name: str) -> str:
    resolved_model_name = str(model_name or "").strip()
    if resolved_model_name.startswith("openai/"):
        return resolved_model_name.removeprefix("openai/")
    return resolved_model_name
