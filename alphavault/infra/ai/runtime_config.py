from __future__ import annotations

import os
from dataclasses import dataclass

from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
)
from alphavault.constants import (
    ENV_AI_API_KEY,
    ENV_AI_API_MODE,
    ENV_AI_BASE_URL,
    ENV_AI_MODEL,
    ENV_AI_REASONING_EFFORT,
    ENV_AI_RETRIES,
    ENV_AI_TEMPERATURE,
    ENV_AI_TIMEOUT_SEC,
)


@dataclass(frozen=True)
class AiRuntimeConfig:
    api_key: str
    model: str
    base_url: str
    api_mode: str
    temperature: float
    reasoning_effort: str
    timeout_seconds: float
    retries: int


def ai_runtime_config_from_env(*, timeout_seconds_default: float) -> AiRuntimeConfig:
    default_timeout = float(timeout_seconds_default)
    return AiRuntimeConfig(
        api_key=os.getenv(ENV_AI_API_KEY, "").strip(),
        model=os.getenv(ENV_AI_MODEL, DEFAULT_MODEL).strip() or DEFAULT_MODEL,
        base_url=os.getenv(ENV_AI_BASE_URL, "").strip(),
        api_mode=os.getenv(ENV_AI_API_MODE, DEFAULT_AI_MODE).strip() or DEFAULT_AI_MODE,
        temperature=float(
            os.getenv(ENV_AI_TEMPERATURE, str(DEFAULT_AI_TEMPERATURE)).strip()
            or DEFAULT_AI_TEMPERATURE
        ),
        reasoning_effort=os.getenv(
            ENV_AI_REASONING_EFFORT,
            DEFAULT_AI_REASONING_EFFORT,
        ).strip()
        or DEFAULT_AI_REASONING_EFFORT,
        timeout_seconds=float(
            os.getenv(ENV_AI_TIMEOUT_SEC, str(default_timeout)).strip()
            or default_timeout
        ),
        retries=int(
            os.getenv(ENV_AI_RETRIES, str(DEFAULT_AI_RETRY_COUNT)).strip()
            or DEFAULT_AI_RETRY_COUNT
        ),
    )


__all__ = [
    "AiRuntimeConfig",
    "ai_runtime_config_from_env",
]
