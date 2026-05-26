from __future__ import annotations

import os
from dataclasses import dataclass, replace

from alphavault.constants import (
    DEFAULT_AI_PROFILE_NAME,
    DEFAULT_RERANKER_LIMIT_GROUP_NAME,
    DEFAULT_RERANKER_MAX_INFLIGHT,
    DEFAULT_RERANKER_MODEL,
    DEFAULT_RERANKER_PROFILE_NAME,
    DEFAULT_RERANKER_RETRIES,
    DEFAULT_RERANKER_RPM,
    DEFAULT_RERANKER_TIMEOUT_SECONDS,
    DEFAULT_RERANKER_TOP_N,
    ENV_RERANKER_API_KEY,
    ENV_RERANKER_BASE_URL,
    ENV_RERANKER_LIMIT_GROUP_PREFIX,
    ENV_RERANKER_MAX_INFLIGHT,
    ENV_RERANKER_MODEL,
    ENV_RERANKER_PROFILE_PREFIX,
    ENV_RERANKER_RETRIES,
    ENV_RERANKER_RPM,
    ENV_RERANKER_TASK_PROFILE_PREFIX,
    ENV_RERANKER_TIMEOUT_SEC,
    ENV_RERANKER_TOP_N,
)
from alphavault.infra.ai.runtime_config import (
    ai_limit_group_field_env_name,
    ai_profile_field_env_name,
    ai_profile_limit_group_env_name,
)

from ._runtime_config_utils import (
    env_float_from_names,
    env_int,
    env_int_from_names,
    env_optional_text,
    env_optional_text_from_names,
    limit_group_field_env_name as build_limit_group_field_env_name,
    normalize_name,
    profile_field_env_name as build_profile_field_env_name,
    profile_limit_group_env_name as build_profile_limit_group_env_name,
    task_limit_group_env_name as build_task_limit_group_env_name,
    task_profile_env_name as build_task_profile_env_name,
)

RERANKER_TASK_SEMANTIC_QUERY = "semantic_query"

_PROFILE_FIELD_ENV_BY_SUFFIX = {
    "API_KEY": ENV_RERANKER_API_KEY,
    "MODEL": ENV_RERANKER_MODEL,
    "BASE_URL": ENV_RERANKER_BASE_URL,
    "TIMEOUT_SEC": ENV_RERANKER_TIMEOUT_SEC,
    "RETRIES": ENV_RERANKER_RETRIES,
    "TOP_N": ENV_RERANKER_TOP_N,
}
_LIMIT_GROUP_FIELD_ENV_BY_SUFFIX = {
    "RPM": ENV_RERANKER_RPM,
    "MAX_INFLIGHT": ENV_RERANKER_MAX_INFLIGHT,
}
_AI_PROFILE_FALLBACK_FIELD_SUFFIXES = frozenset(
    ("API_KEY", "BASE_URL", "TIMEOUT_SEC", "RETRIES")
)
_AI_LIMIT_GROUP_FALLBACK_FIELD_SUFFIXES = frozenset(("RPM", "MAX_INFLIGHT"))


@dataclass(frozen=True)
class RerankerRuntimeConfig:
    api_key: str
    model: str
    base_url: str
    timeout_seconds: float
    retries: int
    rpm: float
    max_inflight: int
    top_n: int
    task_key: str = ""
    profile_name: str = DEFAULT_RERANKER_PROFILE_NAME
    limit_group_name: str = DEFAULT_RERANKER_LIMIT_GROUP_NAME


def reranker_task_profile_env_name(task_key: str) -> str:
    return build_task_profile_env_name(
        task_key=task_key,
        task_prefix=ENV_RERANKER_TASK_PROFILE_PREFIX,
    )


def reranker_task_limit_group_env_name(task_key: str) -> str:
    return build_task_limit_group_env_name(
        task_key=task_key,
        task_prefix=ENV_RERANKER_TASK_PROFILE_PREFIX,
    )


def reranker_profile_field_env_name(profile_name: str, field_suffix: str) -> str:
    return build_profile_field_env_name(
        profile_name=profile_name,
        field_suffix=field_suffix,
        default_profile_name=DEFAULT_RERANKER_PROFILE_NAME,
        profile_prefix=ENV_RERANKER_PROFILE_PREFIX,
        root_field_env_by_suffix=_PROFILE_FIELD_ENV_BY_SUFFIX,
    )


def reranker_profile_limit_group_env_name(profile_name: str) -> str:
    return build_profile_limit_group_env_name(
        profile_name=profile_name,
        default_profile_name=DEFAULT_RERANKER_PROFILE_NAME,
        profile_prefix=ENV_RERANKER_PROFILE_PREFIX,
    )


def reranker_limit_group_field_env_name(
    limit_group_name: str,
    field_suffix: str,
) -> str:
    return build_limit_group_field_env_name(
        limit_group_name=limit_group_name,
        field_suffix=field_suffix,
        default_limit_group_name=DEFAULT_RERANKER_LIMIT_GROUP_NAME,
        limit_group_prefix=ENV_RERANKER_LIMIT_GROUP_PREFIX,
        root_field_env_by_suffix=_LIMIT_GROUP_FIELD_ENV_BY_SUFFIX,
    )


def _ai_profile_name_for_fallback(profile_name: str) -> str:
    normalized_profile_name = normalize_name(
        profile_name,
        default_name=DEFAULT_RERANKER_PROFILE_NAME,
    )
    if normalized_profile_name == DEFAULT_RERANKER_PROFILE_NAME:
        return DEFAULT_AI_PROFILE_NAME
    return normalized_profile_name


def _profile_field_env_names(profile_name: str, field_suffix: str) -> tuple[str, ...]:
    normalized_profile_name = normalize_name(
        profile_name,
        default_name=DEFAULT_RERANKER_PROFILE_NAME,
    )
    names = [reranker_profile_field_env_name(normalized_profile_name, field_suffix)]
    if field_suffix in _AI_PROFILE_FALLBACK_FIELD_SUFFIXES:
        names.append(
            ai_profile_field_env_name(
                _ai_profile_name_for_fallback(normalized_profile_name),
                field_suffix,
            )
        )
    return tuple(names)


def _limit_group_field_env_names(
    limit_group_name: str,
    field_suffix: str,
) -> tuple[str, ...]:
    normalized_limit_group_name = normalize_name(
        limit_group_name,
        default_name=DEFAULT_RERANKER_LIMIT_GROUP_NAME,
    )
    names = [
        reranker_limit_group_field_env_name(normalized_limit_group_name, field_suffix)
    ]
    if field_suffix in _AI_LIMIT_GROUP_FALLBACK_FIELD_SUFFIXES:
        names.append(
            ai_limit_group_field_env_name(
                normalized_limit_group_name,
                field_suffix,
            )
        )
    return tuple(names)


def _profile_text(profile_name: str, field_suffix: str) -> str | None:
    return env_optional_text_from_names(
        *_profile_field_env_names(profile_name, field_suffix)
    )


def _task_profile_name(task_key: str) -> str:
    raw_profile_name = env_optional_text(reranker_task_profile_env_name(task_key))
    if raw_profile_name is None:
        return DEFAULT_RERANKER_PROFILE_NAME
    return normalize_name(
        raw_profile_name,
        default_name=DEFAULT_RERANKER_PROFILE_NAME,
    )


def _profile_limit_group_name(profile_name: str) -> str:
    normalized_profile_name = normalize_name(
        profile_name,
        default_name=DEFAULT_RERANKER_PROFILE_NAME,
    )
    if normalized_profile_name == DEFAULT_RERANKER_PROFILE_NAME:
        return DEFAULT_RERANKER_LIMIT_GROUP_NAME
    raw_limit_group_name = env_optional_text_from_names(
        reranker_profile_limit_group_env_name(normalized_profile_name),
        ai_profile_limit_group_env_name(
            _ai_profile_name_for_fallback(normalized_profile_name)
        ),
    )
    if raw_limit_group_name is None:
        return DEFAULT_RERANKER_LIMIT_GROUP_NAME
    return normalize_name(
        raw_limit_group_name,
        default_name=DEFAULT_RERANKER_LIMIT_GROUP_NAME,
    )


def _task_limit_group_name(task_key: str, *, profile_name: str) -> str:
    raw_limit_group_name = env_optional_text(
        reranker_task_limit_group_env_name(task_key)
    )
    if raw_limit_group_name is not None:
        return normalize_name(
            raw_limit_group_name,
            default_name=DEFAULT_RERANKER_LIMIT_GROUP_NAME,
        )
    return _profile_limit_group_name(profile_name)


def _build_default_runtime_config(
    *,
    timeout_seconds_default: float,
) -> RerankerRuntimeConfig:
    return RerankerRuntimeConfig(
        api_key=_profile_text(DEFAULT_RERANKER_PROFILE_NAME, "API_KEY") or "",
        model=os.getenv(ENV_RERANKER_MODEL, DEFAULT_RERANKER_MODEL).strip()
        or DEFAULT_RERANKER_MODEL,
        base_url=_profile_text(DEFAULT_RERANKER_PROFILE_NAME, "BASE_URL") or "",
        timeout_seconds=max(
            1.0,
            env_float_from_names(
                _profile_field_env_names(
                    DEFAULT_RERANKER_PROFILE_NAME,
                    "TIMEOUT_SEC",
                ),
                timeout_seconds_default,
            ),
        ),
        retries=max(
            0,
            env_int_from_names(
                _profile_field_env_names(
                    DEFAULT_RERANKER_PROFILE_NAME,
                    "RETRIES",
                ),
                DEFAULT_RERANKER_RETRIES,
            ),
        ),
        rpm=max(
            0.0,
            env_float_from_names(
                _limit_group_field_env_names(
                    DEFAULT_RERANKER_LIMIT_GROUP_NAME,
                    "RPM",
                ),
                DEFAULT_RERANKER_RPM,
            ),
        ),
        max_inflight=max(
            1,
            env_int_from_names(
                _limit_group_field_env_names(
                    DEFAULT_RERANKER_LIMIT_GROUP_NAME,
                    "MAX_INFLIGHT",
                ),
                DEFAULT_RERANKER_MAX_INFLIGHT,
            ),
        ),
        top_n=max(1, env_int(ENV_RERANKER_TOP_N, DEFAULT_RERANKER_TOP_N)),
        profile_name=DEFAULT_RERANKER_PROFILE_NAME,
        limit_group_name=DEFAULT_RERANKER_LIMIT_GROUP_NAME,
    )


def _apply_limit_group_runtime_config(
    config: RerankerRuntimeConfig,
    *,
    limit_group_name: str,
) -> RerankerRuntimeConfig:
    normalized_limit_group_name = normalize_name(
        limit_group_name,
        default_name=DEFAULT_RERANKER_LIMIT_GROUP_NAME,
    )
    return replace(
        config,
        rpm=max(
            0.0,
            env_float_from_names(
                _limit_group_field_env_names(
                    normalized_limit_group_name,
                    "RPM",
                ),
                config.rpm,
            ),
        ),
        max_inflight=max(
            1,
            env_int_from_names(
                _limit_group_field_env_names(
                    normalized_limit_group_name,
                    "MAX_INFLIGHT",
                ),
                config.max_inflight,
            ),
        ),
        limit_group_name=normalized_limit_group_name,
    )


def _apply_profile_runtime_config(
    config: RerankerRuntimeConfig,
    *,
    profile_name: str,
) -> RerankerRuntimeConfig:
    normalized_profile_name = normalize_name(
        profile_name,
        default_name=DEFAULT_RERANKER_PROFILE_NAME,
    )
    resolved_api_key = _profile_text(normalized_profile_name, "API_KEY")
    resolved_model = _profile_text(normalized_profile_name, "MODEL")
    resolved_base_url = _profile_text(normalized_profile_name, "BASE_URL")
    resolved_timeout_seconds = _profile_text(normalized_profile_name, "TIMEOUT_SEC")
    resolved_retries = _profile_text(normalized_profile_name, "RETRIES")
    resolved_top_n = _profile_text(normalized_profile_name, "TOP_N")
    return replace(
        config,
        api_key=config.api_key if resolved_api_key is None else resolved_api_key,
        model=config.model if resolved_model is None else resolved_model,
        base_url=config.base_url if resolved_base_url is None else resolved_base_url,
        timeout_seconds=(
            config.timeout_seconds
            if resolved_timeout_seconds is None
            else max(1.0, float(str(resolved_timeout_seconds).strip()))
        ),
        retries=(
            config.retries
            if resolved_retries is None
            else max(0, int(str(resolved_retries).strip()))
        ),
        top_n=(
            config.top_n
            if resolved_top_n is None
            else max(1, int(str(resolved_top_n).strip()))
        ),
        profile_name=normalized_profile_name,
    )


def reranker_runtime_config_from_env(
    *,
    timeout_seconds_default: float = DEFAULT_RERANKER_TIMEOUT_SECONDS,
) -> RerankerRuntimeConfig:
    return reranker_task_runtime_config_from_env(
        task_key="",
        timeout_seconds_default=timeout_seconds_default,
    )


def reranker_task_runtime_config_from_env(
    *,
    task_key: str,
    timeout_seconds_default: float = DEFAULT_RERANKER_TIMEOUT_SECONDS,
) -> RerankerRuntimeConfig:
    default_config = _build_default_runtime_config(
        timeout_seconds_default=timeout_seconds_default
    )
    normalized_task_key = str(task_key or "").strip()
    if not normalized_task_key:
        return default_config
    profile_name = _task_profile_name(normalized_task_key)
    limit_group_name = _task_limit_group_name(
        normalized_task_key,
        profile_name=profile_name,
    )
    base_config = _apply_limit_group_runtime_config(
        default_config,
        limit_group_name=limit_group_name,
    )
    if profile_name == DEFAULT_RERANKER_PROFILE_NAME:
        return replace(
            base_config,
            task_key=normalized_task_key,
            profile_name=profile_name,
        )
    return replace(
        _apply_profile_runtime_config(base_config, profile_name=profile_name),
        task_key=normalized_task_key,
        limit_group_name=limit_group_name,
    )


def reranker_task_runtime_config_is_configured(
    *,
    task_key: str,
    timeout_seconds_default: float = DEFAULT_RERANKER_TIMEOUT_SECONDS,
) -> tuple[bool, str]:
    config = reranker_task_runtime_config_from_env(
        task_key=task_key,
        timeout_seconds_default=timeout_seconds_default,
    )
    if not str(config.api_key or "").strip():
        return False, f"Missing {ENV_RERANKER_API_KEY}"
    if not str(config.model or "").strip():
        return False, f"Missing {ENV_RERANKER_MODEL}"
    if not str(config.base_url or "").strip():
        return False, f"Missing {ENV_RERANKER_BASE_URL}"
    return True, ""


def reranker_task_runtime_config_summary(
    *,
    task_key: str,
    timeout_seconds_default: float = DEFAULT_RERANKER_TIMEOUT_SECONDS,
) -> tuple[dict[str, object], str]:
    ok, err = reranker_task_runtime_config_is_configured(
        task_key=task_key,
        timeout_seconds_default=timeout_seconds_default,
    )
    if not ok:
        return {}, err
    config = reranker_task_runtime_config_from_env(
        task_key=task_key,
        timeout_seconds_default=timeout_seconds_default,
    )
    return (
        {
            "task_key": config.task_key,
            "profile_name": config.profile_name,
            "limit_group_name": config.limit_group_name,
            "model": config.model,
            "base_url": config.base_url,
            "timeout_seconds": float(config.timeout_seconds),
            "retries": int(config.retries),
            "rpm": float(config.rpm),
            "max_inflight": int(config.max_inflight),
            "top_n": int(config.top_n),
        },
        "",
    )


__all__ = [
    "RERANKER_TASK_SEMANTIC_QUERY",
    "RerankerRuntimeConfig",
    "reranker_limit_group_field_env_name",
    "reranker_profile_field_env_name",
    "reranker_profile_limit_group_env_name",
    "reranker_runtime_config_from_env",
    "reranker_task_limit_group_env_name",
    "reranker_task_profile_env_name",
    "reranker_task_runtime_config_from_env",
    "reranker_task_runtime_config_is_configured",
    "reranker_task_runtime_config_summary",
]
