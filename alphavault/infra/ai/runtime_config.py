from __future__ import annotations

import os
import re
from dataclasses import dataclass, replace

from alphavault.ai.analyze import (
    AI_MODE_COMPLETION,
    AI_MODE_RESPONSES,
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
)
from alphavault.constants import (
    DEFAULT_AI_LIMIT_GROUP_NAME,
    DEFAULT_AI_MAX_INFLIGHT,
    DEFAULT_AI_PROFILE_NAME,
    DEFAULT_AI_RPM,
    ENV_AI_API_KEY,
    ENV_AI_API_MODE,
    ENV_AI_BASE_URL,
    ENV_AI_LIMIT_GROUP_PREFIX,
    ENV_AI_MAX_INFLIGHT,
    ENV_AI_MODEL,
    ENV_AI_PROFILE_PREFIX,
    ENV_AI_REASONING_EFFORT,
    ENV_AI_RETRIES,
    ENV_AI_RPM,
    ENV_AI_TASK_PROFILE_PREFIX,
    ENV_AI_TEMPERATURE,
    ENV_AI_TIMEOUT_SEC,
)

AI_TASK_POST_ANALYSIS = "post_analysis"
AI_TASK_POST_CONTEXT = "post_context"
AI_TASK_ALIAS_RESOLVE = "alias_resolve"
AI_TASK_RELATION_CANDIDATE_RANK = "relation_candidate_rank"
AI_TASK_TOPIC_CLUSTER_SUGGEST = "topic_cluster_suggest"
AI_TASK_FOLLOW_KEYWORDS_SUGGEST = "follow_keywords_suggest"
AI_TASK_STOCK_SUMMARY = "stock_summary"
AI_TASK_TRADE_SIGNAL_REVIEW = "trade_signal_review"
AI_REASONING_EFFORT_CHOICES = [
    "none",
    "minimal",
    "low",
    "medium",
    "high",
    "xhigh",
]
_VALID_AI_MODES = {AI_MODE_COMPLETION, AI_MODE_RESPONSES}
_PROFILE_FIELD_ENV_BY_SUFFIX = {
    "API_KEY": ENV_AI_API_KEY,
    "MODEL": ENV_AI_MODEL,
    "BASE_URL": ENV_AI_BASE_URL,
    "API_MODE": ENV_AI_API_MODE,
    "TIMEOUT_SEC": ENV_AI_TIMEOUT_SEC,
    "TEMPERATURE": ENV_AI_TEMPERATURE,
    "REASONING_EFFORT": ENV_AI_REASONING_EFFORT,
    "RETRIES": ENV_AI_RETRIES,
}
_LIMIT_GROUP_FIELD_ENV_BY_SUFFIX = {
    "RPM": ENV_AI_RPM,
    "MAX_INFLIGHT": ENV_AI_MAX_INFLIGHT,
}


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
    ai_rpm: float
    ai_max_inflight: int
    task_key: str = ""
    profile_name: str = DEFAULT_AI_PROFILE_NAME
    limit_group_name: str = DEFAULT_AI_LIMIT_GROUP_NAME


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _env_optional_text(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    return str(raw).strip()


def _normalize_env_segment(value: object) -> str:
    text = re.sub(r"[^0-9A-Za-z]+", "_", str(value or "").strip())
    text = re.sub(r"_+", "_", text).strip("_")
    return text.upper()


def _normalize_profile_name(value: object) -> str:
    text = re.sub(r"[^0-9A-Za-z]+", "_", str(value or "").strip().lower())
    text = re.sub(r"_+", "_", text).strip("_")
    return text or DEFAULT_AI_PROFILE_NAME


def _normalize_limit_group_name(value: object) -> str:
    text = re.sub(r"[^0-9A-Za-z]+", "_", str(value or "").strip().lower())
    text = re.sub(r"_+", "_", text).strip("_")
    return text or DEFAULT_AI_LIMIT_GROUP_NAME


def ai_task_profile_env_name(task_key: str) -> str:
    segment = _normalize_env_segment(task_key)
    if not segment:
        raise RuntimeError("ai_task_key_missing")
    return f"{ENV_AI_TASK_PROFILE_PREFIX}{segment}_PROFILE"


def ai_task_limit_group_env_name(task_key: str) -> str:
    segment = _normalize_env_segment(task_key)
    if not segment:
        raise RuntimeError("ai_task_key_missing")
    return f"{ENV_AI_TASK_PROFILE_PREFIX}{segment}_LIMIT_GROUP"


def ai_profile_field_env_name(profile_name: str, field_suffix: str) -> str:
    normalized_profile_name = _normalize_profile_name(profile_name)
    normalized_suffix = _normalize_env_segment(field_suffix)
    if normalized_profile_name == DEFAULT_AI_PROFILE_NAME:
        try:
            return _PROFILE_FIELD_ENV_BY_SUFFIX[normalized_suffix]
        except KeyError as err:
            raise RuntimeError(f"ai_profile_field_unknown:{normalized_suffix}") from err
    profile_segment = _normalize_env_segment(normalized_profile_name)
    return f"{ENV_AI_PROFILE_PREFIX}{profile_segment}_{normalized_suffix}"


def ai_profile_limit_group_env_name(profile_name: str) -> str:
    profile_segment = _normalize_env_segment(_normalize_profile_name(profile_name))
    if not profile_segment or profile_segment == _normalize_env_segment(
        DEFAULT_AI_PROFILE_NAME
    ):
        raise RuntimeError("ai_profile_limit_group_missing_profile")
    return f"{ENV_AI_PROFILE_PREFIX}{profile_segment}_LIMIT_GROUP"


def ai_limit_group_field_env_name(limit_group_name: str, field_suffix: str) -> str:
    normalized_limit_group_name = _normalize_limit_group_name(limit_group_name)
    normalized_suffix = _normalize_env_segment(field_suffix)
    if normalized_limit_group_name == DEFAULT_AI_LIMIT_GROUP_NAME:
        try:
            return _LIMIT_GROUP_FIELD_ENV_BY_SUFFIX[normalized_suffix]
        except KeyError as err:
            raise RuntimeError(
                f"ai_limit_group_field_unknown:{normalized_suffix}"
            ) from err
    limit_group_segment = _normalize_env_segment(normalized_limit_group_name)
    return f"{ENV_AI_LIMIT_GROUP_PREFIX}{limit_group_segment}_{normalized_suffix}"


def _profile_text(profile_name: str, field_suffix: str) -> str | None:
    return _env_optional_text(ai_profile_field_env_name(profile_name, field_suffix))


def _task_profile_name(task_key: str) -> str:
    raw_profile_name = _env_optional_text(ai_task_profile_env_name(task_key))
    if raw_profile_name is None:
        return DEFAULT_AI_PROFILE_NAME
    return _normalize_profile_name(raw_profile_name)


def _profile_limit_group_name(profile_name: str) -> str:
    normalized_profile_name = _normalize_profile_name(profile_name)
    if normalized_profile_name == DEFAULT_AI_PROFILE_NAME:
        return DEFAULT_AI_LIMIT_GROUP_NAME
    raw_limit_group_name = _env_optional_text(
        ai_profile_limit_group_env_name(normalized_profile_name)
    )
    if raw_limit_group_name is None:
        return DEFAULT_AI_LIMIT_GROUP_NAME
    return _normalize_limit_group_name(raw_limit_group_name)


def _task_limit_group_name(task_key: str, *, profile_name: str) -> str:
    raw_limit_group_name = _env_optional_text(ai_task_limit_group_env_name(task_key))
    if raw_limit_group_name is not None:
        return _normalize_limit_group_name(raw_limit_group_name)
    return _profile_limit_group_name(profile_name)


def _resolve_api_mode(value: object, *, default: str) -> str:
    resolved = str(value or "").strip().lower() or str(default or "").strip().lower()
    if resolved in _VALID_AI_MODES:
        return resolved
    return DEFAULT_AI_MODE


def _coerce_float_override(value: object) -> float:
    return float(str(value).strip())


def _coerce_int_override(value: object) -> int:
    return int(str(value).strip())


def _build_default_runtime_config(*, timeout_seconds_default: float) -> AiRuntimeConfig:
    default_timeout = float(timeout_seconds_default)
    reasoning_effort = _env_optional_text(ENV_AI_REASONING_EFFORT)
    return AiRuntimeConfig(
        api_key=os.getenv(ENV_AI_API_KEY, "").strip(),
        model=os.getenv(ENV_AI_MODEL, DEFAULT_MODEL).strip() or DEFAULT_MODEL,
        base_url=os.getenv(ENV_AI_BASE_URL, "").strip(),
        api_mode=_resolve_api_mode(
            os.getenv(ENV_AI_API_MODE, DEFAULT_AI_MODE),
            default=DEFAULT_AI_MODE,
        ),
        temperature=_env_float(ENV_AI_TEMPERATURE, DEFAULT_AI_TEMPERATURE),
        reasoning_effort=(
            DEFAULT_AI_REASONING_EFFORT
            if reasoning_effort is None
            else reasoning_effort
        ),
        timeout_seconds=max(1.0, _env_float(ENV_AI_TIMEOUT_SEC, default_timeout)),
        retries=max(0, _env_int(ENV_AI_RETRIES, DEFAULT_AI_RETRY_COUNT)),
        ai_rpm=max(0.0, _env_float(ENV_AI_RPM, DEFAULT_AI_RPM)),
        ai_max_inflight=max(
            1,
            _env_int(ENV_AI_MAX_INFLIGHT, DEFAULT_AI_MAX_INFLIGHT),
        ),
        profile_name=DEFAULT_AI_PROFILE_NAME,
        limit_group_name=DEFAULT_AI_LIMIT_GROUP_NAME,
    )


def _apply_limit_group_runtime_config(
    config: AiRuntimeConfig,
    *,
    limit_group_name: str,
) -> AiRuntimeConfig:
    return replace(
        config,
        ai_rpm=max(
            0.0,
            _env_float(
                ai_limit_group_field_env_name(limit_group_name, "RPM"),
                config.ai_rpm,
            ),
        ),
        ai_max_inflight=max(
            1,
            _env_int(
                ai_limit_group_field_env_name(limit_group_name, "MAX_INFLIGHT"),
                config.ai_max_inflight,
            ),
        ),
        limit_group_name=_normalize_limit_group_name(limit_group_name),
    )


def ai_runtime_config_from_env(*, timeout_seconds_default: float) -> AiRuntimeConfig:
    return _build_default_runtime_config(
        timeout_seconds_default=timeout_seconds_default
    )


def ai_task_runtime_config_from_env(
    *,
    task_key: str,
    timeout_seconds_default: float,
) -> AiRuntimeConfig:
    default_config = _build_default_runtime_config(
        timeout_seconds_default=timeout_seconds_default
    )
    profile_name = _task_profile_name(task_key)
    limit_group_name = _task_limit_group_name(task_key, profile_name=profile_name)
    base_config = _apply_limit_group_runtime_config(
        default_config,
        limit_group_name=limit_group_name,
    )
    normalized_task_key = str(task_key or "").strip()
    if profile_name == DEFAULT_AI_PROFILE_NAME:
        return replace(
            base_config,
            task_key=normalized_task_key,
            profile_name=profile_name,
        )

    reasoning_effort = _profile_text(profile_name, "REASONING_EFFORT")
    return AiRuntimeConfig(
        api_key=_profile_text(profile_name, "API_KEY") or default_config.api_key,
        model=_profile_text(profile_name, "MODEL") or default_config.model,
        base_url=_profile_text(profile_name, "BASE_URL") or default_config.base_url,
        api_mode=_resolve_api_mode(
            _profile_text(profile_name, "API_MODE") or default_config.api_mode,
            default=default_config.api_mode,
        ),
        temperature=_env_float(
            ai_profile_field_env_name(profile_name, "TEMPERATURE"),
            default_config.temperature,
        ),
        reasoning_effort=(
            default_config.reasoning_effort
            if reasoning_effort is None
            else reasoning_effort
        ),
        timeout_seconds=max(
            1.0,
            _env_float(
                ai_profile_field_env_name(profile_name, "TIMEOUT_SEC"),
                default_config.timeout_seconds,
            ),
        ),
        retries=max(
            0,
            _env_int(
                ai_profile_field_env_name(profile_name, "RETRIES"),
                default_config.retries,
            ),
        ),
        ai_rpm=base_config.ai_rpm,
        ai_max_inflight=base_config.ai_max_inflight,
        task_key=normalized_task_key,
        profile_name=profile_name,
        limit_group_name=base_config.limit_group_name,
    )


def apply_ai_runtime_config_overrides(
    config: AiRuntimeConfig,
    *,
    api_key: object = None,
    model: object = None,
    base_url: object = None,
    api_mode: object = None,
    temperature: object = None,
    reasoning_effort: object = None,
    timeout_seconds: object = None,
    retries: object = None,
    ai_rpm: object = None,
    ai_max_inflight: object = None,
) -> AiRuntimeConfig:
    resolved_api_key = config.api_key if api_key is None else str(api_key).strip()
    resolved_model = (
        config.model if model is None else str(model).strip() or config.model
    )
    resolved_base_url = config.base_url if base_url is None else str(base_url).strip()
    resolved_api_mode = (
        config.api_mode
        if api_mode is None
        else _resolve_api_mode(api_mode, default=config.api_mode)
    )
    resolved_reasoning_effort = (
        config.reasoning_effort
        if reasoning_effort is None
        else str(reasoning_effort).strip() or config.reasoning_effort
    )
    resolved_temperature = (
        config.temperature
        if temperature is None
        else _coerce_float_override(temperature)
    )
    resolved_timeout_seconds = (
        max(1.0, config.timeout_seconds)
        if timeout_seconds is None
        else max(1.0, _coerce_float_override(timeout_seconds))
    )
    resolved_retries = (
        max(0, config.retries)
        if retries is None
        else max(0, _coerce_int_override(retries))
    )
    resolved_ai_rpm = (
        max(0.0, config.ai_rpm)
        if ai_rpm is None
        else max(0.0, _coerce_float_override(ai_rpm))
    )
    resolved_ai_max_inflight = (
        max(1, config.ai_max_inflight)
        if ai_max_inflight is None
        else max(1, _coerce_int_override(ai_max_inflight))
    )
    return replace(
        config,
        api_key=resolved_api_key,
        model=resolved_model,
        base_url=resolved_base_url,
        api_mode=resolved_api_mode,
        temperature=resolved_temperature,
        reasoning_effort=resolved_reasoning_effort,
        timeout_seconds=resolved_timeout_seconds,
        retries=resolved_retries,
        ai_rpm=resolved_ai_rpm,
        ai_max_inflight=resolved_ai_max_inflight,
    )


def ai_task_runtime_config_is_configured(
    *,
    task_key: str,
    timeout_seconds_default: float,
) -> tuple[bool, str]:
    config = ai_task_runtime_config_from_env(
        task_key=task_key,
        timeout_seconds_default=timeout_seconds_default,
    )
    if str(config.api_key or "").strip():
        return True, ""
    return False, f"Missing {ENV_AI_API_KEY}"


def ai_task_runtime_config_summary(
    *,
    task_key: str,
    timeout_seconds_default: float,
) -> tuple[dict[str, object], str]:
    ok, err = ai_task_runtime_config_is_configured(
        task_key=task_key,
        timeout_seconds_default=timeout_seconds_default,
    )
    if not ok:
        return {}, err
    config = ai_task_runtime_config_from_env(
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
            "api_mode": config.api_mode,
            "timeout_seconds": float(config.timeout_seconds),
            "retries": int(config.retries),
            "temperature": float(config.temperature),
            "reasoning_effort": str(config.reasoning_effort),
            "ai_rpm": float(config.ai_rpm),
            "ai_max_inflight": int(config.ai_max_inflight),
        },
        "",
    )


__all__ = [
    "AI_REASONING_EFFORT_CHOICES",
    "AI_TASK_ALIAS_RESOLVE",
    "AI_TASK_FOLLOW_KEYWORDS_SUGGEST",
    "AI_TASK_POST_ANALYSIS",
    "AI_TASK_POST_CONTEXT",
    "AI_TASK_RELATION_CANDIDATE_RANK",
    "AI_TASK_STOCK_SUMMARY",
    "AI_TASK_TRADE_SIGNAL_REVIEW",
    "AI_TASK_TOPIC_CLUSTER_SUGGEST",
    "AiRuntimeConfig",
    "ai_limit_group_field_env_name",
    "ai_profile_field_env_name",
    "ai_profile_limit_group_env_name",
    "ai_task_limit_group_env_name",
    "ai_task_profile_env_name",
    "ai_task_runtime_config_from_env",
    "ai_task_runtime_config_is_configured",
    "ai_task_runtime_config_summary",
    "ai_runtime_config_from_env",
    "apply_ai_runtime_config_overrides",
]
