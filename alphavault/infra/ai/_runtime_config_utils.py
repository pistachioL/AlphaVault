from __future__ import annotations

import os
import re


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def env_optional_text(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    return str(raw).strip()


def env_optional_text_from_names(*names: str) -> str | None:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        return str(raw).strip()
    return None


def env_float_from_names(names: tuple[str, ...], default: float) -> float:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        try:
            return float(str(raw).strip())
        except Exception:
            return float(default)
    return float(default)


def env_int_from_names(names: tuple[str, ...], default: int) -> int:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        try:
            return int(str(raw).strip())
        except Exception:
            return int(default)
    return int(default)


def normalize_env_segment(value: object) -> str:
    text = re.sub(r"[^0-9A-Za-z]+", "_", str(value or "").strip())
    text = re.sub(r"_+", "_", text).strip("_")
    return text.upper()


def normalize_name(value: object, *, default_name: str) -> str:
    text = re.sub(r"[^0-9A-Za-z]+", "_", str(value or "").strip().lower())
    text = re.sub(r"_+", "_", text).strip("_")
    return text or default_name


def task_profile_env_name(*, task_key: str, task_prefix: str) -> str:
    segment = normalize_env_segment(task_key)
    if not segment:
        raise RuntimeError("runtime_task_key_missing")
    return f"{task_prefix}{segment}_PROFILE"


def task_limit_group_env_name(*, task_key: str, task_prefix: str) -> str:
    segment = normalize_env_segment(task_key)
    if not segment:
        raise RuntimeError("runtime_task_key_missing")
    return f"{task_prefix}{segment}_LIMIT_GROUP"


def profile_field_env_name(
    *,
    profile_name: str,
    field_suffix: str,
    default_profile_name: str,
    profile_prefix: str,
    root_field_env_by_suffix: dict[str, str],
) -> str:
    normalized_profile_name = normalize_name(
        profile_name,
        default_name=default_profile_name,
    )
    normalized_suffix = normalize_env_segment(field_suffix)
    if normalized_profile_name == default_profile_name:
        try:
            return root_field_env_by_suffix[normalized_suffix]
        except KeyError as err:
            raise RuntimeError(
                f"runtime_profile_field_unknown:{normalized_suffix}"
            ) from err
    profile_segment = normalize_env_segment(normalized_profile_name)
    return f"{profile_prefix}{profile_segment}_{normalized_suffix}"


def profile_limit_group_env_name(
    *,
    profile_name: str,
    default_profile_name: str,
    profile_prefix: str,
) -> str:
    normalized_profile_name = normalize_name(
        profile_name,
        default_name=default_profile_name,
    )
    profile_segment = normalize_env_segment(normalized_profile_name)
    if not profile_segment or profile_segment == normalize_env_segment(
        default_profile_name
    ):
        raise RuntimeError("runtime_profile_limit_group_missing_profile")
    return f"{profile_prefix}{profile_segment}_LIMIT_GROUP"


def limit_group_field_env_name(
    *,
    limit_group_name: str,
    field_suffix: str,
    default_limit_group_name: str,
    limit_group_prefix: str,
    root_field_env_by_suffix: dict[str, str],
) -> str:
    normalized_limit_group_name = normalize_name(
        limit_group_name,
        default_name=default_limit_group_name,
    )
    normalized_suffix = normalize_env_segment(field_suffix)
    if normalized_limit_group_name == default_limit_group_name:
        try:
            return root_field_env_by_suffix[normalized_suffix]
        except KeyError as err:
            raise RuntimeError(
                f"runtime_limit_group_field_unknown:{normalized_suffix}"
            ) from err
    limit_group_segment = normalize_env_segment(normalized_limit_group_name)
    return f"{limit_group_prefix}{limit_group_segment}_{normalized_suffix}"
