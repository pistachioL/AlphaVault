from __future__ import annotations

import os
from dataclasses import dataclass

from alphavault.constants import (
    ENV_WEIBO_TURSO_AUTH_TOKEN,
    ENV_WEIBO_TURSO_DATABASE_URL,
    ENV_XUEQIU_TURSO_AUTH_TOKEN,
    ENV_XUEQIU_TURSO_DATABASE_URL,
    PLATFORM_WEIBO,
    PLATFORM_XUEQIU,
)


@dataclass(frozen=True)
class LibsqlSource:
    name: str
    url: str
    token: str


def _env_text(name: str) -> str:
    return os.getenv(name, "").strip()


def infer_platform_from_post_uid(post_uid: object) -> str:
    value = str(post_uid or "").strip().lower()
    if value.startswith(f"{PLATFORM_WEIBO}:"):
        return PLATFORM_WEIBO
    if value.startswith(f"{PLATFORM_XUEQIU}:"):
        return PLATFORM_XUEQIU
    return ""


def load_configured_libsql_sources_from_env() -> list[LibsqlSource]:
    sources: list[LibsqlSource] = []

    for name, url_env, token_env in (
        (PLATFORM_WEIBO, ENV_WEIBO_TURSO_DATABASE_URL, ENV_WEIBO_TURSO_AUTH_TOKEN),
        (PLATFORM_XUEQIU, ENV_XUEQIU_TURSO_DATABASE_URL, ENV_XUEQIU_TURSO_AUTH_TOKEN),
    ):
        url = _env_text(url_env)
        if not url:
            continue
        token = _env_text(token_env)
        sources.append(LibsqlSource(name=name, url=url, token=token))

    return sources


def require_configured_libsql_sources_from_env() -> list[LibsqlSource]:
    sources = load_configured_libsql_sources_from_env()
    if sources:
        return sources
    raise RuntimeError(
        f"missing {ENV_WEIBO_TURSO_DATABASE_URL} or {ENV_XUEQIU_TURSO_DATABASE_URL}"
    )


def require_libsql_source_from_env(platform: str) -> LibsqlSource:
    sources = require_configured_libsql_sources_from_env()
    wanted = str(platform or "").strip().lower()
    for source in sources:
        if source.name == wanted:
            return source

    if wanted == PLATFORM_WEIBO:
        raise RuntimeError(f"missing {ENV_WEIBO_TURSO_DATABASE_URL}")
    if wanted == PLATFORM_XUEQIU:
        raise RuntimeError(f"missing {ENV_XUEQIU_TURSO_DATABASE_URL}")
    raise RuntimeError(f"unknown_platform:{wanted}")


__all__ = [
    "PLATFORM_WEIBO",
    "PLATFORM_XUEQIU",
    "LibsqlSource",
    "infer_platform_from_post_uid",
    "load_configured_libsql_sources_from_env",
    "require_configured_libsql_sources_from_env",
    "require_libsql_source_from_env",
]
