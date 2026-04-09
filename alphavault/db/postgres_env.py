from __future__ import annotations

import os
from dataclasses import dataclass

from alphavault.constants import (
    ENV_POSTGRES_DSN,
    PLATFORM_WEIBO,
    PLATFORM_XUEQIU,
    SCHEMA_STANDARD,
    SCHEMA_WEIBO,
    SCHEMA_XUEQIU,
)


@dataclass(frozen=True)
class PostgresSource:
    name: str
    dsn: str
    schema: str

    @property
    def url(self) -> str:
        return self.dsn

    @property
    def token(self) -> str:
        return ""


def infer_platform_from_post_uid(post_uid: object) -> str:
    value = str(post_uid or "").strip().lower()
    if value.startswith(f"{PLATFORM_WEIBO}:"):
        return PLATFORM_WEIBO
    if value.startswith(f"{PLATFORM_XUEQIU}:"):
        return PLATFORM_XUEQIU
    return ""


def require_postgres_source_platform(value: object) -> str:
    raw = str(value or "").strip()
    platform = infer_platform_from_post_uid(raw)
    if platform:
        return platform
    lowered = raw.lower()
    if lowered in (PLATFORM_WEIBO, PLATFORM_XUEQIU):
        return lowered
    raise RuntimeError(f"unknown_source_platform:{raw}")


def load_configured_postgres_sources_from_env() -> list[PostgresSource]:
    dsn = os.getenv(ENV_POSTGRES_DSN, "").strip()
    if not dsn:
        return []
    return [
        PostgresSource(name="weibo", dsn=dsn, schema=SCHEMA_WEIBO),
        PostgresSource(name="xueqiu", dsn=dsn, schema=SCHEMA_XUEQIU),
        PostgresSource(name="standard", dsn=dsn, schema=SCHEMA_STANDARD),
    ]


def require_configured_postgres_sources_from_env() -> list[PostgresSource]:
    sources = load_configured_postgres_sources_from_env()
    if sources:
        return sources
    raise RuntimeError(f"missing {ENV_POSTGRES_DSN}")


def require_postgres_source_from_env(name: str) -> PostgresSource:
    wanted = str(name or "").strip().lower()
    for source in require_configured_postgres_sources_from_env():
        if source.name == wanted:
            return source
    raise RuntimeError(f"unknown_postgres_source:{wanted}")


__all__ = [
    "PostgresSource",
    "infer_platform_from_post_uid",
    "load_configured_postgres_sources_from_env",
    "require_postgres_source_platform",
    "require_configured_postgres_sources_from_env",
    "require_postgres_source_from_env",
]
