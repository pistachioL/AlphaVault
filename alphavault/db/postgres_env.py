from __future__ import annotations

import os
from dataclasses import dataclass

from alphavault.constants import (
    ENV_POSTGRES_DSN,
    SCHEMA_STANDARD,
    SCHEMA_WEIBO,
    SCHEMA_XUEQIU,
)


@dataclass(frozen=True)
class PostgresSource:
    name: str
    dsn: str
    schema: str


def load_configured_postgres_sources_from_env() -> list[PostgresSource]:
    dsn = os.getenv(ENV_POSTGRES_DSN, "").strip()
    if not dsn:
        return []
    return [
        PostgresSource(name="weibo", dsn=dsn, schema=SCHEMA_WEIBO),
        PostgresSource(name="xueqiu", dsn=dsn, schema=SCHEMA_XUEQIU),
        PostgresSource(name="standard", dsn=dsn, schema=SCHEMA_STANDARD),
    ]


__all__ = ["PostgresSource", "load_configured_postgres_sources_from_env"]
