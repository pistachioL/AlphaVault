from __future__ import annotations

import os
from functools import lru_cache

from alphavault.constants import SCHEMA_STANDARD
from alphavault.db.postgres_db import PostgresEngine, ensure_postgres_engine
from alphavault.db.postgres_env import require_configured_postgres_sources_from_env
from alphavault.env import load_dotenv_if_present


def _env_text(name: str) -> str:
    return os.getenv(name, "").strip()


@lru_cache(maxsize=2)
def _get_cached_research_workbench_engine(dsn: str) -> PostgresEngine:
    return ensure_postgres_engine(dsn)


def get_research_workbench_engine_from_env() -> PostgresEngine:
    load_dotenv_if_present()
    standard_source = next(
        (
            source
            for source in require_configured_postgres_sources_from_env()
            if source.schema == SCHEMA_STANDARD
        ),
        None,
    )
    if standard_source is None:
        raise RuntimeError(f"missing standard postgres source")
    return _get_cached_research_workbench_engine(standard_source.dsn)


__all__ = ["get_research_workbench_engine_from_env"]
