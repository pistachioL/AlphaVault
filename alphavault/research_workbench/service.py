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


def _load_standard_source_from_env():
    load_dotenv_if_present()
    return next(
        (
            source
            for source in require_configured_postgres_sources_from_env()
            if source.schema == SCHEMA_STANDARD
        ),
        None,
    )


def get_research_workbench_engine_from_env() -> PostgresEngine:
    standard_source = _load_standard_source_from_env()
    if standard_source is None:
        raise RuntimeError("missing standard postgres source")
    return _get_cached_research_workbench_engine(standard_source.dsn)


def dispose_research_workbench_engine_from_env() -> None:
    standard_source = _load_standard_source_from_env()
    if standard_source is None:
        return
    engine = _get_cached_research_workbench_engine(standard_source.dsn)
    try:
        engine.dispose()
    finally:
        _get_cached_research_workbench_engine.cache_clear()


__all__ = [
    "dispose_research_workbench_engine_from_env",
    "get_research_workbench_engine_from_env",
]
