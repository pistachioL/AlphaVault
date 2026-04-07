from __future__ import annotations

import os

from alphavault.constants import (
    ENV_STANDARD_TURSO_AUTH_TOKEN,
    ENV_STANDARD_TURSO_DATABASE_URL,
)
from alphavault.db.turso_db import TursoEngine, ensure_turso_engine
from alphavault.env import load_dotenv_if_present


def _env_text(name: str) -> str:
    return os.getenv(name, "").strip()


def get_research_workbench_engine_from_env() -> TursoEngine:
    load_dotenv_if_present()
    url = _env_text(ENV_STANDARD_TURSO_DATABASE_URL)
    if not url:
        raise RuntimeError(f"missing {ENV_STANDARD_TURSO_DATABASE_URL}")
    token = _env_text(ENV_STANDARD_TURSO_AUTH_TOKEN)
    return ensure_turso_engine(url, token)


__all__ = ["get_research_workbench_engine_from_env"]
