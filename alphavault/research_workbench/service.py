from __future__ import annotations

from alphavault.db.turso_db import TursoEngine, ensure_turso_engine
from alphavault.db.turso_env import require_configured_turso_sources_from_env
from alphavault.env import load_dotenv_if_present


def get_research_workbench_engine_from_env() -> TursoEngine:
    load_dotenv_if_present()
    sources = require_configured_turso_sources_from_env()
    preferred = next((s for s in sources if s.name == "weibo"), sources[0])
    return ensure_turso_engine(preferred.url, str(preferred.token or "").strip())


__all__ = ["get_research_workbench_engine_from_env"]
