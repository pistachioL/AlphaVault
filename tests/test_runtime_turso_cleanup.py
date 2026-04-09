from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_TOKENS = (
    "TursoEngine",
    "TursoConnection",
    "ensure_turso_engine",
    "turso_connect_autocommit",
    "run_turso_transaction",
    "require_configured_turso_sources_from_env",
    "require_turso_source_from_env",
)
TARGET_PATHS = (
    PROJECT_ROOT / "alphavault",
    PROJECT_ROOT / "alphavault_reflex",
    PROJECT_ROOT / "startup_healthcheck.py",
)


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for target in TARGET_PATHS:
        if target.is_file():
            files.append(target)
            continue
        files.extend(sorted(target.rglob("*.py")))
    return files


def test_runtime_code_no_longer_references_turso_runtime_symbols() -> None:
    hits: list[str] = []
    for path in _iter_python_files():
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN_TOKENS:
            if token not in text:
                continue
            hits.append(f"{path.relative_to(PROJECT_ROOT)}::{token}")

    assert hits == []
