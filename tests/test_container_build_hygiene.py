from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE_PATH = PROJECT_ROOT / "Dockerfile"
DOCKERIGNORE_PATH = PROJECT_ROOT / ".dockerignore"
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
STREAMLIT_APP_PATH = PROJECT_ROOT / "streamlit_app.py"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_pyproject() -> dict:
    return tomllib.loads(_read_text(PYPROJECT_PATH))


def test_dockerfile_prunes_uv_cache_and_no_pip_cache_mount() -> None:
    dockerfile_text = _read_text(DOCKERFILE_PATH)
    assert "uv cache prune --ci" in dockerfile_text
    assert "--mount=type=cache,target=/root/.cache/pip" not in dockerfile_text


def test_dockerfile_runtime_copy_is_selective() -> None:
    dockerfile_text = _read_text(DOCKERFILE_PATH)
    assert "COPY --from=builder /app /app" not in dockerfile_text
    assert "COPY --from=builder /app/.venv /app/.venv" in dockerfile_text
    assert "COPY --from=builder /app/.web /app/.web" in dockerfile_text


def test_dockerignore_excludes_frontend_artifacts() -> None:
    dockerignore_text = _read_text(DOCKERIGNORE_PATH)
    assert ".web/" in dockerignore_text
    assert ".reflex/" in dockerignore_text


def test_streamlit_dependency_is_removed() -> None:
    pyproject = _load_pyproject()
    project_deps = pyproject["project"]["dependencies"]
    assert "streamlit" not in project_deps
    dependency_groups = pyproject.get("dependency-groups", {})
    assert "streamlit" not in dependency_groups


def test_streamlit_entry_is_removed() -> None:
    assert STREAMLIT_APP_PATH.exists() is False
