# Repository Guidelines

## Project Structure & Module Organization
- `alphavault/`: backend core (RSS ingest, AI analysis via `litellm`, Turso/Redis persistence, worker logic).
- `alphavault_reflex/`: Reflex web UI (state, services, pages). Key entry: `alphavault_reflex/alphavault_reflex.py`; config: `rxconfig.py`.
- `tests/`: `pytest` suite (`test_*.py`).
- `assets/`: static CSS/JS used by the UI.
- Root scripts: `weibo_rss_turso_worker.py` (main worker), `streamlit_app.py` (legacy UI shell), plus one-off maintenance tools.
- `docs/superpowers/specs/`: design/architecture specs and notes.

## Build, Test, and Development Commands
Uses `uv` (lockfile: `uv.lock`).
- `uv sync`: install dependencies.
- `uv sync --group streamlit`: install optional Streamlit extras.
- `uv run pre-commit install`: install git hooks.
- `uv run pre-commit run -a`: run format/lint/type-check/spell-check/tests (Ruff, mypy, codespell, vulture, pytest).
- `uv run pytest`: run tests.
- `uv run reflex run`: start the Reflex dev server.
- `uv run python weibo_rss_turso_worker.py --verbose`: run the RSS → AI → Turso worker locally.
- `docker compose up --build`: run the container on `http://localhost:8080` using `.env`.

## Coding Style & Naming Conventions
- Python 3.10+, 4-space indentation, add type hints for public APIs.
- Formatting/linting: Ruff (`ruff format`, `ruff check`). Type checking: mypy (via pre-commit).
- Naming: `snake_case` for modules/functions, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants, `test_*.py` for tests.
- Keep modules small and single-purpose; avoid deeply nested control flow (prefer guard clauses and helper functions).

## Testing Guidelines
- Framework: `pytest` (tests live in `tests/`).
- Keep unit tests deterministic and fast; if a test needs env/config, document required variables in the test or link to `.env.example`.

## Commit & Pull Request Guidelines
- Commit messages follow a scoped pattern used in `git log`: `feat(scope): ...`, `fix(scope): ...`, `refactor(scope): ...`, `chore: ...`, `docs: ...`.
- PRs should include: concise description, linked issue/spec (often `docs/superpowers/specs/...`), and UI screenshots for Reflex page changes.
- Before opening a PR, ensure `uv run pre-commit run -a` passes locally.

## Agent-Specific Notes
- Avoid duplication: reuse existing constants/config; don’t copy-paste similar strings or numbers.
- Keep complexity low: prefer guard clauses; avoid >3 levels of nesting; split large functions into helpers.
- No “god files”: if a file grows beyond ~500 lines or mixes unrelated responsibilities, split by domain/module.
- YAGNI: don’t add “maybe needed later” abstractions; only add backwards-compat fallbacks when explicitly required.

## Configuration & Secrets
- Copy `.env.example` → `.env` and fill in values; never commit secrets (Turso tokens, API keys).
- Common variables: `*_TURSO_DATABASE_URL`, `*_TURSO_AUTH_TOKEN`, `REDIS_URL`, `AI_MODEL`, `AI_BASE_URL`, `AI_API_KEY`.
