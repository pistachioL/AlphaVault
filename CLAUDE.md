# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Structure & Module Organization
- `alphavault/`: backend core — RSS ingest, AI analysis via `litellm`, Turso/Redis persistence, worker logic.
- `alphavault_reflex/`: Reflex web UI (state, services, pages). Entry: `alphavault_reflex/alphavault_reflex.py`; config: `rxconfig.py`.
- `tests/`: `pytest` suite (`test_*.py`).
- `assets/`: static CSS/JS used by the UI.
- Root scripts: `weibo_rss_turso_worker.py` (main worker entry), plus one-off maintenance tools (`backfill_display_md.py`, `reset_ai_results.py`, `scan_and_reset_invalid_ai_tags.py`).
- `docs/superpowers/specs/`: design/architecture specs and notes.

## Build, Test, and Development Commands
Uses `uv` (lockfile: `uv.lock`).
- `uv sync`: install dependencies.
- `uv run pre-commit install`: install git hooks.
- `uv run pre-commit run -a`: run format/lint/type-check/spell-check/tests (Ruff, mypy, codespell, vulture, pytest).
- `uv run pytest`: run all tests.
- `uv run pytest tests/test_foo.py::test_bar`: run a single test.
- `uv run reflex run`: start the Reflex dev server.
- `uv run python weibo_rss_turso_worker.py --verbose`: run the RSS → AI → Turso worker locally.
- `docker compose up --build`: run the full container on `http://localhost:8080` using `.env`.

## Architecture & Data Flow

### Worker pipeline (`weibo_rss_turso_worker.py` → `alphavault/worker/`)
The main loop runs two parallel tracks:

1. **RSS ingest** (`worker/ingest.py`): Fetches feeds via `alphavault/rss/utils.py`, deduplicates via Redis (or falls back to Turso), writes raw posts as `ai_status='pending'` rows via `db/turso_queue.py:upsert_pending_post`. Posts are also written to a local spool directory (`worker/spool.py`) as a crash-safe buffer.

2. **AI processing** (`worker/worker.py`, `ai/analyze.py`): A `ThreadPoolExecutor` picks up `pending` rows (`select_due_post_uids`), calls `try_mark_ai_running` (optimistic lock), invokes `analyze_with_litellm` (via `ai/_client.py` → `litellm`), then writes results atomically via `write_assertions_and_mark_done`. Errors go to `mark_ai_error` with exponential backoff via `next_retry_at`.

**State machine for posts**: `pending` → `running` → `done` (with `processed_at` set) or back to `pending` on retry. Only posts with `processed_at IS NOT NULL` are shown in the UI.

**Recovery helpers** in `turso_queue.py`: `recover_stuck_ai_tasks` resets tasks stuck in `running`, and `recover_done_without_processed_at` fixes inconsistent rows — both run on each maintenance cycle.

### Database layer (`alphavault/db/`)
- `turso_db.py`: `TursoEngine` (custom LIFO connection pool over `libsql`), `TursoConnection` (named→qmark param translation via `sqlparams`, retry on transient errors), `turso_savepoint` (manual `BEGIN/COMMIT/ROLLBACK` since libsql doesn't support DBAPI transactions). All SQL constants live in `db/sql/`.
- `turso_queue.py`: queue-specific read/write helpers (upsert, select-due, mark-running, write-done, error, recovery). This is the main write path for the worker.
- `turso_env.py`: parses `WEIBO_TURSO_DATABASE_URL/AUTH_TOKEN` and `XUEQIU_TURSO_DATABASE_URL/AUTH_TOKEN` into source configs.
- Two Turso databases are supported simultaneously (weibo + xueqiu); each has its own engine.

### AI layer (`alphavault/ai/`)
- `analyze.py`: public API — `analyze_with_litellm` calls the LLM, parses JSON output, normalizes assertion `action` values (via `ALLOWED_ACTIONS` + `LEGACY_ACTION_MAP`), and validates results.
- `_client.py` / `_litellm.py`: low-level LLM call with rate limiting (`RateLimiter`), streaming, and retries.
- `topic_prompt_v3.py` + `topic_prompt_v3_header.txt`: prompt construction. The prompt asks the model to return structured `assertions` (with `topic_key`, `action`, `stock_codes_json`, `stock_names_json`, etc.).
- `tag_validate.py`: post-hoc validation of AI output tags.

### Stock object / alias layer (`alphavault/domains/stock` + `alphavault/infra/ai`)
- `alphavault/domains/stock/object_index.py`: builds stock objects from fragmented `topic_key` / `stock_codes_json` / `stock_names_json`, and resolves aliases via confirmed relations.
- `alphavault/infra/ai/stock_alias.py`: optional AI-assisted alias resolving for short names / nicknames (can be disabled).
- `alphavault/app/relation/candidate_builders.py`: builds relation candidates for the organizer.
- `alphavault/infra/ai/relation_candidate_ranker.py`: optional AI ranking for relation candidates (can be disabled).

### Reflex read layer (`alphavault_reflex/services/`)
- `turso_read.py`: a small facade; actual loaders live in `*_loader.py` (`trade_board_loader.py`, `tree_loader.py`, `stock_fast_loader.py`, `url_loader.py`, `source_loader.py`).
- `stock_backfill.py`: finds posts that mention a stock but lack assertions, surfaced as "待回补" on the stock research page.

### Reflex UI (`alphavault_reflex/`)
- `alphavault_reflex.py`: app entry, registers all pages and the `/api/rss/trigger` API route.
- State is split: `HomeworkState` (trade flow `/homework`), `ResearchState` (stock/sector research pages), `OrganizerState` (organizer `/organizer`).
- Pages live in `pages/`; heavy data loading is done in `services/` and called from state event handlers.
- Custom CSS in `assets/` (`homework_board.css`, `research_workbench.css`) plus JS (`table_resizer.js`).

### Redis (optional)
When `REDIS_URL` is set, Redis acts as the primary AI work queue and dedup store, reducing Turso read pressure. Author recent-post context is also cached in Redis. Without Redis the system falls back to Turso for all state.

## Coding Style & Naming Conventions
- Python 3.10+, 4-space indentation, type hints on public APIs.
- Formatting/linting: Ruff (`ruff format`, `ruff check`). Type checking: mypy (via pre-commit).
- Naming: `snake_case` for modules/functions, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants, `test_*.py` for tests.
- All env var names are centralized as constants in `alphavault/constants.py` — always use those, never raw strings.
- SQL statements live in `alphavault/db/sql/` as string constants, not inline in business logic.
- Keep modules small and single-purpose; avoid >3 levels of nesting; prefer guard clauses.
- No "god files": split at ~500 lines or when responsibilities diverge.
- YAGNI: no speculative abstractions, no backwards-compat shims unless explicitly required.

## Testing
- Framework: `pytest` (tests in `tests/`).
- Run a single test: `uv run pytest tests/test_foo.py::test_bar -v`.
- Tests must be deterministic and fast; document any required env vars or link to `.env.example`.

## Commit & Pull Request Guidelines
- Commit messages: `feat(scope): ...`, `fix(scope): ...`, `refactor(scope): ...`, `chore: ...`, `docs: ...`.
- PRs: concise description + linked spec (`docs/superpowers/specs/...`) + UI screenshots for Reflex page changes.
- Before opening a PR: `uv run pre-commit run -a` must pass.

## Runtime Environment
- **Hosting**: Render free tier — 0.1 CPU, 512 MB RAM, deployed via `Dockerfile`.
- **Database**: Turso free tier (read/write quota limited; avoid unnecessary reads).
- **Cache/Queue**: Upstash Redis free tier (command quota limited; use Redis sparingly).
- Resource constraints are real: every extra Turso read or Redis command counts against daily free quotas. Prefer batching, caching, and early-exit guards over repeated I/O.

## Configuration & Secrets
- Copy `.env.example` → `.env`; never commit secrets.
- Key env groups: `WEIBO_TURSO_*` / `XUEQIU_TURSO_*`, `REDIS_URL`, `AI_MODEL` / `AI_BASE_URL` / `AI_API_KEY` / `AI_API_MODE`.
- `AI_BASE_URL` must point to an OpenAI-compatible `/v1` endpoint (not a gateway homepage).
- `AI_MODEL` must be the real model name (e.g. `openai/gpt-5.2`), not a placeholder.
- `load_dotenv_if_present()` (in `alphavault/env.py`) loads `.env` without overriding existing env vars — safe for both local and Docker use.
