# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Structure & Module Organization
- `alphavault/`: backend core — RSS ingest, AI analysis via `litellm`, Postgres/Redis persistence, worker logic.
- `alphavault_reflex/`: Reflex web UI (state, services, pages). Entry: `alphavault_reflex/alphavault_reflex.py`; config: `rxconfig.py`.
- `tests/`: `pytest` suite (`test_*.py`).
- `assets/`: static CSS/JS used by the UI.
- Root scripts: `weibo_rss_worker.py` (main worker entry), plus maintenance tools (`reset_ai_results.py`, `scan_and_reset_invalid_ai_tags.py`).
- `docs/superpowers/specs/`: design/architecture specs and notes.

## Build, Test, and Development Commands
Uses `uv` (lockfile: `uv.lock`).
- `uv sync`: install dependencies.
- `uv run pre-commit install`: install git hooks.
- `uv run pre-commit run -a`: run format/lint/type-check/spell-check/tests (Ruff, mypy, codespell, vulture, pytest).
- `uv run pytest`: run the core-path test suite.
- `uv run pytest tests/test_foo.py::test_bar`: run a single test.
- `uv run reflex run`: start the Reflex dev server.
- `uv run python weibo_rss_worker.py --log-level info`: run the RSS → AI → Postgres worker locally.
- `docker compose up --build`: run the full container on `http://localhost:8080` using `.env`.

## Architecture & Data Flow

### Worker pipeline (`weibo_rss_worker.py` → `alphavault/worker/`)
The main loop runs two parallel tracks:

1. **RSS ingest** (`worker_loop_rss.py`, `worker/ingest.py`, `worker/spool.py`): Fetches feeds via `alphavault/rss/utils.py`, pushes each payload straight into the Redis AI ready queue, and only writes local `spool/` when Redis is unavailable or push fails. `spool` is then only used as a recovery buffer, not as the daily primary path.

2. **AI processing** (`worker_loop_ai.py`, `worker/ai_processor.py`, `worker/post_processor_topic_prompt_v4.py`): A `ThreadPoolExecutor` pops payloads from Redis, claims a Redis lease for `post_uid`, invokes the LLM, then writes assertions atomically via `write_assertions_and_mark_done`. Success acks Redis and cleans up the matching spool fallback file if it exists; failures go to the Redis delayed retry queue.

**State signal for posts**: the cloud `posts` table no longer stores per-post AI runtime columns. The main durable signal is `processed_at`: unprocessed rows have `processed_at IS NULL`, and processed rows have `processed_at` filled. Only posts with `processed_at IS NOT NULL` are shown in the UI.

**Recovery path** (`worker_loop_maintenance.py`, `worker/spool.py`): maintenance scans `spool/`, drops files whose posts are already done, requeues unfinished payloads back to Redis, and requeues Redis `processing` jobs that lost their lease.

### Database layer (`alphavault/db/`)
- `postgres_db.py`: shared Postgres engine, connect helpers, schema qualification, and transactions. All SQL constants live in `db/sql/`.
- `source_queue.py`: main source post/assertion read-write helpers (`upsert_pending_post`, `load_cloud_post`, `write_assertions_and_mark_done`).
- `postgres_env.py`: parses `POSTGRES_DSN` into `weibo / xueqiu / standard` schema configs.
- One Postgres DSN is shared by `weibo / xueqiu / standard` schemas.

### AI layer (`alphavault/ai/`)
- `analyze.py`: public API — `analyze_with_litellm` calls the LLM, parses JSON output, normalizes assertion `action` values (via `ALLOWED_ACTIONS` + `LEGACY_ACTION_MAP`), and validates results.
- `_client.py` / `_litellm.py`: low-level LLM call with rate limiting (`RateLimiter`), streaming, and retries.
- `topic_prompt_v4.py` + `topic_prompt_v4_header.txt`: prompt construction. The prompt asks the model to return `assertions + mentions`，由系统自己再落 `topic_key` 和原词分桶字段。
- `tag_validate.py`: post-hoc validation of AI output tags.

### Stock object / alias layer (`alphavault/domains/stock` + `alphavault/domains/entity_match` + `alphavault/research_workbench`)
- `alphavault/domains/stock/object_index.py`: builds stock objects from fragmented `topic_key` / `stock_codes_json` / `stock_names_json`, and resolves aliases via confirmed relations.
- `alphavault/domains/entity_match/resolve.py`: main stock mention resolver. `stock_code` maps directly, `stock_name` uses `security_master`, `stock_alias` uses confirmed `alias_of`; unresolved aliases become candidates or `alias_resolve_tasks`.
- `alphavault/research_workbench/security_master_repo.py` + `relation_repo.py` + `alias_task_repo.py`: maintain truth tables, manual alias confirmation, and pending alias tasks.
- `alphavault/app/relation/candidate_builders.py`: builds relation candidates for the organizer.
- `alphavault/infra/ai/relation_candidate_ranker.py`: optional AI ranking for relation candidates (can be disabled).

### Reflex read layer (`alphavault_reflex/services/`)
- `source_read.py`: a small facade; actual loaders live in `*_loader.py` (`trade_board_loader.py`, `tree_loader.py`, `stock_fast_loader.py`, `url_loader.py`, `source_loader.py`).
- `stock_backfill.py`: finds posts that mention a stock but lack assertions, surfaced as "待回补" on the stock research page.

### Reflex UI (`alphavault_reflex/`)
- `alphavault_reflex.py`: app entry, registers all pages and the `/api/rss/trigger` API route.
- State is split: `HomeworkState` (trade flow `/homework`), `ResearchState` (stock/sector research pages), `OrganizerState` (organizer `/organizer`).
- Pages live in `pages/`; heavy data loading is done in `services/` and called from state event handlers.
- Custom CSS in `assets/` (`homework_board.css`, `research_workbench.css`) plus JS (`table_resizer.js`).

### Redis (required for AI worker)
The AI worker requires `REDIS_URL`. Redis holds the runtime queue state: ready / processing / delayed / lease / dedup. Postgres remains the durable truth, and maintenance can rebuild the Redis queue only from `spool/`. Without Redis the worker startup fails.

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
- The repository keeps only core-path tests.
- Run a single test: `uv run pytest tests/test_foo.py::test_bar -v`.
- Tests must be deterministic and fast; document any required env vars or link to `.env.example`.

## Commit & Pull Request Guidelines
- Commit messages: `feat(scope): ...`, `fix(scope): ...`, `refactor(scope): ...`, `chore: ...`, `docs: ...`.
- PRs: concise description + linked spec (`docs/superpowers/specs/...`) + UI screenshots for Reflex page changes.
- Before opening a PR: `uv run pre-commit run -a` must pass.

## Runtime Environment
- **Hosting**: Render free tier — 0.1 CPU, 512 MB RAM, deployed via `Dockerfile`.
- **Database**: Postgres (avoid unnecessary reads and writes).
- **Cache/Queue**: Upstash Redis free tier (command quota limited; use Redis sparingly).
- Resource constraints are real: every extra Postgres query or Redis command costs latency and capacity. Prefer batching, caching, and early-exit guards over repeated I/O.

## Configuration & Secrets
- Copy `.env.example` → `.env`; never commit secrets.
- Key env groups: `POSTGRES_DSN`, `REDIS_URL`, `AI_MODEL` / `AI_BASE_URL` / `AI_API_KEY` / `AI_API_MODE`.
- `AI_BASE_URL` must point to an OpenAI-compatible `/v1` endpoint (not a gateway homepage).
- `AI_MODEL` must be the real model name (e.g. `openai/gpt-5.2`), not a placeholder.
- `load_dotenv_if_present()` (in `alphavault/env.py`) loads `.env` without overriding existing env vars — safe for both local and Docker use.
