# Repository Guidelines

## Project Structure & Module Organization
- `alphavault/`: backend core (RSS ingest, AI analysis via `litellm`, Turso/Redis persistence, worker logic).
- `alphavault_reflex/`: Reflex web UI (state, services, pages). Key entry: `alphavault_reflex/alphavault_reflex.py`; config: `rxconfig.py`.
- `tests/`: `pytest` suite (`test_*.py`).
- `assets/`: static CSS/JS used by the UI.
- Root scripts: `weibo_rss_turso_worker.py` (main worker), plus one-off maintenance tools.
- `docs/superpowers/specs/`: design/architecture specs and notes.

## Build, Test, and Development Commands
Uses `uv` (lockfile: `uv.lock`).
- `uv sync`: install dependencies.
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

## 流程图与逻辑图写法标准
- 默认目标：图本身就要让人看懂，不依赖图外解释；用户只看图，也要知道整体逻辑、数据怎么流、哪里容易出问题。
- 默认语言：只用简体中文；必要的英文只保留真实表名、字段名、任务名、代码里的固定名字。
- 英文字段写法：先写中文意思，再在后面带英文名，例如 `股票标识（stock_key）`、`锁过期时间（locked_until）`；不要直接硬翻成“脏的股票键”这类不自然的话。
- 节点文案要求：用白话，按正常人说话的顺序写；不要用“主路 / 旁路 / 回放链路 / 头名 / 共现统计”这类图外才能懂的词，除非节点里把意思展开写清楚。
- 时间顺序要求：优先按真实执行顺序来画，从开始、检查、读取、筛选、整理、写回、结束依次展开；不要为了省节点把时序压扁。
- 数据流要求：图里必须写清楚“数据从哪里来，进哪张表，又从哪张表出去”；如果任务涉及数据库，必须把关键表名直接放进图里。
- 读写关系要求：关键步骤要标明是在读表、写表、更新表、删表里的哪类动作；不要只写抽象动作，不写落到哪张表。
- 表解释要求：图里出现的关键表，要直接写明“这张表是干什么的”；特别是队列表、待办表、锁表、缓存表，都要说明用途。
- 来源解释要求：如果图里有待办表、脏表、缓存表之类，图里要写清楚它“怎么来的”；例如是谁把数据写进去、在什么时机写进去。
- 判断条件要求：如果某一步靠规则判断，图里要写清楚具体规则；不要只写“够明显”“像简称”“像关系”“没查全”这种结论词。
- 规则表达要求：优先写成“怎么判断”的完整句子；如果代码里有固定阈值、固定批次上限、固定过滤条件，图里直接写出来。
- 业务词优先：字段和表的解释优先用业务话，例如“待重算的股票”“待刷新的股票”“帖子编号”；不要按字面硬翻技术词。
- 作用域要求：当前图只讲当前任务本身；不属于这次要讲的下游任务、旁边任务，默认不展开，除非用户明确要求一起画出来。
- 复杂度控制：不节约关键细节，但也不要把图写成流水账；保留会影响理解和排错的细节，删掉和当前问题无关的延伸内容。
- 用户反馈优先：如果用户指出某个词不懂、某个节点不顺、某个表没讲清楚，要直接改进图内文案，不要把解释放到图外补充代替。

## Configuration & Secrets
- Copy `.env.example` → `.env` and fill in values; never commit secrets (Turso tokens, API keys).
- Common variables: `*_TURSO_DATABASE_URL`, `*_TURSO_AUTH_TOKEN`, `REDIS_URL`, `AI_MODEL`, `AI_BASE_URL`, `AI_API_KEY`.
