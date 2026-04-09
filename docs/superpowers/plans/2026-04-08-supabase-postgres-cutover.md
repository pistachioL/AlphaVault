# Supabase Postgres Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把运行时数据库从 `Turso/libsql` 切到 `Supabase Postgres`，保留 `weibo / xueqiu / standard` 三个 schema 边界，并让 worker、Reflex、research workbench、迁移脚本都能在新库上工作。

**Architecture:** 继续保留现在“手写 SQL + 小 helper”的结构，只把最底层连接、事务、环境变量入口换成 `Postgres` 版本。运行时只用一个 `POSTGRES_DSN`，所有 SQL 直接写带 schema 的全名，不依赖 `search_path`，两份 schema 真相文件通过 `{{schema_name}}` 模板装到目标 schema。

**Tech Stack:** `psycopg`, `psycopg_pool`, `sqlparams`, `pytest`, 本地 `Postgres`, `mise`, `uv`

---

## 文件结构

### 新增文件

- `alphavault/db/postgres_db.py`
  - `PostgresEngine`
  - `PostgresConnection`
  - `postgres_connect_autocommit()`
  - `run_postgres_transaction()`
  - `ensure_postgres_engine()`
- `alphavault/db/postgres_env.py`
  - `PostgresSource`
  - `load_configured_postgres_sources_from_env()`
  - `require_postgres_source_from_env()`
- `tests/conftest.py`
  - 本地 `Postgres` 连接 fixture
  - 每个测试会话独立 schema 或独立清理逻辑
- `tests/test_postgres_env.py`
  - 环境变量读取测试
- `tests/test_postgres_db.py`
  - 连接层、事务、参数绑定、映射结果测试
- `mise.toml`
  - 本地临时 `Postgres` 测试任务
- `migrate_turso_to_postgres.py`
  - 一次性全量迁移脚本

### 重点修改文件

- `pyproject.toml`
- `.env.example`
- `README.md`
- `alphavault/constants.py`
- `alphavault/db/cloud_schema.py`
- `alphavault/db/sql/source_schema.sql`
- `alphavault/db/sql/standard_schema.sql`
- `alphavault/db/turso_queue.py`
- `alphavault/research_workbench/service.py`
- `alphavault/research_workbench/schema.py`
- `alphavault/research_workbench/security_master_repo.py`
- `alphavault/research_workbench/relation_repo.py`
- `alphavault/research_workbench/candidate_repo.py`
- `alphavault/research_workbench/alias_task_repo.py`
- `alphavault/research_workbench/shadow_dict_repo.py`
- `alphavault/research_stock_cache.py`
- `alphavault/homework_trade_feed.py`
- `alphavault/follow_pages.py`
- `alphavault/worker/cli.py`
- `alphavault/worker/ingest.py`
- `alphavault/worker/spool.py`
- `alphavault/worker/job_state.py`
- `alphavault/worker/turso_runtime.py`
- `alphavault/worker/worker_loop_runtime.py`
- `alphavault/worker/runtime_models.py`
- `alphavault_reflex/services/source_loader.py`
- `alphavault_reflex/services/tree_loader.py`
- `alphavault_reflex/services/url_loader.py`
- `alphavault_reflex/services/stock_fast_loader.py`
- `alphavault_reflex/services/trade_board_loader.py`
- `alphavault_reflex/services/stock_hot_read.py`
- `alphavault_reflex/services/sector_hot_read.py`
- `alphavault_reflex/services/turso_read.py`
- `startup_healthcheck.py`
- 直接依赖 `libsql` 或 `Turso*` 的测试文件

### 保守边界

- 第一阶段先切运行时和测试，不急着批量 rename 所有 `turso_*` 文件名。
- 只有确认代码里不再 import `Turso*` 运行时类型后，才做删除或重命名清理。
- `libsql` 依赖先保留到迁移脚本落好并通过验收；只有确认仓库里不再需要用它读旧库，才考虑删。

### 统一命名

- 常量：
  - `ENV_POSTGRES_DSN`
  - `ENV_POSTGRES_POOL_MAX_SIZE`
  - `SCHEMA_WEIBO`
  - `SCHEMA_XUEQIU`
  - `SCHEMA_STANDARD`
- schema 全名：
  - `weibo.posts`
  - `xueqiu.posts`
  - `standard.relations`

## Task 1: 依赖和环境变量入口

**Files:**
- Modify: `pyproject.toml`
- Modify: `alphavault/constants.py`
- Create: `alphavault/db/postgres_env.py`
- Create: `tests/test_postgres_env.py`

- [ ] **Step 1: 写环境变量读取测试**

```python
from alphavault.db.postgres_env import load_configured_postgres_sources_from_env


def test_load_configured_postgres_sources_from_single_dsn(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://postgres:postgres@127.0.0.1:55432/postgres")
    sources = load_configured_postgres_sources_from_env()
    assert [source.name for source in sources] == ["weibo", "xueqiu", "standard"]
```

- [ ] **Step 2: 运行测试确认先失败**

Run: `uv run pytest tests/test_postgres_env.py -v`
Expected: FAIL，提示 `alphavault.db.postgres_env` 不存在

- [ ] **Step 3: 写最小实现**

```python
ENV_POSTGRES_DSN = "POSTGRES_DSN"
ENV_POSTGRES_POOL_MAX_SIZE = "POSTGRES_POOL_MAX_SIZE"
SCHEMA_WEIBO = "weibo"
SCHEMA_XUEQIU = "xueqiu"
SCHEMA_STANDARD = "standard"
```

```python
@dataclass(frozen=True)
class PostgresSource:
    name: str
    dsn: str
    schema: str
```

```python
def load_configured_postgres_sources_from_env() -> list[PostgresSource]:
    dsn = os.getenv(ENV_POSTGRES_DSN, "").strip()
    if not dsn:
        return []
    return [
        PostgresSource(name="weibo", dsn=dsn, schema=SCHEMA_WEIBO),
        PostgresSource(name="xueqiu", dsn=dsn, schema=SCHEMA_XUEQIU),
        PostgresSource(name="standard", dsn=dsn, schema=SCHEMA_STANDARD),
    ]
```

- [ ] **Step 4: 补依赖并跑测试**

Run: `uv sync`
Expected: `psycopg` / `psycopg_pool` 安装完成

Run: `uv run pytest tests/test_postgres_env.py -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add pyproject.toml alphavault/constants.py alphavault/db/postgres_env.py tests/test_postgres_env.py uv.lock
git commit -m "feat(db): add postgres env config"
```

## Task 2: 本地 Postgres 测试基座

**Files:**
- Create or Modify: `mise.toml`
- Create: `tests/conftest.py`
- Create: `tests/test_postgres_db.py`

- [ ] **Step 1: 写连接冒烟测试**

```python
def test_postgres_fixture_can_connect(pg_conn):
    assert pg_conn.execute("SELECT 1").fetchone()[0] == 1
```

- [ ] **Step 2: 运行测试确认先失败**

Run: `uv run pytest tests/test_postgres_db.py::test_postgres_fixture_can_connect -v`
Expected: FAIL，提示 `pg_conn` fixture 不存在

- [ ] **Step 3: 写最小测试基座**

```toml
[tools]
postgres = "latest"

[tasks."test:db-hook"]
run = """
#!/bin/bash
set -euo pipefail
...
export TEST_POSTGRES_DSN="${TEST_POSTGRES_DSN:-postgresql://$(id -un)@127.0.0.1:$PGPORT/postgres}"
if [[ -n "${DB_TEST_CMD:-}" ]]; then
    eval "$DB_TEST_CMD"
else
    psql -p $PGPORT -d postgres -c "SELECT 1;"
fi
"""
```

```python
@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    return os.getenv(
        "TEST_POSTGRES_DSN",
        "postgresql://postgres:postgres@127.0.0.1:55432/postgres",
    )
```

```python
@pytest.fixture()
def pg_conn(postgres_dsn: str):
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute("BEGIN")
        try:
            yield conn
        finally:
            conn.rollback()
```

- [ ] **Step 4: 启本地临时库并跑测试**

Run: `DB_TEST_CMD='uv run pytest tests/test_postgres_db.py::test_postgres_fixture_can_connect -v' mise run test:db-hook`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add mise.toml tests/conftest.py tests/test_postgres_db.py
git commit -m "test(db): add local postgres test harness"
```

## Task 3: Postgres 连接层

**Files:**
- Create: `alphavault/db/postgres_db.py`
- Modify: `tests/test_postgres_db.py`

- [ ] **Step 1: 写失败测试，锁定参数绑定和事务**

```python
from alphavault.db.postgres_db import ensure_postgres_engine, run_postgres_transaction


def test_postgres_execute_supports_named_params(pg_conn):
    pg_conn.execute("CREATE TEMP TABLE t(id integer primary key, v text not null)")
    pg_conn.execute("INSERT INTO t(id, v) VALUES (:id, :v)", {"id": 1, "v": "a"})
    row = pg_conn.execute("SELECT v FROM t WHERE id = :id", {"id": 1}).fetchone()
    assert row[0] == "a"
```

```python
def test_run_postgres_transaction_rolls_back_on_error(postgres_dsn):
    engine = ensure_postgres_engine(postgres_dsn)
    with pytest.raises(RuntimeError):
        run_postgres_transaction(engine, _boom)
```

- [ ] **Step 2: 运行测试确认先失败**

Run: `uv run pytest tests/test_postgres_db.py -v`
Expected: FAIL，提示 `postgres_db` 模块或方法不存在

- [ ] **Step 3: 写最小实现**

```python
_NAMED_TO_PYFORMAT = sqlparams.SQLParams("named", "pyformat", escape_char=True)
```

```python
class PostgresEngine:
    def __init__(self, dsn: str, max_connections: int):
        self._pool = ConnectionPool(conninfo=dsn, max_size=max_connections, open=True)
```

```python
def run_postgres_transaction(engine_or_conn, fn):
    if isinstance(engine_or_conn, PostgresConnection):
        with engine_or_conn.transaction():
            return fn(engine_or_conn)
    with postgres_connect_autocommit(engine_or_conn) as conn:
        with conn.transaction():
            return fn(conn)
```

- [ ] **Step 4: 跑连接层测试**

Run: `uv run pytest tests/test_postgres_db.py -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add alphavault/db/postgres_db.py tests/test_postgres_db.py
git commit -m "feat(db): add postgres connection layer"
```

## Task 4: schema 模板和装库入口

**Files:**
- Modify: `alphavault/db/cloud_schema.py`
- Modify: `alphavault/db/sql/source_schema.sql`
- Modify: `alphavault/db/sql/standard_schema.sql`
- Modify: `tests/test_cloud_schema.py`

- [ ] **Step 1: 先写失败测试，锁定 schema 模板装库**

```python
def test_apply_cloud_schema_source_installs_into_target_schema(pg_conn):
    apply_cloud_schema(pg_conn, target="source", schema_name="weibo")
    row = pg_conn.execute(
        """
        SELECT to_regclass('weibo.posts')
        """
    ).fetchone()
    assert row[0] == "weibo.posts"
```

```python
def test_apply_cloud_schema_standard_installs_into_standard_schema(pg_conn):
    apply_cloud_schema(pg_conn, target="standard", schema_name="standard")
    row = pg_conn.execute("SELECT to_regclass('standard.relations')").fetchone()
    assert row[0] == "standard.relations"
```

- [ ] **Step 2: 运行测试确认先失败**

Run: `uv run pytest tests/test_cloud_schema.py -v`
Expected: FAIL，提示旧 `sqlite_schema` / `PRAGMA` 断言不再适用

- [ ] **Step 3: 写最小实现**

```sql
CREATE SCHEMA IF NOT EXISTS {{schema_name}};

CREATE TABLE IF NOT EXISTS {{schema_name}}.posts (
    post_uid TEXT PRIMARY KEY,
    ...
);
```

```python
def render_cloud_schema_sql(sql_text: str, *, schema_name: str) -> str:
    return sql_text.replace("{{schema_name}}", schema_name)
```

```python
def apply_cloud_schema(engine_or_conn, *, target: str, schema_name: str | None = None):
    ...
```

- [ ] **Step 4: 跑 schema 测试**

Run: `uv run pytest tests/test_cloud_schema.py -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add alphavault/db/cloud_schema.py alphavault/db/sql/source_schema.sql alphavault/db/sql/standard_schema.sql tests/test_cloud_schema.py
git commit -m "feat(db): add postgres schema templating"
```

## Task 5: standard schema 读写链路

**Files:**
- Modify: `alphavault/research_workbench/service.py`
- Modify: `alphavault/research_workbench/schema.py`
- Modify: `alphavault/research_workbench/security_master_repo.py`
- Modify: `alphavault/research_workbench/relation_repo.py`
- Modify: `alphavault/research_workbench/candidate_repo.py`
- Modify: `alphavault/research_workbench/alias_task_repo.py`
- Modify: `alphavault/research_workbench/shadow_dict_repo.py`
- Modify: `alphavault/homework_trade_feed.py`
- Modify: `alphavault/follow_pages.py`
- Modify: `tests/test_research_workbench.py`
- Modify: `tests/test_homework_trade_feed.py`

- [ ] **Step 1: 写失败测试，锁定 standard 侧读写**

```python
def test_record_stock_sector_relation_uses_standard_schema(pg_conn):
    apply_cloud_schema(pg_conn, target="standard", schema_name="standard")
    record_stock_sector_relation(pg_conn, ...)
    row = pg_conn.execute("SELECT count(*) FROM standard.relations").fetchone()
    assert row[0] == 1
```

```python
def test_save_homework_trade_feed_writes_standard_schema(pg_conn):
    apply_cloud_schema(pg_conn, target="standard", schema_name="standard")
    save_homework_trade_feed(pg_conn, ...)
    row = pg_conn.execute("SELECT count(*) FROM standard.homework_trade_feed").fetchone()
    assert row[0] == 1
```

- [ ] **Step 2: 运行测试确认先失败**

Run: `uv run pytest tests/test_research_workbench.py tests/test_homework_trade_feed.py -v`
Expected: FAIL，提示 `TursoConnection` / `sqlite_schema` / 缺少 schema 全名

- [ ] **Step 3: 写最小实现**

```python
def get_research_workbench_engine_from_env() -> PostgresEngine:
    sources = require_configured_postgres_sources_from_env()
    standard = next(source for source in sources if source.name == "standard")
    return ensure_postgres_engine(standard.dsn)
```

```python
RESEARCH_RELATIONS_TABLE = f"{SCHEMA_STANDARD}.relations"
HOMEWORK_TRADE_FEED_TABLE = f"{SCHEMA_STANDARD}.homework_trade_feed"
```

- [ ] **Step 4: 跑 standard 相关测试**

Run: `uv run pytest tests/test_research_workbench.py tests/test_homework_trade_feed.py -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add alphavault/research_workbench/service.py alphavault/research_workbench/schema.py alphavault/research_workbench/security_master_repo.py alphavault/research_workbench/relation_repo.py alphavault/research_workbench/candidate_repo.py alphavault/research_workbench/alias_task_repo.py alphavault/research_workbench/shadow_dict_repo.py alphavault/homework_trade_feed.py alphavault/follow_pages.py tests/test_research_workbench.py tests/test_homework_trade_feed.py
git commit -m "feat(db): cut standard schema writes to postgres"
```

## Task 6: source schema 读写链路和 worker

**Files:**
- Modify: `alphavault/db/turso_queue.py`
- Modify: `alphavault/research_stock_cache.py`
- Modify: `alphavault/worker/job_state.py`
- Modify: `alphavault/worker/ingest.py`
- Modify: `alphavault/worker/spool.py`
- Modify: `alphavault/worker/turso_runtime.py`
- Modify: `alphavault/worker/worker_loop_runtime.py`
- Modify: `alphavault/worker/runtime_models.py`
- Modify: `alphavault/worker/research_stock_cache.py`
- Modify: `tests/test_turso_db.py`
- Modify: `tests/test_research_stock_cache.py`
- Modify: `tests/test_worker_stock_hot_cache.py`
- Modify: `tests/test_turso_runtime.py`

- [ ] **Step 1: 写失败测试，锁定 source 侧事务和 worker 最小路径**

```python
def test_write_assertions_and_mark_done_writes_weibo_schema(pg_conn):
    apply_cloud_schema(pg_conn, target="source", schema_name="weibo")
    write_assertions_and_mark_done(pg_conn, ...)
    row = pg_conn.execute("SELECT count(*) FROM weibo.assertions").fetchone()
    assert row[0] == 1
```

```python
def test_job_state_reads_and_writes_xueqiu_schema(pg_conn):
    apply_cloud_schema(pg_conn, target="source", schema_name="xueqiu")
    save_job_state(pg_conn, ...)
    row = pg_conn.execute("SELECT count(*) FROM xueqiu.worker_cursor").fetchone()
    assert row[0] == 1
```

- [ ] **Step 2: 运行测试确认先失败**

Run: `uv run pytest tests/test_turso_db.py tests/test_research_stock_cache.py tests/test_turso_runtime.py tests/test_worker_stock_hot_cache.py -v`
Expected: FAIL，提示旧 `Turso*` 类型和旧 schema 假设不成立

- [ ] **Step 3: 写最小实现**

```python
POSTS_TABLE = f"{SCHEMA_WEIBO}.posts"
ASSERTIONS_TABLE = f"{SCHEMA_WEIBO}.assertions"
```

```python
def source_table(schema_name: str, table: str) -> str:
    return f"{schema_name}.{table}"
```

```python
def load_cloud_post(engine: PostgresEngine, post_uid: str) -> CloudPost:
    ...
```

- [ ] **Step 4: 跑 source 相关测试**

Run: `uv run pytest tests/test_turso_db.py tests/test_research_stock_cache.py tests/test_turso_runtime.py tests/test_worker_stock_hot_cache.py -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add alphavault/db/turso_queue.py alphavault/research_stock_cache.py alphavault/worker/job_state.py alphavault/worker/ingest.py alphavault/worker/spool.py alphavault/worker/turso_runtime.py alphavault/worker/worker_loop_runtime.py alphavault/worker/runtime_models.py alphavault/worker/research_stock_cache.py tests/test_turso_db.py tests/test_research_stock_cache.py tests/test_turso_runtime.py tests/test_worker_stock_hot_cache.py
git commit -m "feat(db): cut source schema writes to postgres"
```

## Task 7: Reflex 读库链路、配置和健康检查

**Files:**
- Modify: `alphavault_reflex/services/source_loader.py`
- Modify: `alphavault_reflex/services/tree_loader.py`
- Modify: `alphavault_reflex/services/url_loader.py`
- Modify: `alphavault_reflex/services/stock_fast_loader.py`
- Modify: `alphavault_reflex/services/trade_board_loader.py`
- Modify: `alphavault_reflex/services/stock_hot_read.py`
- Modify: `alphavault_reflex/services/sector_hot_read.py`
- Modify: `alphavault_reflex/services/turso_read.py`
- Modify: `startup_healthcheck.py`
- Modify: `alphavault/worker/cli.py`
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `tests/test_source_loader.py`
- Modify: `tests/test_turso_read.py`
- Modify: `tests/test_turso_read_fast_stock.py`
- Modify: `tests/test_startup_healthcheck.py`

- [ ] **Step 1: 写失败测试，锁定 Reflex 和启动检查**

```python
def test_load_trade_assertions_from_env_uses_postgres_dsn(monkeypatch, ...):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://...")
    frame, err = load_trade_assertions_from_env()
    assert err == ""
```

```python
def test_startup_healthcheck_uses_postgres_dsn(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://...")
    ...
```

- [ ] **Step 2: 运行测试确认先失败**

Run: `uv run pytest tests/test_source_loader.py tests/test_turso_read.py tests/test_turso_read_fast_stock.py tests/test_startup_healthcheck.py -v`
Expected: FAIL，提示旧 `*_TURSO_*` 环境变量和旧 helper 仍在使用

- [ ] **Step 3: 写最小实现**

```python
MISSING_POSTGRES_DSN_ERROR = f"Missing {ENV_POSTGRES_DSN}"
```

```python
engine = ensure_postgres_engine(os.getenv(ENV_POSTGRES_DSN, "").strip())
```

```python
STARTUP_HEALTHCHECK_DB_TARGET = "standard"
```

- [ ] **Step 4: 跑 Reflex 和配置测试**

Run: `uv run pytest tests/test_source_loader.py tests/test_turso_read.py tests/test_turso_read_fast_stock.py tests/test_startup_healthcheck.py -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add alphavault_reflex/services/source_loader.py alphavault_reflex/services/tree_loader.py alphavault_reflex/services/url_loader.py alphavault_reflex/services/stock_fast_loader.py alphavault_reflex/services/trade_board_loader.py alphavault_reflex/services/stock_hot_read.py alphavault_reflex/services/sector_hot_read.py alphavault_reflex/services/turso_read.py startup_healthcheck.py alphavault/worker/cli.py .env.example README.md tests/test_source_loader.py tests/test_turso_read.py tests/test_turso_read_fast_stock.py tests/test_startup_healthcheck.py
git commit -m "feat(db): switch reflex and startup checks to postgres"
```

## Task 8: 一次性迁移脚本和收尾清理

**Files:**
- Create: `migrate_turso_to_postgres.py`
- Modify: `tests/test_migrate_standard_history_tables.py`
- Modify: `tests/test_migrate_weibo_raw_text.py`
- Modify: `tests/test_cloud_schema_helpers_removed.py`
- Modify: `pyproject.toml`（如果最终可以删 `libsql`）
- Delete or Modify: `alphavault/db/turso_db.py`
- Delete or Modify: `alphavault/db/turso_env.py`

- [ ] **Step 1: 写失败测试，锁定迁移对数**

```python
def test_migrate_turso_to_postgres_copies_posts_and_assertions(...):
    summary = migrate_all(...)
    assert summary["weibo_posts"] == 1
    assert summary["standard_relations"] == 1
```

- [ ] **Step 2: 运行测试确认先失败**

Run: `uv run pytest tests/test_migrate_standard_history_tables.py tests/test_migrate_weibo_raw_text.py -v`
Expected: FAIL，提示旧脚本只认 `Turso` 或目标还是本地 `libsql`

- [ ] **Step 3: 写最小迁移和清理实现**

```python
def migrate_all(*, turso_weibo_url: str, turso_xueqiu_url: str, postgres_dsn: str) -> dict[str, int]:
    ...
```

```python
with libsql.connect(turso_url, auth_token=token, isolation_level=None) as old_conn:
    with psycopg.connect(postgres_dsn) as new_conn:
        ...
```

```python
if no_runtime_turso_imports:
    remove_old_runtime_helpers()
```

- [ ] **Step 4: 跑迁移和全量测试**

Run: `uv run pytest tests/test_postgres_env.py tests/test_postgres_db.py tests/test_cloud_schema.py tests/test_research_workbench.py tests/test_homework_trade_feed.py tests/test_turso_db.py tests/test_research_stock_cache.py tests/test_source_loader.py tests/test_turso_read.py tests/test_turso_read_fast_stock.py tests/test_startup_healthcheck.py tests/test_migrate_standard_history_tables.py tests/test_migrate_weibo_raw_text.py -v`
Expected: PASS

Run: `uv run pre-commit run -a`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add migrate_turso_to_postgres.py tests/test_migrate_standard_history_tables.py tests/test_migrate_weibo_raw_text.py tests/test_cloud_schema_helpers_removed.py pyproject.toml uv.lock alphavault/db/turso_db.py alphavault/db/turso_env.py
git commit -m "feat(db): add turso to postgres cutover path"
```

## 最终验收清单

- [ ] `rg -n "TursoEngine|TursoConnection|ensure_turso_engine|turso_connect_autocommit|run_turso_transaction" alphavault alphavault_reflex startup_healthcheck.py` 只剩迁移脚本或历史测试允许的结果
- [ ] `uv run pytest ...` 全量数据库相关测试通过
- [ ] `uv run pre-commit run -a` 通过
- [ ] `DB_TEST_CMD='uv run pytest tests/test_postgres_db.py::test_postgres_fixture_can_connect -v' mise run test:db-hook` 可跑通
- [ ] `migrate_turso_to_postgres.py` 能输出各 schema 的迁移条数
- [ ] worker 和 Reflex 在 `POSTGRES_DSN` 下能正常启动

## 执行备注

- 先做 Task 1 到 Task 4，先把底层和 schema 跑通。
- Task 5 和 Task 6 是真正的大头，必须小步提交。
- Task 8 里删 `libsql` 依赖前，先确认迁移脚本是否还要保留在仓库里；如果要保留，`libsql` 只能延后删除或不删。
