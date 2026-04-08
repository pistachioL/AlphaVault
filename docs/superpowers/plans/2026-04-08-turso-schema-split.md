# Turso Schema Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把云端 schema 从“1 份总 SQL”改成“source / standard 2 份职责 SQL”，同时保留 `apply_cloud_schema(...)` 作为唯一底层装库入口。

**Architecture:** 新增 `source_schema.sql` 和 `standard_schema.sql` 两份真实 schema，`alphavault.db.cloud_schema` 只做读取、拼装和执行，不在 Python 里保留任何第二份表定义。测试和文档同步切到“按目标装库”的口径，`target="all"` 只给测试或临时空库使用。

**Tech Stack:** Python, libsql/Turso, SQLite DDL, pytest

---

### Task 1: 先把 schema 边界用测试锁死

**Files:**
- Modify: `tests/test_cloud_schema.py`
- Check: `alphavault/db/sql/cloud_schema.sql`
- Check: `alphavault/db/cloud_schema.py`

- [ ] **Step 1: 写 source / standard / all 三种装库口径测试**

补这些断言：

- `apply_cloud_schema(conn, target="source")` 只装 source 表
- `apply_cloud_schema(conn, target="standard")` 只装 standard 表
- `apply_cloud_schema(conn, target="all")` 同时装两边
- `load_cloud_schema_sql(target=...)` 或等价加载入口能读到对应 SQL

- [ ] **Step 2: 先跑测试，确认现在会失败**

Run: `uv run pytest tests/test_cloud_schema.py -k 'source or standard or all' -v`

Expected:

- 因为当前还没有目标化 schema loader，所以至少有 1 个新测试失败

- [ ] **Step 3: 只补最小测试辅助，不先动业务代码**

要求：

- 不新增第二份 Python DDL
- 只改测试和必要的 helper 断言结构

- [ ] **Step 4: 跑一遍 `tests/test_cloud_schema.py`，确认失败形态对**

Run: `uv run pytest tests/test_cloud_schema.py -v`

Expected:

- 新测试失败
- 老测试如果依赖“总 SQL 全表”，也会暴露出需要改的地方

- [ ] **Step 5: Commit**

```bash
git add tests/test_cloud_schema.py
git commit -m "test: lock turso schema split targets"
```

### Task 2: 实现 2 份 SQL 和目标化 loader

**Files:**
- Create: `alphavault/db/sql/source_schema.sql`
- Create: `alphavault/db/sql/standard_schema.sql`
- Modify: `alphavault/db/cloud_schema.py`
- Delete: `alphavault/db/sql/cloud_schema.sql`
- Test: `tests/test_cloud_schema.py`

- [ ] **Step 1: 新建 source schema SQL**

放入这些表：

- `posts`
- `assertions`
- `assertion_mentions`
- `assertion_entities`
- `topic_clusters`
- `topic_cluster_topics`
- `topic_cluster_post_overrides`
- `entity_page_snapshot`
- `projection_dirty`
- `worker_cursor`
- `worker_locks`

- [ ] **Step 2: 新建 standard schema SQL**

放入这些表：

- `security_master`
- `relations`
- `relation_candidates`
- `alias_resolve_tasks`
- `homework_trade_feed`
- `follow_pages`

- [ ] **Step 3: 改 `alphavault.db.cloud_schema` 读取入口**

最小实现要求：

- `load_cloud_schema_sql(target=\"source\" | \"standard\" | \"all\")`
- `apply_cloud_schema(..., target=\"all\")`
- `target=\"all\"` 只是把两份 SQL 按固定顺序拼起来执行
- 不在 Python 里复制 `CREATE TABLE`

- [ ] **Step 4: 跑目标测试，确认从红变绿**

Run: `uv run pytest tests/test_cloud_schema.py -v`

Expected:

- source / standard / all 相关断言通过

- [ ] **Step 5: 再跑一轮受影响核心测试**

Run:

- `uv run pytest tests/test_research_workbench.py tests/test_alias_resolve_tasks.py tests/test_worker_job_state.py tests/test_research_stock_cache.py tests/test_homework_trade_feed.py -v`

Expected:

- standard 侧和 source 侧的代表性表读写都通过

- [ ] **Step 6: Commit**

```bash
git add alphavault/db/sql/source_schema.sql alphavault/db/sql/standard_schema.sql alphavault/db/cloud_schema.py tests/test_cloud_schema.py
git commit -m "refactor: split turso schema by db role"
```

### Task 3: 文档和仓库规则切到新口径

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `Plan.md`

- [ ] **Step 1: 改 README 装库说明**

要求：

- `weibo / xueqiu` 改成执行 `source_schema.sql`
- `standard` 改成执行 `standard_schema.sql`
- 不再继续写 `cloud_schema.sql`

- [ ] **Step 2: 改仓库规则文字**

要求：

- 把“只认 1 份总 SQL”改成“按库角色只认 2 份 SQL”
- 继续保留“Python 里只允许一个最底层装库入口”的限制

- [ ] **Step 3: 把最终决策回写 `Plan.md`**

要写清：

- 为什么不是 3 份
- 哪些表属于 source
- 哪些表属于 standard
- `target=\"all\"` 只给测试用

- [ ] **Step 4: 跑最小验证**

Run:

- `python -m compileall alphavault/db/cloud_schema.py`
- `uv run pytest tests/test_cloud_schema.py tests/test_research_workbench.py tests/test_worker_job_state.py -v`

Expected:

- 文档更新后，代码和测试都和新口径一致

- [ ] **Step 5: Commit**

```bash
git add README.md AGENTS.md Plan.md alphavault/db/cloud_schema.py tests/test_cloud_schema.py
git commit -m "docs: document source and standard turso schemas"
```

### Task 4: 验收和人工检查

**Files:**
- Check: `alphavault/db/sql/source_schema.sql`
- Check: `alphavault/db/sql/standard_schema.sql`
- Check: `README.md`
- Check: `AGENTS.md`

- [ ] **Step 1: 跑最终回归**

Run:

- `uv run pytest tests/test_cloud_schema.py tests/test_research_workbench.py tests/test_alias_resolve_tasks.py tests/test_worker_job_state.py tests/test_research_stock_cache.py tests/test_homework_trade_feed.py -v`
- `python -m compileall alphavault/db/cloud_schema.py`

- [ ] **Step 2: 人工核对边界**

检查：

- source SQL 里没有 `security_master / relations / relation_candidates / alias_resolve_tasks / homework_trade_feed / follow_pages`
- standard SQL 里没有 `posts / assertions / assertion_mentions / assertion_entities / projection_dirty / worker_cursor / worker_locks`

- [ ] **Step 3: 人工核对 README**

检查：

- source 库和 standard 库的装库命令不再混写

- [ ] **Step 4: Commit**

```bash
git add README.md AGENTS.md Plan.md alphavault/db/sql/source_schema.sql alphavault/db/sql/standard_schema.sql alphavault/db/cloud_schema.py tests/test_cloud_schema.py
git commit -m "chore: finalize turso schema split"
```
