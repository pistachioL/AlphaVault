# Supabase Postgres Migration Design

**日期：** 2026-04-08

## 目标

- 去掉运行时对 `Turso/libsql` 的依赖。
- 把数据库统一切到 `Supabase Postgres`。
- 尽量不改上层业务流程，只收口数据库底层。
- 保持现在按角色分开的数据边界：
  - `weibo`
  - `xueqiu`
  - `standard`

## 明确不做

- 这一步不保留 `Turso` 兼容层。
- 这一步不做双写、双读、灰度切流。
- 这一步不把手写 SQL 一次性重写成 ORM。
- 这一步不顺手把 JSON 文本列改成 `JSONB`。
- 这一步不做隐藏的事务自动重放。
- 这一步不为了“以后可能换库”先造一套很大的通用数据库框架。

## 已确认约束

- 运行时只用 `1` 个 `POSTGRES_DSN`。
- 这个 `DSN` 指向同一个 `Supabase` 项目里的一个 `Postgres` 数据库。
- 库里保留 `3` 个 schema 常量：
  - `weibo`
  - `xueqiu`
  - `standard`
- 代码常量固定 schema 名，不先做环境变量。
- Python 连接层用 `psycopg` + `psycopg_pool`。
- 线上连接方式用 `Supabase` 的 session pooler。
- 测试不用 mock 数据库，改成起本地 `Postgres` 真库。
- 正式切换方式是停机一次后全量迁移，不做平滑迁移。

## 方案比较

### 方案 A：只换底层驱动，保留现有上层结构

做法：

- 继续保留现在“手写 SQL + 小 helper”的结构。
- 把 `TursoEngine`、`TursoConnection`、`turso_connect_autocommit()`、`run_turso_transaction()` 这套底层能力换成 `Postgres` 版本。
- 上层模块尽量不改业务逻辑，只改数据库入口和类型引用。

优点：

- 改动面最小。
- 最符合“上层应用尽量不变”的目标。
- 不会为了抽象而抽象。

缺点：

- 仍然要逐个清掉当前代码里的 `Turso` 直接依赖。
- 仍然要逐个检查 SQL 在 `Postgres` 下的兼容性。

结论：

- 推荐。

### 方案 B：先做一层完整数据库抽象，再接 `Postgres`

做法：

- 新造 `DatabaseEngine`、`DatabaseConnection`、`Repository` 之类的大接口。
- 再把 `Postgres` 挂到这套接口后面。

优点：

- 结构看起来更整齐。
- 如果以后真要再换库，会更顺。

缺点：

- 现在已经明确不保留 `Turso`，这层抽象短期没有直接收益。
- 很容易为了未来假设做过度设计。
- 第一阶段工作量会明显变大。

结论：

- 不推荐。

### 方案 C：顺手切到 ORM

做法：

- 引入 `SQLAlchemy` 或别的 ORM，把现在的 SQL 和事务整体改写。

优点：

- 长远上统一度更高。

缺点：

- 这已经不是单纯切库，是大重构。
- 仓库当前明显是“手写 SQL + 小 helper”风格，硬切 ORM 风险太大。

结论：

- 不推荐。

## 推荐设计

### 1. 连接层

新增：

- `alphavault/db/postgres_db.py`
- `alphavault/db/postgres_env.py`

职责：

- `postgres_db.py` 只放最底层连接池、连接、事务和查询执行逻辑。
- `postgres_env.py` 只放环境变量读取逻辑。

连接层原则：

- 上层不要直接碰 `psycopg`。
- 上层继续只用项目自己的 helper。
- 一次 `with` 连接块里使用同一条连接。
- 一次事务块里使用同一条连接。
- 不保证“整个进程一直复用同一条真实连接”。

### 2. 参数风格

- 业务 SQL 继续保留现在的 `:name` 写法。
- 底层继续用 `sqlparams` 做参数转换。
- 只是把现在的 `named -> qmark`，改成 `named -> pyformat`。

这样可以保住现有大量 SQL，不需要为了参数风格整仓重写。

### 3. 事务边界

- 新底层只提供保守的事务 helper。
- 连接坏了就丢回池外。
- 事务失败就回滚并抛错。
- 第一版不做隐藏的自动事务重跑。

原因：

- 自动重放写事务容易把副作用做两次。
- 这一步目标是稳定切库，不是顺手加复杂重试系统。

### 4. schema 布局

- 只保留一个 `POSTGRES_DSN`。
- 在同一个数据库里保留 3 个 schema：
  - `weibo`
  - `xueqiu`
  - `standard`

设计原则：

- 不靠 `search_path` 在运行时来回切。
- 运行时 SQL 尽量直接带全名。
- 例如：
  - `weibo.posts`
  - `xueqiu.posts`
  - `standard.relations`

### 5. schema 真相文件

继续只保留两份 SQL 真相：

- `alphavault/db/sql/source_schema.sql`
- `alphavault/db/sql/standard_schema.sql`

处理方式：

- 两份文件都改成带 `{{schema_name}}` 占位的模板。
- `source_schema.sql` 执行两次：
  - 一次装到 `weibo`
  - 一次装到 `xueqiu`
- `standard_schema.sql` 执行一次：
  - 装到 `standard`

这样可以保住“source 只有一份真相”“standard 只有一份真相”，避免重复维护。

## 目录和文件落点

新增文件：

- `alphavault/db/postgres_db.py`
- `alphavault/db/postgres_env.py`
- 一次性迁移脚本

需要改的核心文件：

- `alphavault/db/cloud_schema.py`
- `alphavault/db/sql/source_schema.sql`
- `alphavault/db/sql/standard_schema.sql`
- `alphavault/db/turso_queue.py`
- `alphavault/research_workbench/service.py`
- `alphavault/research_workbench/schema.py`
- `alphavault/research_stock_cache.py`
- `alphavault/homework_trade_feed.py`
- `alphavault/worker/` 下所有直接依赖 `TursoEngine`、`TursoConnection` 的文件
- `alphavault_reflex/services/` 下所有直接开 `Turso` 连接的文件
- 数据库相关测试

旧文件处理方式：

- 第一阶段先切调用点，不立刻删旧 `turso_*` 文件。
- 确认没有运行时引用后，再统一删除。

## 需要改的 SQL 范围

一定要改：

- `sqlite_schema` / `sqlite_master` 相关查询。
- `PRAGMA` 相关逻辑。
- `Turso/libsql` 专用的连接错误处理、dispose、重试逻辑。
- 所有直接把类型写成 `TursoEngine` / `TursoConnection` 的地方。

大概率可以继续沿用，只是底层驱动换掉：

- `:post_uid`、`:limit` 这种命名参数。
- `ON CONFLICT ... DO UPDATE`。
- `CHECK`、`UNIQUE`。
- `LIMIT :limit`。
- `substr(...)` 这种基础 SQL 函数调用。

## 配置设计

第一版只留最少配置：

- `POSTGRES_DSN`
- `POSTGRES_POOL_MAX_SIZE`

代码常量：

- `SCHEMA_WEIBO = "weibo"`
- `SCHEMA_XUEQIU = "xueqiu"`
- `SCHEMA_STANDARD = "standard"`

第一版不先加：

- 多套 DSN
- search_path 配置
- 事务自动重试配置
- 复杂连接池生命周期参数

## 迁移路径

1. 在同一个 `Supabase Postgres` 数据库里建 3 个 schema：
   - `weibo`
   - `xueqiu`
   - `standard`
2. 把 `source_schema.sql` 模板执行到 `weibo` 和 `xueqiu`。
3. 把 `standard_schema.sql` 模板执行到 `standard`。
4. 写一个一次性迁移脚本，从旧 `Turso` 读数据，再写入 `Postgres`。
5. 迁移顺序先 source，再 standard。
6. 正式切换前先停掉会写库的进程。
7. 迁移完成后先做关键表对数，再切运行时配置。

## 测试和验证

测试方式：

- 数据库相关测试改成起本地 `Postgres` 真库。
- 上层测试也直接连本地 `Postgres`，不再 mock 数据库。
- 保留少量底层冒烟测试，验证连接、装库、读写、事务和关键查询。

验收标准：

- 代码运行时不再依赖 `Turso/libsql`。
- worker 能正常写 `posts`、`assertions`。
- Reflex 能正常读 source 和 standard 数据。
- 本地 `Postgres` 测试可跑。
- 迁移后关键表行数和旧库对得上。

## 风险和限制

- 这次不是驱动替换那么简单，当前代码里有很多地方直接依赖 `Turso` 类型和行为，必须逐步清掉。
- 同一个数据库里虽然只有一个 DSN，但如果运行时不带 schema 全名，就有串到错误 schema 的风险。
- 旧测试大量基于 `libsql/SQLite :memory:`，迁到 `Postgres` 后不能再把这套测试当成数据库真实行为。
- 如果第一阶段顺手把 JSON 文本列、ORM、缓存策略一起改，范围会明显失控。

## 推荐结论

- 直接放弃 `Turso` 支持，只做 `Supabase Postgres`。
- 用一个很薄的 `Postgres` 底层替掉现在的 `Turso` 底层。
- 保住上层最常见的使用手感，避免大面积改业务代码。
- 用 `1` 个 `POSTGRES_DSN` + `3` 个固定 schema 常量收口。
- schema 仍然只保留 `source` 和 `standard` 两份真相文件，通过 `{{schema_name}}` 模板执行到目标 schema。
