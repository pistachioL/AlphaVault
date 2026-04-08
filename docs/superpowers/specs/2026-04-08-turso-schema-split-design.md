# Turso Schema Split Design

**日期：** 2026-04-08

## 目标

- 把现在“运行时已经分成 `weibo / xueqiu / standard` 3 个库，但 schema 还只放在 1 份总 SQL 里”的状态收清楚。
- 让人一眼就知道：
  - source 库该装什么表
  - standard 库该装什么表
  - 哪些代码该连 source
  - 哪些代码该连 standard

## 明确不做

- 这一步不改业务表结构。
- 这一步不顺手做旧库迁移。
- 这一步不把 `weibo` 和 `xueqiu` 再拆成两份不同 schema。
- 这一步不新增运行时自动建表。

## 现状问题

- 代码入口已经分库：
  - `weibo / xueqiu` 走 source env
  - `standard` 走 `STANDARD_*`
- 但表结构还塞在同一个 `alphavault/db/sql/cloud_schema.sql`。
- 结果是：
  - 运维看不出哪个库该装哪些表
  - 文档会把 source 表和 standard 表混在一起
  - 以后改表时，很容易继续把“source 表”和“standard 表”写到一个大锅里

## 方案比较

### 方案 A：继续保留 1 份总 SQL

优点：

- 改动最少

缺点：

- 运行时边界已经分了，schema 还不分，概念继续打架
- 人工装库和排错都不直观

结论：

- 不推荐

### 方案 B：拆成 3 份

- `weibo_schema.sql`
- `xueqiu_schema.sql`
- `standard_schema.sql`

优点：

- 名字上和数据库个数完全对齐

缺点：

- `weibo` 和 `xueqiu` 现在本质上是同一套 source schema
- 两份文件大概率长期一样，后面改一份漏一份的风险很高

结论：

- 不推荐

### 方案 C：按职责拆成 2 份

- `source_schema.sql`
- `standard_schema.sql`

优点：

- 和现在代码真实边界一致
- 不会制造 `weibo.sql / xueqiu.sql` 双份重复真相
- 以后扩展 source 平台时，还能继续复用同一份 source schema

缺点：

- 需要把“总 SQL 为唯一真相”的仓库规则改成“按库角色分 2 份真相”

结论：

- 推荐

## 推荐设计

### 1. SQL 文件拆分

新增：

- `alphavault/db/sql/source_schema.sql`
- `alphavault/db/sql/standard_schema.sql`

处理：

- 删除 `alphavault/db/sql/cloud_schema.sql`
- 不保留第三份“拼起来的总 SQL”，避免又变成多份真相

### 2. Python 装库入口保持只有 1 个

保留唯一底层装库入口：

- `alphavault.db.cloud_schema.apply_cloud_schema(...)`

但它要改成按目标装：

- `target="source"`
- `target="standard"`
- `target="all"` 只给测试或本地临时空库用

实现方式：

- `apply_cloud_schema(...)` 只负责读取对应 SQL 文件并执行
- 不能在 Python 里再写第二份 `CREATE TABLE`

### 3. 表归属

#### source schema

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

说明：

- 这些表都和 source 库里的帖子、断言、投影缓存、worker 进度强绑定。
- `weibo` 和 `xueqiu` 共用这套 schema。

#### standard schema

- `security_master`
- `relations`
- `relation_candidates`
- `alias_resolve_tasks`
- `homework_trade_feed`
- `follow_pages`

说明：

- 这些表都属于标准整理库或前端标准视图状态。

### 4. 运行时边界

- source worker 继续只连 source 库
- research workbench / organizer / relation actions / homework feed 继续只连 standard 库
- 不新增跨库自动建表

### 5. 文档口径

README 和仓库规则都改成白话：

- `weibo / xueqiu` 库先执行 `source_schema.sql`
- `standard` 库先执行 `standard_schema.sql`
- 只有测试或本地临时空库才允许一次装 `all`

## 验证方式

- `tests/test_cloud_schema.py`
  - 验证 `target="source"` 只装 source 表
  - 验证 `target="standard"` 只装 standard 表
  - 验证 `target="all"` 会同时装两边
- 相关功能测试继续验证：
  - source worker 相关逻辑仍可用
  - standard research workbench 相关逻辑仍可用
- 文档和 healthcheck 文案同步更新

## 风险和限制

- 这一步会改掉仓库里现有“总 SQL 唯一真相”的规则文本，影响面不只代码，还包括文档和认知习惯。
- 有些旧测试现在直接默认“一个空库装完后什么表都有”，这批测试需要按目标拆开看。
- `target="all"` 只能作为测试便利口，不应该再拿去指导生产装库。
