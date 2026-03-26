# 设计：别名重试超限 → 网页手动处理 + 拉黑（2026-03-26）

## 一句话

对“个股别名（比如 `stock:紫金`）”做 **最大自动重试次数** 限制：超过次数就进网页列表，让人手动“确认归并”或“拉黑”。拉黑后 worker 永远不再自动处理它。

## 目标（Goals）

1. 避免别名一直 `resolved=0/inserted=0` 还反复跑，浪费 CPU + 打爆日志。
2. 给一个网页入口：把“AI 一直判断不出来”的别名交给人处理。
3. **拉黑** 后，worker 不再自动处理该别名（不再尝试 AI）。

## 不做（Non-goals）

1. 不做复杂的“解释为什么 AI 失败”的分析页。
2. 不做自动“猜测并强行归并”的规则（继续保持保守）。

## 配置

新增环境变量（默认 6 次）：

- `WORKER_STOCK_ALIAS_MAX_RETRIES=6`

含义：

- 每个 `alias_key` 最多自动尝试 6 次。
- 第 6 次还没成功，就进入 **manual（手动）** 列表。

## 数据（Turso）

新增表：`research_alias_resolve_tasks`

字段：

- `alias_key TEXT PRIMARY KEY`
- `status TEXT NOT NULL DEFAULT 'pending'`
  - 值：`pending | manual | blocked | resolved`
- `attempt_count INTEGER NOT NULL DEFAULT 0`
- `created_at TEXT NOT NULL`
- `updated_at TEXT NOT NULL`

索引：

- `idx_research_alias_resolve_tasks_status`：`(status, updated_at)`

说明：

- `attempt_count` 只统计“真的发起过 AI 判断”的次数。
- `blocked`：拉黑；worker 永远跳过。
- `manual`：超限；worker 跳过，等网页处理。
- `resolved`：已处理（通常会同时写入 `research_relations` 的 `alias_of`）。

## Worker 行为（别名同步）

位置：`alphavault/worker/stock_alias_sync.py`（alias 同步任务）

### 关键流程

1. 找出当前“未归并”的 `alias_key` 列表（来源还是 assertions 的 `topic_key`，规则保持不变）。
2. 过滤掉：
   - status 是 `blocked/manual/resolved` 的别名
   - `attempt_count >= WORKER_STOCK_ALIAS_MAX_RETRIES` 的别名（并把它标成 `manual`）
3. 对剩下的别名，取前 `ALIAS_SYNC_MAX_KEYS_PER_RUN` 个（默认 8）来做本轮 AI 判断：
   - 先把这批别名的 `attempt_count += 1`
   - 然后调用 AI 做判断
4. AI 有结果（`alias_key -> stock:xxxx.SH`）：
   - 写入 `research_relations`（`alias_of`）
   - 把该别名任务 `status=resolved`
5. AI 没结果：
   - 如果 `attempt_count >= max_retries`：把任务 `status=manual`
   - 否则维持 `pending`，下次再试

### 保证

- `blocked/manual/resolved` 的别名不会再进 AI。
- 超限会停在 `manual`，不会无限重试。

## 网页（Reflex：整理中心）

入口：`/organizer`（整理中心）

新增一个分区按钮：

- “别名超限”

展示内容：

- alias_key（比如 `stock:紫金`）
- attempt_count（比如 `6`）
- updated_at（可选）

交互：

1. 点“手动处理”打开对话框（dialog）
2. 对话框里输入“目标股票代码”（比如 `601899.SH` 或 `stock:601899.SH`）
3. 两个按钮：
   - “确认归并”：写入 `alias_of` + 任务标记 `resolved`
   - “拉黑”：任务标记 `blocked`

输入规则（简单且保守）：

- 只接受“股票代码”形式（`601899.SH` / `000001.SZ` / `stock:601899.SH`）。
- 不是代码就提示错误，不写入。

## 测试（Test）

1. 任务表的 SQL + CRUD：用 `libsql.connect(':memory:')` 写单测。
2. Worker 的“超限变 manual / blocked 跳过”逻辑：写单测（可用 in-memory DB）。
3. OrganizerState 的“拉黑/确认归并”事件：写单测（mock DB 写入或用 in-memory DB）。
