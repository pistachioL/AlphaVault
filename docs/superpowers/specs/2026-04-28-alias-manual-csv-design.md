# 未确认简称 CSV 导出回填设计

## 背景

当前整理中心里的“未确认简称”主要依赖网页逐条处理。待处理数量上来后，人工确认效率低，来回切换页面也不方便。现有页面已经具备人工判断所需的大部分上下文字段，包括：

- `alias_key`
- `sample_post_uid`
- `sample_post_url`
- `sample_evidence`
- `sample_raw_text_excerpt`
- 同博主历史命中上下文
- 当前 AI 结果字段

这说明更适合补一个“导出到表格，人工集中回填，再本地命令导入”的闭环，而不是继续把批量人工确认压在网页里完成。

## 目标

- 一次性导出全部待人工确认的简称记录。
- 导出的表格能直接被 Excel 打开使用。
- 表格里保留和网页一致的判断上下文，支持直接打开原文链接。
- 人工只回填一个字段：`stock_code`。
- 本地命令导入回填结果。
- `stock_code` 有值时确认别名关系；`stock_code` 为空时按忽略处理。
- 导入行为与现有网页“确认 / 忽略”语义保持一致。

## 非目标

- 本次不做网页上传导入。
- 本次不改 AI 提示词、批量大小或判定逻辑。
- 本次不新增第三种人工动作。
- 本次不引入额外的数据库表。
- 本次不把导出格式做成主要依赖 `.xlsx`。

## 方案选择

推荐方案是 `CSV + 本地命令导入`。

选择原因：

- 仓库已经有成熟的 `csv.DictWriter` / `csv.DictReader` 脚本模式。
- Excel 可以直接打开 `UTF-8 with BOM` 的 `CSV`。
- `CSV` 对导入协议最稳定，字段更透明，排错也最简单。
- 当前需求只有“导出全部 + 人工填一列 + 本地导回”，不需要额外引入更重的表格写入逻辑。

不采用 `.xlsx` 作为主格式的原因：

- 读写逻辑更重。
- 出错面更大。
- 当前需求没有依赖 `.xlsx` 特有能力。

## 总体流程

1. 运行导出命令。
2. 系统从 `standard.alias_resolve_tasks` 读取全部 `status='pending'` 的记录。
3. 系统补齐网页展示所需字段和历史上下文，写出 `CSV`。
4. 人工在 Excel 中打开文件，只填写 `stock_code` 一列。
5. 运行导入命令。
6. 系统逐行读取回填结果。
7. `stock_code` 有值时写入正式 `alias_of` 关系，并把任务标成 `resolved`。
8. `stock_code` 为空时把任务标成 `blocked`，与网页“忽略”保持一致。
9. 输出导入汇总，便于确认导入结果。

## 文件边界

### 共享模块

新增：

- `alphavault/research_workbench/alias_manual_csv.py`

职责：

- 定义导出列常量。
- 把待人工确认记录整理成导出行。
- 把历史命中上下文整理成适合单元格展示的文本。
- 解析回填 `CSV`。
- 校验导入所需字段。
- 提供导出与导入共用的轻量数据转换函数。

### 导出脚本

新增：

- `scripts/export_alias_manual_csv.py`

职责：

- 读取环境变量。
- 连接 research workbench 数据库。
- 拉取全部待人工确认项。
- 生成导出文件。
- 打印导出数量与输出路径。

### 导入脚本

新增：

- `scripts/import_alias_manual_csv.py`

职责：

- 读取环境变量。
- 解析人工回填后的 `CSV`。
- 按行执行确认或忽略。
- 打印成功、忽略、跳过、冲突、失败的汇总。

## 导出字段设计

导出文件使用 `UTF-8 with BOM` 编码，文件格式为 `CSV`。

导出列保持“人工判断优先”，字段如下：

- `alias_key`
- `sample_post_uid`
- `sample_post_url`
- `sample_evidence`
- `sample_raw_text_excerpt`
- `history_hits_count`
- `history_hits_text`
- `attempt_count`
- `updated_at`
- `ai_status`
- `ai_stock_code`
- `ai_official_name`
- `ai_confidence`
- `ai_reason`
- `ai_uncertain`
- `ai_validation_status`
- `stock_code`

说明：

- 前 16 列是只读上下文。
- `stock_code` 是唯一人工回填列，初始为空。
- `history_hits_text` 是把网页里的历史命中列表压成一个多行文本单元格，保留时间、`post_uid`、摘要文本。
- `sample_post_url` 保留可直接打开的网页链接，方便人工从 Excel 跳回原文。

## 导入协议

导入时只认两列真相：

- `alias_key`
- `stock_code`

其余列只作为辅助上下文，不参与写库判断。

规则如下：

- `stock_code` 有值：按确认处理。
- `stock_code` 为空：按忽略处理。

### 确认处理

确认时执行以下校验：

1. `alias_key` 非空。
2. `stock_code` 可规范化为标准股票代码。
3. 规范化后的目标 `stock_key` 存在于 `security_master`。

全部通过后：

- 写入 `stock_alias / alias_of` 正式关系。
- `left_key` 为目标 `stock_key`。
- `right_key` 为原始 `alias_key`。
- `source` 记为 `manual_csv`。
- 把 `alias_resolve_tasks.status` 更新为 `resolved`。

### 忽略处理

当 `stock_code` 为空时：

- 不写入任何别名关系。
- 直接把 `alias_resolve_tasks.status` 更新为 `blocked`。

这样与现有网页“忽略”动作完全一致。

## 幂等与冲突处理

导入脚本必须可重复执行，规则如下：

### 可直接跳过的情况

- 当前 `alias_key` 已经存在同样目标的正式 `alias_of` 关系。
- 当前任务状态已经是 `resolved`，且已确认到同一目标。
- 当前任务状态已经是 `blocked`，且这次回填仍然为空。

这些情况计入 `skipped_count`。

### 需要报冲突的情况

- 当前 `alias_key` 已经确认到了另一只股票。
- 当前 `alias_key` 已经不再是待处理状态，且本次回填意图和现有结果不一致。
- 同一份 `CSV` 里同一个 `alias_key` 出现多次且回填值不一致。

这些情况不静默覆盖，计入 `conflict_count` 并打印明细。

### 失败情况

- 缺少必需列。
- `stock_code` 非法。
- 目标代码在 `security_master` 中不存在。
- 单行写库失败。

这些情况计入 `error_count` 并打印明细。

## 命令行接口

### 导出命令

建议参数：

- `--output-path`

默认输出路径建议：

- `data/alias_manual_pending.csv`

命令形态：

```bash
uv run python scripts/export_alias_manual_csv.py --output-path data/alias_manual_pending.csv
```

输出内容：

- 导出总数
- 输出文件路径

### 导入命令

建议参数：

- `--csv-path`

命令形态：

```bash
uv run python scripts/import_alias_manual_csv.py --csv-path data/alias_manual_pending.csv
```

输出内容：

- 总行数
- 确认数
- 忽略数
- 跳过数
- 冲突数
- 失败数

## 与现有系统的一致性

为了保证导入结果和网页手工操作完全一致，本方案沿用现有页面语义：

- 网页“确认”：
  - 写 `alias_of`
  - 任务状态改为 `resolved`
- 网页“忽略”：
  - 任务状态改为 `blocked`

CSV 导入只是在命令行批量复用这套语义，不引入新的人工状态定义。

## 风险与取舍

### 风险 1：空 `stock_code` 被误填成“忘记处理”

这是当前方案的主要取舍。因为协议已经约定“空值等于忽略”，导入前需要人工确认这份表已经处理完毕。

控制方式：

- 导出文件名与导入脚本说明都明确写出“空值表示忽略”。
- 导入汇总里打印忽略数量。
- 冲突与失败行单独打印，减少误判。

### 风险 2：历史命中内容过长

如果把全部历史上下文原样摊平，单元格会很长。

控制方式：

- `history_hits_text` 只保留必要字段。
- 每条命中保留时间、`post_uid`、摘要文本。
- 不额外引入复杂多表结构。

### 风险 3：导出后数据发生变化

导出文件和导入时间可能有间隔，期间任务状态可能被网页或脚本改掉。

控制方式：

- 导入时做当前状态校验。
- 不一致时计入冲突，不静默覆盖。

## 验证方式

实现完成后至少验证以下场景：

1. 成功导出全部待人工确认项。
2. 导出文件能被 Excel 正常打开，中文不乱码，链接可点击。
3. 回填合法 `stock_code` 后能成功写入正式关系。
4. 空 `stock_code` 会把任务标成 `blocked`。
5. 重复导入同一文件时结果幂等。
6. 非法 `stock_code` 会被拦住并输出错误。
7. 已被其他流程处理过的行会进入跳过或冲突，而不是被静默覆盖。

## 推荐实现顺序

1. 新增 `alias_manual_csv.py`，先固化导出列和导入解析。
2. 完成导出脚本。
3. 完成导入脚本。
4. 用少量样本手工导出、修改、导入做闭环验证。
5. 确认无误后再批量使用。
