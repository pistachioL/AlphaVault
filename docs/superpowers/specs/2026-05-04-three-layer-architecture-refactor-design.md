# 三层架构全量重构设计

## 结论

本轮重构采用三层结构：

- 通用层：唯一业务真相，负责领域对象、规则、规范化、对象归并、信号聚合
- 前端层：只负责页面视图模型和交互状态，不直接处理原始数据库行和 AI 输出
- AI 层：只负责 prompt 输入包、模型输出包和 AI 专用校验，不直接处理页面字段和数据库列格式

本轮直接全量替换，不保留向后兼容分支，不长期保留旧 helper，不允许两套契约并存。

## 背景

当前代码已经出现边界混杂问题：

- `alphavault/ai/analyze.py` 同时承担 prompt、模型输出清洗、数据库写入字段格式映射
- `alphavault_reflex/services/research_data.py` 同时承担对象聚合和前端展示字段映射
- `alphavault_reflex/homework_state.py` 直接改写断言行，状态层承担了业务归并职责
- `alphavault/agent_tools/stock_tools.py` 单独裁了一套给 AI/代理消费的结构，说明 AI 专用输出层已经真实存在
- 多条调用链共享 `dict[str, object]` 和隐式字段约定，字段来源和职责边界不清晰

这类结构会让以下改动彼此牵连：

- 改领域规则时影响前端页面
- 改前端字段时影响 AI 层
- 改 AI 输出格式时影响数据库写入和页面拼装

## 目标

- 建立一套稳定的通用领域模型，作为业务唯一真相
- 让前端层和 AI 层只依赖通用层，不互相依赖
- 让数据库原始行和 JSON 列格式只停留在边界适配器
- 清理当前研究页、交易流、整理页、代理工具中的隐式字段共享
- 让后续改规则、改页面、改 prompt 时各自只改一层

## 非目标

- 不顺手重构与三层边界无关的页面布局和样式
- 不补历史兼容路径
- 不额外扩展新业务能力

## 架构设计

### 1. 通用层

通用层放在 `alphavault/domains` 下，承担唯一业务真相。

建议新增或收拢以下模型与服务：

- `alphavault/domains/content/models.py`
  - `Post`
  - `TopicThread`
- `alphavault/domains/signal/models.py`
  - `Assertion`
  - `Signal`
  - `SignalGroup`
- `alphavault/domains/stock/models.py`
  - `StockObject`
  - `StockAliasCandidate`
- `alphavault/domains/ai_draft/models.py`
  - `AssertionDraft`
  - `AnalyzeDecision`

建议新增或收拢以下规则与转换器：

- `alphavault/domains/content/row_mapper.py`
  - 数据库 `posts` 原始行转 `Post`
- `alphavault/domains/signal/row_mapper.py`
  - 数据库 `assertions` 原始行转 `Assertion`
- `alphavault/domains/signal/aggregator.py`
  - `Assertion + Post -> Signal`
  - 负责补齐作者、时间、正文、树结构标签等通用聚合字段
- `alphavault/domains/stock/object_index.py`
  - 保留为对象归并核心
- `alphavault/domains/stock/service.py`
  - 个股对象解析、个股聚合、搜索对象行生成
- `alphavault/domains/thread_tree/service.py`
  - 保留消息树相关领域能力

通用层规则：

- 原始 `dict` 只允许在 repo / loader 边界存在
- 进入通用层后全部改为显式模型
- `Signal` 是研究页、交易流、代理工具共享的统一中间对象
- 个股别名归并、对象标题、搜索文本、板块关联统计都由通用层负责
- 前端层和 AI 层都不能直接读 `stock_codes_json` 这类数据库列格式

### 2. 前端层

前端层放在 `alphavault_reflex/view_models` 和 `alphavault_reflex/services` 下，只消费通用层模型。

建议新增以下前端视图模型：

- `alphavault_reflex/view_models/research_page.py`
  - `StockResearchPageView`
  - `SectorResearchPageView`
  - `SignalRowView`
  - `RelatedSectorRowView`
- `alphavault_reflex/view_models/homework_board.py`
  - `HomeworkBoardView`
  - `HomeworkBoardRowView`
- `alphavault_reflex/view_models/organizer.py`
  - `OrganizerPendingRowView`
  - `OrganizerSummaryView`

建议调整以下文件职责：

- `alphavault_reflex/services/research_data.py`
  - 改成前端映射器
  - 输入通用层对象
  - 输出研究页视图模型
- `alphavault_reflex/services/research_page_loader.py`
  - 只负责加载数据和串联服务
- `alphavault_reflex/homework_state.py`
  - 移除对象归并和断言改写逻辑
  - 只保留筛选、分页、跳转、加载状态
- `alphavault_reflex/organizer_state.py`
  - 只保留页面状态和用户动作

前端层规则：

- `State` 不直接操作原始断言行
- `State` 不直接调用个股归并规则
- 页面服务只消费通用层模型并输出页面视图模型
- 页面组件只消费视图模型字段

### 3. AI 层

AI 层放在 `alphavault/ai` 下，只负责 AI 输入输出契约和 AI 专用校验。

建议新增以下模块：

- `alphavault/ai/contracts.py`
  - `AiTopicPackage`
  - `AiAnalyzeInput`
  - `AiAnalyzeOutput`
  - `AiAssertionDraft`
- `alphavault/ai/prompt_builders.py`
  - 从 `AiTopicPackage` 生成 prompt 文本
- `alphavault/ai/result_mapper.py`
  - 模型输出转 `AiAnalyzeOutput`
  - `AiAnalyzeOutput` 转通用层 `AssertionDraft`
- `alphavault/ai/validators.py`
  - AI 输出字段校验

建议调整以下文件职责：

- `alphavault/ai/analyze.py`
  - 缩成编排入口
  - 不再直接返回数据库写入格式
- `alphavault/ai/topic_prompt_v4.py`
  - 只负责模板和文本拼装
- `alphavault/weibo/topic_prompt_tree.py`
  - 输出 `AiTopicPackage`
  - 不再透传松散 `dict`

AI 层规则：

- prompt 输入包必须是显式契约
- 模型输出包必须先转 AI 层结构，再转通用层草稿
- 通用层草稿再交给写库适配器转数据库列格式
- AI 层不直接感知前端字段
- AI 层不直接感知数据库列名和 JSON 存储细节

## 数据流

### 研究页 / 交易流

1. loader / capability 从数据库取原始行
2. 原始行转 `Post`、`Assertion`
3. 通用层完成个股对象归并、断言过滤、信号聚合
4. 前端层把 `Signal`、`StockObject` 等通用对象映射成页面视图模型
5. `State` 和页面组件只消费页面视图模型

### AI 分析链路

1. 上下文构建器从帖子和消息树生成通用层对象
2. AI 层把通用层对象映射成 `AiTopicPackage` 或 `AiAnalyzeInput`
3. prompt builder 生成 prompt
4. 模型输出先转 `AiAnalyzeOutput`
5. AI mapper 把输出转成通用层 `AssertionDraft`
6. 写库适配器把草稿转成数据库写入格式

## 目录与文件调整

### 保留并收拢职责

- `alphavault/domains/stock/object_index.py`
- `alphavault/domains/thread_tree/service.py`
- `alphavault/ai/topic_prompt_v4.py`

### 重点重写

- `alphavault/ai/analyze.py`
- `alphavault/weibo/topic_prompt_tree.py`
- `alphavault_reflex/services/research_data.py`
- `alphavault_reflex/homework_state.py`
- `alphavault_reflex/organizer_state.py`
- `alphavault/agent_tools/stock_tools.py`

### 重点新增

- `alphavault/domains/content/models.py`
- `alphavault/domains/content/row_mapper.py`
- `alphavault/domains/signal/models.py`
- `alphavault/domains/signal/row_mapper.py`
- `alphavault/domains/signal/aggregator.py`
- `alphavault/domains/stock/service.py`
- `alphavault/domains/ai_draft/models.py`
- `alphavault/ai/contracts.py`
- `alphavault/ai/prompt_builders.py`
- `alphavault/ai/result_mapper.py`
- `alphavault/ai/validators.py`
- `alphavault_reflex/view_models/research_page.py`
- `alphavault_reflex/view_models/homework_board.py`
- `alphavault_reflex/view_models/organizer.py`

## 迁移顺序

### 阶段 1：先立通用层

- 新增 `Post`、`Assertion`、`Signal`、`StockObject`、`AssertionDraft` 等模型
- 新增数据库原始行到通用模型的 mapper
- 把当前散落在 `research_signal_view.py`、`research_data.py`、`homework_state.py` 里的聚合逻辑收回通用层

### 阶段 2：改 AI 层

- 新增 AI 输入输出契约
- `topic_prompt_tree` 输出 `AiTopicPackage`
- `analyze.py` 只做编排，不再产出数据库写入格式
- 新增 AI 输出到通用草稿的 mapper

### 阶段 3：改研究页和个股页

- 页面服务从“读原始行再拼页面”改成“读通用对象再映射视图”
- `research_data.py` 只保留前端视图映射
- `research_page_loader.py` 只保留加载与串联

### 阶段 4：改交易流和整理页

- `homework_state.py` 去掉对象归并和断言改写
- `organizer_state.py` 去掉业务归类和行裁剪
- 页面状态层只保留交互状态

### 阶段 5：改代理工具和外围能力

- `agent_tools` 改成消费通用对象或 AI 专用输出
- 收口 `capabilities` 与旧 helper 的松散结构

### 阶段 6：删旧入口

- 删除被新模型替代的 `dict` helper
- 删除中途过渡函数
- 删除前端状态层里的业务规则残留
- 删除 AI 层里直接面向数据库列格式的清洗逻辑

## 删除规则

本轮直接全改，删除策略如下：

- 新入口接管后，旧 helper 同步删除
- 不保留双写、双读、桥接适配层
- 不保留兼容旧字段的分支
- 不保留只为迁移服务的中间常量和包装函数

## 风险与处理

### 风险 1：改动面大

处理方式：

- 严格按阶段推进
- 每阶段完成后立即替换所有消费方
- 不允许一半新模型一半旧 `dict` 长期并存

### 风险 2：前端层和 AI 层继续偷用通用层内部字段

处理方式：

- 用显式模型卡住输入输出
- 页面视图模型和 AI 契约只暴露本层需要的字段

### 风险 3：删旧不彻底导致边界回退

处理方式：

- 每阶段结束都带删除清单
- 最终以导入引用为准，收口旧函数和旧文件职责

## 验收标准

- 研究页、交易流、整理页、代理工具都不再直接消费原始断言 `dict`
- AI 输入输出都走显式契约
- 数据库列格式只停留在 loader / repo / write adapter 边界
- 通用层成为唯一业务真相
- 旧 helper 和兼容分支完成删除
