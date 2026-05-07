# agent_tools

给 AI 和开发者看的最小工具契约。实现细节直接看代码。

## `stock_tools.py`

### `ai_resolve_stock(query: str, *, limit: int = 5)`

- 作用：把自然语言股票输入解析成唯一股票或候选列表。
- 关键返回：
  - `resolved_stock_key`
  - `has_unique_match`
  - `candidates`
  - `error`

### `ai_get_stock_page(stock: str, *, signal_page: int = 1, signal_page_size: int = 10, author: str = "", related_filter: str = "all", view_scope: str = "security")`

- 作用：读取个股页结果。
- 默认行为：`view_scope="security"` 只查单票。
- 公司聚合：`view_scope="company"` 聚合同公司 A/H。
- 名字输入：单票模式下，名字必须先经过 `ai_resolve_stock` 唯一化；公司聚合模式允许同名 A/H 直接进入公司视图。
- 关键返回：
  - `requested_stock`
  - `resolved_stock_key`
  - `view_scope`
  - `covered_stock_keys`
  - `page_title`
  - `signal_total`
  - `signals`
  - `same_company_stocks`
  - `load_error`

## `post_tools.py`

### `ai_search_posts(query: str, *, limit: int = 10, cursor: str = "")`

- 作用：按关键字搜索帖子。
- 关键返回：
  - `rows`
  - `next_cursor`
  - `has_more`
  - `error`

### `ai_get_post_detail(post_uid: str)`

- 作用：读取单条帖子详情和对话流。
- 关键返回：
  - `post_uid`
  - `raw_text`
  - `tree_text`
  - `message`
  - `load_error`
