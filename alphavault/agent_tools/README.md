# agent_tools

给 AI 调用的工具层。

### `stock_tools.py`

- `ai_resolve_stock(query: str, *, limit: int = 5)`
  - 作用：把自然语言股票输入整理成适合 AI 后续继续调用的候选结果。
  - 关键返回：
    - `query`
    - `resolved_stock_key`
    - `has_unique_match`
    - `candidates`
    - `error`
  - `candidates` 行字段：
    - `stock_key`
    - `label`
    - `subtitle`
    - `match_reason`
    - `is_exact`

- `ai_get_stock_page(stock: str, *, signal_page: int = 1, signal_page_size: int = 10, author: str = "", related_filter: str = "all")`
  - 作用：读取 AI 可直接消费的个股页结果；如果 `stock` 还不能唯一映射，直接返回 `load_error`。
  - 关键返回：
    - `requested_stock`
    - `resolved_stock_key`
    - `page_title`
    - `signal_total`
    - `signal_page`
    - `signal_page_size`
    - `signals`
    - `related_sectors`
    - `same_company_stocks`
    - `load_error`
  - `signals` 行字段：
    - `post_uid`
    - `title`
    - `preview`
    - `author`
    - `created_at`
    - `url`
    - `action`
    - `signal_badge`
    - `match_kind`

### `post_tools.py`

- `ai_search_posts(query: str, *, limit: int = 10, cursor: str = "")`
  - 作用：给 AI 提供分页后的帖子搜索结果。
  - 关键返回：
    - `rows`
    - `next_cursor`
    - `has_more`
    - `error`
  - `rows` 行字段：
    - `post_uid`
    - `source`
    - `source_label`
    - `author`
    - `created_at`
    - `url`
    - `title`
    - `preview`
    - `match_reason`

- `ai_get_post_detail(post_uid: str)`
  - 作用：补充单条帖子的原文和对话流。
  - 关键返回：
    - `post_uid`
    - `raw_text`
    - `tree_text`
    - `message`
    - `load_error`
