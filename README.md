# AlphaVault

面向投资大佬观点追踪的采集与分析系统。
当前支持：微博 RSS 抓取、LLM 投资相关性分析、断言抽取、写入 Turso 云端库、Reflex 研究台。

## 功能概览
- RSS 增量抓取（去重）
- LLM 分析与观点抽取（写入 `posts` + `assertions`）
- Turso 云端数据库作为队列 + 归档（幂等 upsert）
- 可选 Redis：Turso 临时挂了时先排队（避免丢）

## 目录结构（核心脚本）
- `weibo_rss_turso_worker.py`：Worker（RSS → spool → Turso → AI → Turso）
- `alphavault/db/turso_queue.py`：Turso 队列字段与读写
- `alphavault/db/turso_db.py`：Turso engine + 基础表（posts/assertions）
- `alphavault_reflex/`：Reflex 前端（交易流、个股页、板块页、整理中心）
- `streamlit_app.py`：旧入口壳（保留总览、风险、日志；研究入口已跳到 Reflex）

## 环境要求
- Python 3.10+
- uv（用来装依赖/启动）
- 可用的 LLM 代理或 API

## 安装
```bash
uv sync
```

如果你要跑旧版 Streamlit 入口，再额外装可选依赖：
```bash
uv sync --group streamlit
```

## 开发（pre-commit + tests）
```bash
# 1) 安装 hooks（提交前自动检查）
uv run pre-commit install

# 2) 手动跑一遍检查
uv run pre-commit run -a

# 3) 跑 tests
uv run pytest
```

## Worker（RSS → Turso → AI）
推荐用环境变量配 AI（Docker/supervisord 也用这一套）：
```bash
# 只跑微博：填 WEIBO_ 这一组
export WEIBO_RSS_URLS="https://rsshub.xxx/weibo/user/3962719063?key=YOUR_KEY,https://rsshub.xxx/weibo/user/123?key=YOUR_KEY"
export RSS_TIMEOUT_SECONDS="60"
export RSS_RETRIES="5"

export WEIBO_TURSO_DATABASE_URL="libsql://xxx.turso.io"
export WEIBO_TURSO_AUTH_TOKEN="YOUR_TOKEN"

export AI_MODEL="openai/gpt-5.2"
# 注意：AI_MODEL 要是“真实模型名”，不要写成 rss 这类占位词
export AI_BASE_URL="http://localhost:3001/proxy/gpt5-2/v1"
# 注意：AI_BASE_URL 要指向 OpenAI 兼容接口（一般以 /v1 结尾），不要填网关首页/网页（会返回 HTML）
export AI_API_KEY="YOUR_KEY"
export AI_API_MODE="responses"
export AI_STREAM="1"
export AI_TIMEOUT_SEC="1000"
export AI_REASONING_EFFORT="xhigh"
export AI_RPM="12"
export AI_RETRIES="11"
export AI_MAX_INFLIGHT="30"
export AI_TRACE_OUT="trace.txt"

uv run python weibo_rss_turso_worker.py --verbose
```

说明：
- `WEIBO_RSS_URLS` 支持逗号/换行分隔；也可以用 `WEIBO_RSS_URL`（只传 1 个）。
- 你也可以同时填 `XUEQIU_RSS_URLS` + `XUEQIU_TURSO_DATABASE_URL`，worker 会同时跑两套（weibo + xueqiu）。
- RSS 抓取网络参数：`RSS_TIMEOUT_SECONDS`（默认 60 秒）和 `RSS_RETRIES`（默认失败后再试 5 次）。
- `WEIBO_AUTHOR/WEIBO_USER_ID`、`XUEQIU_AUTHOR/XUEQIU_USER_ID` 都是可选的：为空时会尽量从 RSS/URL 自动推断。
- Worker 会先写本地 `spool` 文件；Turso 写失败时会保留 `spool`，并且（可选）推到 Redis。
- Reflex / Streamlit 只展示 `processed_at IS NOT NULL` 的帖子（避免 “pending 占位” 被当成 irrelevant）。

## 手动触发 RSS 抓取 API
先设置鉴权 key：
```bash
export RSS_MANUAL_TRIGGER_KEY="YOUR_TRIGGER_KEY"
```

接口：
- `GET /api/rss/trigger?key=YOUR_TRIGGER_KEY`
- 成功返回：`200`，包含 `inserted_total`、`turso_error`、`sources`
- key 错误返回：`401`

示例：
```bash
curl "http://127.0.0.1:8080/api/rss/trigger?key=YOUR_TRIGGER_KEY"
```

## Reflex 前端
```bash
uv run reflex run
```

需要：`WEIBO_TURSO_DATABASE_URL` 或 `XUEQIU_TURSO_DATABASE_URL`（token 可选）。

主要页面：
- `/`：首页 + 全局搜索
- `/homework`：交易流
- `/research/stocks/[stock_slug]`：个股研究页
- `/research/sectors/[sector_slug]`：板块研究页
- `/organizer`：整理中心

## AI 标签不准时，当前怎么处理

### 背景故事
一开始，系统更像“先抽标签，再按标签看内容”。

这条路有一个老问题：真实世界里，别人说股票不会老老实实只用一个名字。

同一只票，可能会同时出现：
- 代码：`601899.SH`
- 全名：`紫金矿业`
- 简称：`紫金`
- 黑话 / 圈内叫法：比如别的票会有人叫 `长电`（指 `长江电力`）

而早期 `AI` 抽取，本来就是按“单条内容”在工作。
它看到一条微博，可能写 `stock:601899.SH`；
再看到另一条，可能写 `stock:紫金矿业`；
再看到下一条，可能只敢写 `stock:紫金`。

如果前端直接按一条 `topic_key` 精确去查，就会出现一个假象：
看起来像是“系统只找到 1 条正文”，
其实不是库里没有，
而是同一只票被拆成了几块。

以前在旧 `Streamlit` 里，遇到这种情况，经常靠“关键字搜索”硬补。
这能救急，但有两个问题：
- 关键字命中不等于真的在讲这只票
- 人每次都要自己猜关键字，流程很绕

现在 Reflex 这套处理思路，目标不是“让 AI 一次就永远抽对”，
而是承认 `AI` 会不准，然后给系统补一层“纠错”和“回补”。

### 现在的总体思路
现在不是只信一条 `topic_key`，而是分 3 层看：

1. 原始层：保留 `posts` 和 `assertions`
2. 对象层：把同一只票的不同叫法尽量并到一个 `stock object`
3. 回补层：如果 `AI` 漏抽了，再把文章找回来，定向重跑

### 第一层：先把最硬的东西并起来
最稳的，是代码和全名。

例如一条断言里如果同时有：
- `stock_codes_json=["601899.SH"]`
- `stock_names_json=["紫金矿业"]`

系统会先把：
- `stock:601899.SH`
- `stock:紫金矿业`

放进同一个个股对象。

如果你在整理中心人工确认过：
- `stock:601899.SH alias_of stock:紫金矿业`

这种正式关系也会直接进入对象层，后面全局生效。

### 第二层：简称和黑话，不再靠字符串硬猜
现在已经去掉了：
- `紫金 -> 紫金矿业`

这类“只因为字符串前面长得像，所以直接自动并”的逻辑。

原因很简单：
- `紫金 -> 紫金矿业` 也许还凑合
- 但 `长电 -> 长江电力` 这种，字符串本身并不可靠

所以现在对简称、黑话、圈内叫法，改成这个原则：

- 没有正式 `alias_of` 之前，不自动并
- 先当成“未确认别名”
- 再走 `AI alias map`

`AI alias map` 的做法是：
- 先给 AI 一小批候选正式个股对象
- 再把作者重合、板块重合、最近摘要这些证据一起给 AI
- AI 只能从候选里选，不能自己编新票
- 没把握就返回空

也就是说，AI 现在负责“在候选里判断”，
而不是“拍脑袋创造关系”。

### 第三层：个股页、搜索、交易流都按对象走
对象层立起来以后，前端的主要入口都不再直接吃碎的 `topic_key`。

现在：
- 个股页会先把当前别名解析到 canonical 个股对象，再取数
- 搜索会先按个股对象去重，再跳到 canonical 个股页
- 交易流里同一只票的碎标签，也会先并成一个对象再展示和跳转

这样做的目的，就是不让用户在页面里看到：
- `stock:601899.SH`
- `stock:紫金矿业`
- `stock:紫金`

像 3 只不同的票。

### AI 漏抽时，怎么补
除了“同票被拆碎”，另一种常见问题是：
正文其实在讲这只票，
但 `AI` 当时没有抽出这只票的标签，
于是这篇文章就从个股页里“消失”了。

以前常靠关键字搜索补。
现在这一步被正式做成了“待回补文章”。

个股页会扫描 `posts`：
- 找出正文里明显提到这只票
- 但还没有进入这只票 `assertions` 的文章

这些文章不会直接塞进主信号区，
而是先放到：
- `待回补文章`

这样可以先看，再决定要不要补。

### 现在可以页内直接做定向 AI 回补
`待回补文章` 里点 `立即 AI 回补` 后，
系统不会去跑全量泛化抽取，
而是只做一个很窄的任务：

- 判断这篇文章是不是在讲“当前这只票”
- 如果是，只抽这只票相关的 `trade.*` 断言

这个定向 prompt 的范围很小，
比全量抽取更稳，也更容易补漏。

补出来的新断言不会粗暴覆盖整篇旧断言。
现在的写法是：
- 先读这篇文章已有断言
- 再把新断言去重后合并进去
- 最后写回 `assertions`

这样可以避免“为了补 1 条，把旧的 3 条全擦掉”。

### 现在这套逻辑，最适合解决什么问题
当前最适合处理这 3 类问题：

- 同一只票被写成代码 / 全名 / 简称，页面被拆碎
- 简称和黑话不能只靠字符串猜，需要 AI 在候选里做判断
- 正文明明在讲这只票，但 AI 当时漏抽，需要页内定向回补

### 现在还没完全做完的边界
这套补救逻辑，现在重点先落在 `个股`。

还没有完全铺开的部分：
- `板块` 还没做到同样强的对象层
- `板块` 的定向 AI 回补还没像个股这样完整

所以目前最稳的组合是：
- `代码 / 全名 / 已确认 alias_of`：强规则
- `简称 / 黑话`：AI 候选判断
- `AI 漏抽文章`：待回补文章 + 定向 AI 回补

## 旧 Streamlit 入口
```bash
uv run streamlit run streamlit_app.py
```

注意：`streamlit` 现在是可选依赖组，先执行一次：
```bash
uv sync --group streamlit
```

保留原因：
- 总览
- 风险雷达
- 主题时间线
- 学习库 / 日志 / 数据表

已经迁走的研究入口：
- `交易流` -> `Reflex /homework`
- `关注页` -> `Reflex /organizer`
- `主题聚合` -> `Reflex /organizer`

## 线上 Docker（推荐）

容器默认用 `supervisord.conf` 常驻跑 2 件事：
- Web：Reflex
- Worker：`weibo_rss_turso_worker.py`

启动时会先做一次 startup check（失败就直接退出容器）：
- 本地缓存：`SPOOL_DIR`（默认 `/tmp/alphavault-spool`）需要可写
- Turso：必须配置 `WEIBO_TURSO_DATABASE_URL` 或 `XUEQIU_TURSO_DATABASE_URL`，并且能连（healthcheck 只读）
- Redis：只有配置了 `REDIS_URL` 才检查；没配就跳过

定时（通过 env 配）：
- Worker（全流程：AI/flush/RSS）
  - 默认：全天；AI 空了就补单；每 10 分钟做一次维护（不用配）
  - 可选：`WORKER_CRON="*/10 6-22 * * *"` 表示只在 6-22 点做事
- RSS（只管抓取，不影响 AI/flush）
  - 可选：`RSS_CRON="*/15 6-22 * * *"` 表示只在 6-22 点抓 RSS，每 15 分钟尝试一次

```bash
docker build -t alphavault .

docker run -d --name alphavault \
	  -p 8080:8080 \
	  -e WORKER_CRON="*/10 * * * *" \
	  -e WEIBO_RSS_URLS="https://rsshub.xxx/weibo/user/3962719063?key=YOUR_KEY" \
	  -e RSS_CRON="*/15 6-22 * * *" \
	  -e RSS_TIMEOUT_SECONDS="60" \
	  -e RSS_RETRIES="5" \
	  -e AI_MODEL="openai/gpt-5.2" \
	  -e AI_API_MODE="responses" \
	  -e AI_STREAM="1" \
	  -e AI_TIMEOUT_SEC="1000" \
	  -e AI_REASONING_EFFORT="xhigh" \
	  -e AI_RPM="12" \
	  -e AI_RETRIES="11" \
	  -e AI_MAX_INFLIGHT="30" \
	  -e AI_TRACE_OUT="/data/trace.txt" \
	  -e AI_BASE_URL="http://xxx/v1" \
	  -e AI_API_KEY="YOUR_KEY" \
	  -e WEIBO_TURSO_DATABASE_URL="libsql://xxx.turso.io" \
	  -e WEIBO_TURSO_AUTH_TOKEN="YOUR_TOKEN" \
	  -e REDIS_URL="redis://:pass@host:6379/0" \
	  -e SPOOL_DIR="/tmp/alphavault-spool" \
	  alphavault
```

也可以用 `docker-compose.yml`：
- `cp .env.example .env` 然后填值
- `docker compose up -d --build`

`docker-compose.yml` 只是本地省事用的；像 Render 这类平台一般只认 `Dockerfile`，你直接在它的 UI 里填 env 就行。

## 常见问题
- `auth role not found`：Turso token 与 DB 不匹配，请重新生成 token 并核对 URL。
- Turso 连不上：worker 不会退出，会先写 `spool`/Redis；恢复后会自动 flush + 重试 AI。
- Reflex / Streamlit 看不到数据：因为只展示 `processed_at IS NOT NULL`（AI 还没跑完就不会显示）。
- RSS 抓取正常但 LLM 不跑：确认 `AI_API_KEY` / `AI_MODEL` / `AI_BASE_URL` 配置正确。
