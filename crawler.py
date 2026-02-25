import time
from datetime import datetime

import feedparser

from db import init_db, get_session, Influencer, Post


# 简单关键词配置，用于打标签
STOCK_KEYWORDS = ["电力", "银行", "券商", "黄金", "煤炭", "消费", "医药"]
TOPIC_KEYWORDS = {
    "大盘": ["大盘", "指数", "A股", "沪深", "估值", "高估", "低估", "风险"],
    "电力": ["电力", "公用事业"],
    "银行": ["银行"],
    "黄金": ["黄金", "金价"],
}

# TODO: 将这里替换成你实际关注的大佬的 RSS 地址
INFLUENCERS_CONFIG = [
    {
        "name": "大佬A",
        "platform": "某平台",
        "rss_url": "https://example.com/feedA.xml",
    },
    {
        "name": "大佬B",
        "platform": "某平台",
        "rss_url": "https://example.com/feedB.xml",
    },
]

# 是否使用本地模拟的 RSS 数据（True 时不真正访问网络）
USE_MOCK_DATA = True

# 本地模拟的 RSS 数据，按 rss_url 分组
MOCK_RSS_FEEDS = {
    "https://example.com/feedA.xml": [
        {
            "title": "电力板块估值修复机会",
            "summary": "近期电力板块调整后性价比提升，关注水电与火电龙头。",
            "link": "https://mock.example.com/a1",
            "published": datetime(2026, 2, 10, 9, 30),
        },
        {
            "title": "大盘震荡中的防御策略",
            "summary": "指数短期波动加大，更看重现金流稳定、分红率较高的标的。",
            "link": "https://mock.example.com/a2",
            "published": datetime(2026, 2, 15, 14, 0),
        },
        {
            "title": "银行板块估值与风险溢价",
            "summary": "银行板块当前 PB 处于历史低位，关注不良率与息差变化。",
            "link": "https://mock.example.com/a3",
            "published": datetime(2026, 2, 20, 10, 15),
        },
    ],
    "https://example.com/feedB.xml": [
        {
            "title": "黄金与实际利率的再平衡",
            "summary": "黄金价格与实际利率相关性再次强化，可作为组合对冲工具。",
            "link": "https://mock.example.com/b1",
            "published": datetime(2026, 2, 12, 11, 45),
        },
        {
            "title": "电力与公用事业的长期逻辑",
            "summary": "公用事业在高波动环境下具有防御属性，关注电力龙头资产注入预期。",
            "link": "https://mock.example.com/b2",
            "published": datetime(2026, 2, 22, 16, 30),
        },
    ],
}


def ensure_influencers():
    """写入默认的大佬配置到数据库（如不存在）"""
    with get_session() as session:
        for info in INFLUENCERS_CONFIG:
            existing = (
                session.query(Influencer)
                .filter(Influencer.rss_url == info["rss_url"])
                .one_or_none()
            )
            if existing is None:
                session.add(
                    Influencer(
                        name=info["name"],
                        platform=info.get("platform"),
                        rss_url=info["rss_url"],
                    )
                )


def extract_tags(text):
    """基于关键词的非常简单的标签提取"""
    text = text or ""
    stock_tags = set()
    topic_tags = set()

    for kw in STOCK_KEYWORDS:
        if kw in text:
            stock_tags.add(kw)

    for topic, kws in TOPIC_KEYWORDS.items():
        if any(k in text for k in kws):
            topic_tags.add(topic)

    return ",".join(sorted(stock_tags)), ",".join(sorted(topic_tags))


def fetch_once():
    """抓取所有已配置 RSS 一次"""
    with get_session() as session:
        influencers = session.query(Influencer).all()
        for inf in influencers:
            if USE_MOCK_DATA:
                entries = MOCK_RSS_FEEDS.get(inf.rss_url, [])
            else:
                feed = feedparser.parse(inf.rss_url)
                entries = feed.entries

            for entry in entries:
                link = entry.get("link")
                if not link:
                    continue

                # 去重
                exists = session.query(Post).filter(Post.link == link).one_or_none()
                if exists:
                    continue

                title = entry.get("title", "")
                summary = entry.get("summary", "")
                content = summary  # demo 里先用 summary，当正文用

                if USE_MOCK_DATA:
                    published = entry.get("published")
                else:
                    published = None
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        published = datetime(
                            entry.published_parsed.tm_year,
                            entry.published_parsed.tm_mon,
                            entry.published_parsed.tm_mday,
                            entry.published_parsed.tm_hour,
                            entry.published_parsed.tm_min,
                            entry.published_parsed.tm_sec,
                        )

                stock_tags, topic_tags = extract_tags(
                    "%s\n%s\n%s" % (title, summary, content)
                )

                post = Post(
                    influencer_id=inf.id,
                    title=title,
                    summary=summary,
                    content=content,
                    link=link,
                    published_at=published,
                    stock_tags=stock_tags or None,
                    topic_tags=topic_tags or None,
                )
                session.add(post)


def main_loop(interval_seconds=1800):
    """循环定时抓取"""
    init_db()
    ensure_influencers()
    while True:
        try:
            print("[crawler] fetching at %s..." % datetime.utcnow().isoformat())
            fetch_once()
            print("[crawler] done.")
        except Exception as e:
            print("[crawler] error: %s" % e)
        time.sleep(interval_seconds)


if __name__ == "__main__":
    # 半小时抓一次，可以自己调整
    main_loop(1800)

