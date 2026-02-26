import time
from datetime import datetime

import feedparser
import requests

from db import init_db, get_session, Influencer, Post, SuningTime


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

SUNING_API_URL = "https://f.m.suning.com/api/ct.do"


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
            feed = feedparser.parse(inf.rss_url)
            for entry in feed.entries:
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


def fetch_suning_once():
    """调用苏宁接口一次并写入 sqlite"""
    try:
        resp = requests.get(SUNING_API_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print("[crawler] suning api error: %s" % e)
        return

    with get_session() as session:
        row = SuningTime(
            api=data.get("api"),
            code=str(data.get("code")) if data.get("code") is not None else None,
            current_time=data.get("currentTime"),
            msg=data.get("msg"),
        )
        session.add(row)


def main_loop(interval_seconds=1800):
    """循环定时抓取"""
    init_db()
    ensure_influencers()
    while True:
        try:
            print("[crawler] fetching at %s..." % datetime.utcnow().isoformat())
            fetch_once()
            fetch_suning_once()
            print("[crawler] done.")
        except Exception as e:
            print("[crawler] error: %s" % e)
        time.sleep(interval_seconds)


if __name__ == "__main__":
    # 半小时抓一次，可以自己调整
    main_loop(1800)

