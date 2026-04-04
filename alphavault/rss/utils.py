"""RSS parsing + small helpers."""

from __future__ import annotations

import hashlib
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from urllib.parse import urlparse

import feedparser
import requests

from alphavault.constants import DATETIME_FMT
from alphavault.timeutil import CST, format_cst_datetime, now_cst_str
from alphavault.text.html import html_to_text

# NOTE: This module is extracted from the old local-sqlite ingest scripts.
# It keeps only RSS parsing + small helpers, so the worker can delete the old route.

BASE62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE62_INDEX = {ch: idx for idx, ch in enumerate(BASE62_ALPHABET)}
XUEQIU_CONTEXT_SPLIT_RE = re.compile(r"\s+---\s+")


def env_bool(name: str) -> Optional[bool]:
    value = os.getenv(name)
    if value is None:
        return None
    v = str(value).strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return None


def env_int(name: str) -> Optional[int]:
    value = os.getenv(name)
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None


def env_float(name: str) -> Optional[float]:
    value = os.getenv(name)
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        return float(v)
    except Exception:
        return None


class RateLimiter:
    def __init__(self, rpm: float) -> None:
        self.min_interval = 60.0 / rpm if rpm and rpm > 0 else 0.0
        self._lock = threading.Lock()
        self._next_ts = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.time()
            if now < self._next_ts:
                time.sleep(self._next_ts - now)
                now = time.time()
            self._next_ts = max(self._next_ts, now) + self.min_interval


def now_str() -> str:
    return now_cst_str()


def fetch_feed(
    url: str, timeout: float, *, retries: int = 2
) -> feedparser.FeedParserDict:
    headers = {
        "User-Agent": "AlphaVault-RSS-Ingest/1.0",
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
    }
    last_err: Optional[BaseException] = None
    total_attempts = max(1, int(retries) + 1)
    for attempt in range(total_attempts):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return feedparser.parse(resp.content)
        except Exception as e:
            last_err = e
            if attempt >= total_attempts - 1:
                break
            time.sleep(float(attempt + 1))
    if last_err is not None:
        raise last_err
    raise RuntimeError("rss_fetch_failed")


def get_entry_content(entry: feedparser.FeedParserDict) -> str:
    if "content" in entry and entry.content:
        for item in entry.content:
            if isinstance(item, dict) and item.get("value"):
                return item["value"]
            if hasattr(item, "value") and item.value:
                return item.value
    for key in ("summary", "description", "title"):
        val = entry.get(key)
        if val:
            return val
    return ""


def parse_datetime(entry: feedparser.FeedParserDict) -> str:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        parsed = getattr(entry, key, None)
        if parsed:
            year, month, day, hour, minute, second = parsed[:6]
            dt = datetime(
                year,
                month,
                day,
                hour,
                minute,
                second,
                tzinfo=timezone.utc,
            ).astimezone(CST)
            return format_cst_datetime(dt)
    return now_str()


def extract_numeric_id(value: str) -> str:
    if not value:
        return ""
    value = str(value)
    if value.isdigit():
        return value
    match = re.search(r"(?:mid|id)=([0-9]{6,})", value)
    if match:
        return match.group(1)
    match = re.search(r"/detail/([0-9]{6,})", value)
    if match:
        return match.group(1)
    return ""


def _extract_last_numeric(value: str, *, min_len: int = 6) -> str:
    text = str(value or "")
    if not text:
        return ""
    matches = re.findall(rf"(\d{{{min_len},}})", text)
    if not matches:
        return ""
    return matches[-1]


def extract_bid(link: str, user_id: Optional[str]) -> str:
    if not link:
        return ""
    if user_id:
        match = re.search(rf"/{re.escape(user_id)}/([A-Za-z0-9]+)", link)
        if match:
            return match.group(1)
    path = urlparse(link).path.strip("/")
    if not path:
        return ""
    seg = path.split("/")[-1]
    if user_id and seg == user_id:
        return ""
    return seg


def base62_to_int(value: str) -> int:
    num = 0
    for ch in value:
        if ch not in BASE62_INDEX:
            raise ValueError(f"invalid_base62_char:{ch}")
        num = num * 62 + BASE62_INDEX[ch]
    return num


def bid_to_mid(bid: str) -> str:
    if not bid:
        return ""
    result = ""
    i = len(bid)
    while i > 0:
        start = max(0, i - 4)
        part = bid[start:i]
        num = base62_to_int(part)
        part_str = str(num)
        if start > 0:
            part_str = part_str.zfill(7)
        result = part_str + result
        i = start
    return result


def _build_weibo_ids(
    entry: feedparser.FeedParserDict, link: str, user_id: Optional[str]
) -> tuple[str, str, str]:
    candidates: list[str] = []
    for key in ("id", "guid"):
        val = entry.get(key)
        if val:
            candidates.append(str(val))
    if link:
        candidates.append(link)
    for val in candidates:
        mid = extract_numeric_id(val)
        if mid:
            return mid, f"weibo:{mid}", ""
    bid = extract_bid(link, user_id)
    if bid:
        try:
            mid = bid_to_mid(bid)
        except ValueError:
            mid = ""
        if mid:
            return mid, f"weibo:{mid}", bid
        return bid, f"weibo:bid:{bid}", bid
    if link:
        digest = hashlib.sha1(link.encode("utf-8")).hexdigest()[:20]
        return f"linkhash:{digest}", f"weibo:linkhash:{digest}", ""
    return "", "", ""


def _build_xueqiu_ids(
    entry: feedparser.FeedParserDict, link: str
) -> tuple[str, str, str]:
    guid = entry.get("guid") or entry.get("id") or ""
    guid = str(guid or "").strip()
    if guid:
        post_uid = guid if guid.lower().startswith("xueqiu:") else f"xueqiu:{guid}"
        return guid, post_uid, ""
    link_num = _extract_last_numeric(link)
    if link_num:
        return link_num, f"xueqiu:{link_num}", ""
    if link:
        digest = hashlib.sha1(link.encode("utf-8")).hexdigest()[:20]
        return f"linkhash:{digest}", f"xueqiu:linkhash:{digest}", ""
    return "", "", ""


def build_ids(
    entry: feedparser.FeedParserDict,
    link: str,
    user_id: Optional[str],
    *,
    platform: str = "weibo",
) -> tuple[str, str, str]:
    if str(platform or "").lower() == "xueqiu":
        return _build_xueqiu_ids(entry, link)
    return _build_weibo_ids(entry, link, user_id)


def split_xueqiu_context_segments(text: str) -> list[str]:
    value = html_to_text(str(text or ""))
    if not value.strip():
        return []
    return [seg.strip() for seg in XUEQIU_CONTEXT_SPLIT_RE.split(value) if seg.strip()]


def _extract_author_from_title(title: str) -> str:
    text = html_to_text(str(title or "")).strip()
    if not text:
        return ""
    for sep in ("：", ":"):
        if sep not in text:
            continue
        candidate = text.split(sep, 1)[0].strip()
        if 0 < len(candidate) <= 30:
            return candidate
    return ""


def choose_author(
    entry: feedparser.FeedParserDict,
    feed: feedparser.FeedParserDict,
    fallback: str,
    *,
    platform: str = "weibo",
) -> str:
    author = entry.get("author")
    author = (author or "").strip()
    if author:
        return author
    if str(platform or "").lower() == "xueqiu":
        from_title = _extract_author_from_title(entry.get("title") or "")
        if from_title:
            return from_title
    author = feed.feed.get("author") or feed.feed.get("title")
    author = (author or "").strip()
    if author:
        return author
    return fallback


def split_commentary_and_quoted(raw_text: str) -> tuple[str, str]:
    text = (raw_text or "").strip()
    marker = "//@"
    idx = text.find(marker)
    if idx < 0:
        return text, ""
    commentary = text[:idx].strip()
    quoted = text[idx:].strip()
    return commentary, quoted


def build_analysis_context(raw_text: str) -> Dict[str, str]:
    commentary_text, quoted_text = split_commentary_and_quoted(raw_text)
    return {"commentary_text": commentary_text, "quoted_text": quoted_text}


def infer_user_id_from_rss_url(rss_url: str) -> Optional[str]:
    if not rss_url:
        return None
    match = re.search(r"/weibo/user/([0-9]{4,})", rss_url)
    if match:
        return match.group(1)
    match = re.search(r"/user/([0-9]{4,})", rss_url)
    if match:
        return match.group(1)
    match = re.search(r"/u/([0-9]{4,})", rss_url)
    if match:
        return match.group(1)
    return None


def parse_active_hours(value: str) -> Optional[tuple[int, int]]:
    v = str(value or "").strip()
    if not v:
        return None
    match = re.fullmatch(r"(\d{1,2})\s*-\s*(\d{1,2})", v)
    if not match:
        raise ValueError("invalid_active_hours_format")
    start = int(match.group(1))
    end = int(match.group(2))
    if start < 0 or start > 23 or end < 0 or end > 23:
        raise ValueError("invalid_active_hours_range")
    return start, end


def in_active_hours(now_dt: datetime, active_hours: tuple[int, int]) -> bool:
    start, end = active_hours
    hour = now_dt.hour
    if start <= end:
        return start <= hour <= end
    return hour >= start or hour <= end


def sleep_until_active(active_hours: tuple[int, int], *, verbose: bool) -> None:
    now_dt = datetime.now(CST)
    if in_active_hours(now_dt, active_hours):
        return
    start_hour, end_hour = active_hours
    today_start = now_dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)

    if start_hour <= end_hour:
        if now_dt.hour < start_hour:
            next_dt = today_start
        else:
            next_dt = today_start + timedelta(days=1)
    else:
        next_dt = today_start

    sleep_sec = max(1.0, (next_dt - now_dt).total_seconds())
    if verbose:
        print(
            f"[schedule] inactive now={now_dt.strftime(DATETIME_FMT)} "
            f"active={start_hour}-{end_hour} sleep={int(sleep_sec)}s",
            flush=True,
        )
    time.sleep(sleep_sec)


def build_row_meta(
    *,
    mid_or_bid: str,
    bid: str,
    link: str,
    title: str,
    author: str,
    created_at: str,
    raw_text: str,
) -> Dict[str, str]:
    row: Dict[str, str] = {
        "link": link,
        "title": title,
        "author": author,
        "published_at": created_at,
        "正文": raw_text,
    }
    if mid_or_bid.isdigit():
        row["id"] = mid_or_bid
    if bid:
        row["bid"] = bid
    return row


__all__ = [
    "build_analysis_context",
    "build_row_meta",
]
