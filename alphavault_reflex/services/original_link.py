from __future__ import annotations

from typing import Final
from urllib.parse import urlparse

from alphavault.constants import PLATFORM_WEIBO, PLATFORM_XUEQIU
from alphavault.db.postgres_env import infer_platform_from_post_uid
from alphavault.domains.thread_tree.api import extract_platform_post_id

APP_LINK_SUPPORTED_PLATFORMS: Final[frozenset[str]] = frozenset(
    {PLATFORM_WEIBO, PLATFORM_XUEQIU}
)
MOBILE_USER_AGENT_PATTERN: Final[str] = r"android|iphone|ipad|ipod|mobile"
ORIGINAL_LINK_CLASS_NAME: Final[str] = "av-original-link"
ORIGINAL_LINK_APP_ICON: Final[str] = "📱"
ORIGINAL_LINK_APP_ICON_CLASS_NAME: Final[str] = "av-original-link-app-icon"
ORIGINAL_LINK_SCRIPT_PATH: Final[str] = "/original_link.js"
ORIGINAL_LINK_URL_ATTR: Final[str] = "data-av-url"
ORIGINAL_LINK_POST_UID_ATTR: Final[str] = "data-av-post-uid"
PHONE_DEVICE_MAX_DIMENSION_PX: Final[int] = 1024
WEIBO_HOST_SUFFIXES: Final[frozenset[str]] = frozenset({"weibo.com", "weibo.cn"})
WEIBO_DETAIL_PATH_HEADS: Final[frozenset[str]] = frozenset({"status", "detail"})
XUEQIU_HOST_SUFFIX: Final[str] = "xueqiu.com"
XUEQIU_HTML_SUFFIX: Final[str] = ".html"
XUEQIU_STOCK_PATH_HEAD: Final[str] = "S"


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clean_hostname(value: object) -> str:
    return _clean_text(value).lower()


def _matches_host_suffix(hostname: object, suffix: str) -> bool:
    host = _clean_hostname(hostname)
    return bool(host) and (host == suffix or host.endswith(f".{suffix}"))


def _parse_url(value: object):
    text = _clean_text(value)
    if not text:
        return None
    try:
        parsed = urlparse(text)
    except ValueError:
        return None
    if not parsed.scheme or not parsed.netloc:
        return None
    return parsed


def _path_segments(value: object) -> list[str]:
    parsed = _parse_url(value)
    if parsed is None:
        return []
    return [segment.strip() for segment in parsed.path.split("/") if segment.strip()]


def _is_weibo_host(hostname: object) -> bool:
    return any(_matches_host_suffix(hostname, suffix) for suffix in WEIBO_HOST_SUFFIXES)


def _is_xueqiu_host(hostname: object) -> bool:
    return _matches_host_suffix(hostname, XUEQIU_HOST_SUFFIX)


def _resolve_weibo_post_id(url: object, post_uid: object) -> str:
    parsed = _parse_url(url)
    if parsed is not None and _is_weibo_host(parsed.hostname):
        segments = _path_segments(url)
        if len(segments) >= 2 and segments[0].lower() in WEIBO_DETAIL_PATH_HEADS:
            return segments[1]
        if len(segments) >= 2 and segments[0].isdigit():
            return segments[1]

    raw_post_uid = _clean_text(post_uid).lower()
    if ":linkhash:" in raw_post_uid:
        return ""

    post_id = _clean_text(extract_platform_post_id(post_uid))
    if post_id.startswith(("http://", "https://", "linkhash:")):
        return ""
    return post_id


def _resolve_xueqiu_user_post_path(segments: list[str]) -> str:
    if len(segments) < 2:
        return ""
    user_id = segments[0]
    post_id = segments[1].removesuffix(XUEQIU_HTML_SUFFIX)
    if not user_id or not post_id.isdigit():
        return ""
    return f"{user_id}/{post_id}"


def _resolve_xueqiu_stock_post_path(segments: list[str]) -> str:
    if len(segments) < 3:
        return ""
    stock_symbol = segments[1]
    post_id = segments[2].removesuffix(XUEQIU_HTML_SUFFIX)
    if not stock_symbol or not post_id.isdigit():
        return ""
    return f"{XUEQIU_STOCK_PATH_HEAD}/{stock_symbol}/{post_id}"


def _resolve_xueqiu_deep_link_path(url: object) -> str:
    parsed = _parse_url(url)
    if parsed is None or not _is_xueqiu_host(parsed.hostname):
        return ""

    segments = _path_segments(url)
    if not segments:
        return ""
    if segments[0].upper() == XUEQIU_STOCK_PATH_HEAD:
        return _resolve_xueqiu_stock_post_path(segments)
    return _resolve_xueqiu_user_post_path(segments)


def resolve_original_link_platform(url: object, post_uid: object) -> str:
    from_post_uid = infer_platform_from_post_uid(post_uid)
    if from_post_uid in APP_LINK_SUPPORTED_PLATFORMS:
        return from_post_uid

    parsed = _parse_url(url)
    if parsed is None:
        return ""

    if _is_weibo_host(parsed.hostname):
        return PLATFORM_WEIBO
    if _is_xueqiu_host(parsed.hostname):
        return PLATFORM_XUEQIU
    return ""


def build_original_app_deep_link(url: object, post_uid: object) -> str:
    platform = resolve_original_link_platform(url, post_uid)
    if platform == PLATFORM_WEIBO:
        post_id = _resolve_weibo_post_id(url, post_uid)
        return f"sinaweibo://detail?mblogid={post_id}" if post_id else ""

    if platform != PLATFORM_XUEQIU:
        return ""

    deep_link_path = _resolve_xueqiu_deep_link_path(url)
    return f"xueqiu://{deep_link_path}" if deep_link_path else ""


__all__ = [
    "MOBILE_USER_AGENT_PATTERN",
    "ORIGINAL_LINK_APP_ICON",
    "ORIGINAL_LINK_APP_ICON_CLASS_NAME",
    "ORIGINAL_LINK_CLASS_NAME",
    "ORIGINAL_LINK_POST_UID_ATTR",
    "ORIGINAL_LINK_SCRIPT_PATH",
    "ORIGINAL_LINK_URL_ATTR",
    "PHONE_DEVICE_MAX_DIMENSION_PX",
    "build_original_app_deep_link",
    "resolve_original_link_platform",
]
