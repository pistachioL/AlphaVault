from __future__ import annotations

from typing import Final

from alphavault.original_link import build_original_app_deep_link
from alphavault.original_link import resolve_original_link_platform

MOBILE_USER_AGENT_PATTERN: Final[str] = r"android|iphone|ipad|ipod|mobile"
ORIGINAL_LINK_CLASS_NAME: Final[str] = "av-original-link"
ORIGINAL_LINK_APP_ICON: Final[str] = "📱"
ORIGINAL_LINK_APP_ICON_CLASS_NAME: Final[str] = "av-original-link-app-icon"
ORIGINAL_LINK_SCRIPT_PATH: Final[str] = "/original_link.js"
ORIGINAL_LINK_URL_ATTR: Final[str] = "data-av-url"
ORIGINAL_LINK_POST_UID_ATTR: Final[str] = "data-av-post-uid"
PHONE_DEVICE_MAX_DIMENSION_PX: Final[int] = 1024

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
