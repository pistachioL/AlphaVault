"""
Thread tree helpers (facade).

Note: legacy `alphavault.ui.*` has been retired (no compatibility shim).
"""

from __future__ import annotations

from alphavault.domains.thread_tree.build import build_weibo_thread_forest
from alphavault.domains.thread_tree.parse import (
    CSV_RAW_FIELDS_MARKER,
    DISPLAY_MD_SPLIT_RE,
    FORWARD_ORIGINAL_MARKER,
    MATCH_KEY_LEN,
    REPOST_TOKEN,
    SYNTHETIC_SOURCE_ID_PREFIX,
    extract_parent_post_id,
    extract_platform_post_id,
    parse_display_md_segments,
    parse_weibo_csv_raw_fields,
    strip_csv_raw_fields,
)
from alphavault.domains.thread_tree.render import VIRTUAL_NODE_LABEL

__all__ = [
    "CSV_RAW_FIELDS_MARKER",
    "DISPLAY_MD_SPLIT_RE",
    "FORWARD_ORIGINAL_MARKER",
    "MATCH_KEY_LEN",
    "REPOST_TOKEN",
    "SYNTHETIC_SOURCE_ID_PREFIX",
    "VIRTUAL_NODE_LABEL",
    "build_weibo_thread_forest",
    "extract_parent_post_id",
    "extract_platform_post_id",
    "parse_display_md_segments",
    "parse_weibo_csv_raw_fields",
    "strip_csv_raw_fields",
]
