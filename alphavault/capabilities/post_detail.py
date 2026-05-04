from __future__ import annotations

import importlib
from functools import cache
from types import ModuleType
from typing import TypedDict

from alphavault.domains.thread_tree.service import (
    normalize_tree_lookup_post_uid,
    slice_posts_for_single_post_tree,
)

TREE_MESSAGE_EMPTY = "没有对话流。"
TREE_MESSAGE_LOAD_ERROR_PREFIX = "加载失败："


class PostDetailResult(TypedDict):
    post_uid: str
    raw_text: str
    tree_text: str
    message: str
    load_error: str


@cache
def _load_homework_board_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.homework_board")


@cache
def _load_source_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.source_read")


def get_post_detail(post_uid: str) -> PostDetailResult:
    uid = normalize_tree_lookup_post_uid(post_uid)
    if not uid:
        return {
            "post_uid": "",
            "raw_text": "",
            "tree_text": "",
            "message": TREE_MESSAGE_EMPTY,
            "load_error": "",
        }

    posts, err = _load_source_read_module().load_single_post_for_tree_from_env(uid)
    if err:
        return {
            "post_uid": uid,
            "raw_text": "",
            "tree_text": "",
            "message": f"{TREE_MESSAGE_LOAD_ERROR_PREFIX}{err}",
            "load_error": err,
        }

    if not posts:
        return {
            "post_uid": uid,
            "raw_text": "",
            "tree_text": "",
            "message": TREE_MESSAGE_EMPTY,
            "load_error": "",
        }

    matched_post = next(
        (
            dict(row)
            for row in posts
            if normalize_tree_lookup_post_uid(row.get("post_uid")) == uid
        ),
        dict(posts[0]),
    )
    raw_text = str(matched_post.get("raw_text") or "").strip()
    posts_view = slice_posts_for_single_post_tree(post_uid=uid, posts=posts)
    _label, tree_text = (
        _load_homework_board_module().build_tree(post_uid=uid, posts=posts_view)
        if posts_view
        else (
            "",
            "",
        )
    )
    message = "" if raw_text or tree_text else TREE_MESSAGE_EMPTY
    return {
        "post_uid": uid,
        "raw_text": raw_text,
        "tree_text": str(tree_text or "").strip(),
        "message": message,
        "load_error": "",
    }


__all__ = [
    "PostDetailResult",
    "TREE_MESSAGE_EMPTY",
    "TREE_MESSAGE_LOAD_ERROR_PREFIX",
    "get_post_detail",
]
