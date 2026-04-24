from __future__ import annotations

from typing import Optional

from alphavault.ai.post_context_prompt import build_post_context_prompt
from alphavault.weibo.topic_prompt_tree import build_topic_runtime_context


def max_message_tree_text_len(node: object) -> int:
    if not isinstance(node, dict):
        return 0
    max_len = len(str(node.get("text") or ""))
    children = node.get("children")
    if isinstance(children, list):
        for child in children:
            max_len = max(max_len, max_message_tree_text_len(child))
    return max_len


def build_post_context_prompt_with_prompt_chars_limit(
    *,
    root_key: str,
    root_segment: str,
    root_content_key: str,
    focus_username: str,
    posts: list[dict[str, object]],
    max_prompt_chars: int,
) -> tuple[dict[str, object], int, str, int, int, bool, bool]:
    def build_ctx(
        *,
        node_chars: int,
        include_comments: bool,
    ) -> tuple[dict[str, object], int]:
        return build_topic_runtime_context(
            root_key=root_key,
            root_segment=root_segment,
            root_content_key=root_content_key,
            focus_username=focus_username,
            posts=posts,
            manual_feedback_hint=None,
            include_virtual_comments=bool(include_comments),
            max_node_text_chars=int(node_chars),
        )

    def build_prompt(ctx: dict[str, object], *, compact_json: bool) -> tuple[str, int]:
        pkg = ctx.get("ai_topic_package")
        if not isinstance(pkg, dict):
            raise RuntimeError("ai_context_package_invalid")
        prompt = build_post_context_prompt(
            ai_topic_package=pkg,
            compact_json=bool(compact_json),
        )
        return prompt, len(prompt)

    def search_best_cap(
        *,
        include_comments: bool,
    ) -> Optional[tuple[dict[str, object], int, str, int, int]]:
        base_ctx, _base_truncated = build_ctx(
            node_chars=0,
            include_comments=include_comments,
        )
        max_len = max(1, max_message_tree_text_len(base_ctx.get("message_tree")))
        lo = 1
        hi = int(max_len)
        best: Optional[tuple[dict[str, object], int, str, int, int]] = None
        while lo <= hi:
            mid = (lo + hi) // 2
            mid_ctx, mid_truncated = build_ctx(
                node_chars=mid,
                include_comments=include_comments,
            )
            mid_prompt, mid_chars = build_prompt(mid_ctx, compact_json=True)
            if mid_chars <= max_prompt_chars:
                best = (
                    mid_ctx,
                    int(mid_truncated),
                    mid_prompt,
                    int(mid_chars),
                    int(mid),
                )
                lo = mid + 1
                continue
            hi = mid - 1
        return best

    ctx_full, truncated_full = build_ctx(node_chars=0, include_comments=True)
    pretty_prompt, pretty_chars = build_prompt(ctx_full, compact_json=False)
    if max_prompt_chars <= 0 or pretty_chars <= max_prompt_chars:
        return (
            ctx_full,
            int(truncated_full),
            pretty_prompt,
            int(pretty_chars),
            0,
            False,
            True,
        )

    compact_prompt, compact_chars = build_prompt(ctx_full, compact_json=True)
    if compact_chars <= max_prompt_chars:
        return (
            ctx_full,
            int(truncated_full),
            compact_prompt,
            int(compact_chars),
            0,
            True,
            True,
        )

    best = search_best_cap(include_comments=True)
    if best is not None:
        best_ctx, best_truncated, best_prompt, best_prompt_chars, best_cap = best
        return (
            best_ctx,
            best_truncated,
            best_prompt,
            best_prompt_chars,
            best_cap,
            True,
            True,
        )

    ctx_no_comments, truncated_nc = build_ctx(node_chars=0, include_comments=False)
    nc_pretty_prompt, nc_pretty_chars = build_prompt(
        ctx_no_comments,
        compact_json=False,
    )
    if nc_pretty_chars <= max_prompt_chars:
        return (
            ctx_no_comments,
            int(truncated_nc),
            nc_pretty_prompt,
            int(nc_pretty_chars),
            0,
            False,
            False,
        )

    nc_compact_prompt, nc_compact_chars = build_prompt(
        ctx_no_comments,
        compact_json=True,
    )
    if nc_compact_chars <= max_prompt_chars:
        return (
            ctx_no_comments,
            int(truncated_nc),
            nc_compact_prompt,
            int(nc_compact_chars),
            0,
            True,
            False,
        )

    best_nc = search_best_cap(include_comments=False)
    if best_nc is not None:
        best_ctx, best_truncated, best_prompt, best_prompt_chars, best_cap = best_nc
        return (
            best_ctx,
            best_truncated,
            best_prompt,
            best_prompt_chars,
            best_cap,
            True,
            False,
        )

    raise RuntimeError(
        f"post_context_prompt_too_long max_prompt_chars={max_prompt_chars}"
    )


__all__ = [
    "build_post_context_prompt_with_prompt_chars_limit",
    "max_message_tree_text_len",
]
