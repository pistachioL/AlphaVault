"""
Thread tree builder.

Input:
- view_df: current filtered view (tells which post_uid are "selected")
- posts_all: full posts table (used to build complete trees)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, cast

from alphavault.domains.thread_tree.parse import (
    clean_id,
    extract_forward_original_text,
    extract_repost_original_text,
    extract_speaker_name,
    extract_parent_post_id,
    extract_platform_post_id,
    make_synthetic_source_id,
    match_key,
    parse_thread_segments,
    parse_weibo_csv_raw_fields,
)
from alphavault.domains.thread_tree.render import (
    format_ts,
    render_ascii_tree,
    short_text,
    to_ts,
)


def _timestamp_sort_fallback():
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _collect_selected_ids(view_rows: list[dict[str, object]]) -> set[str]:
    selected_ids: set[str] = set()
    for row in view_rows:
        if "platform_post_id" in row:
            item = row.get("platform_post_id")
            pid = clean_id(item)
            if pid:
                selected_ids.add(pid)
    for row in view_rows:
        item = row.get("post_uid")
        pid = clean_id(extract_platform_post_id(item))
        if pid:
            selected_ids.add(pid)
    return selected_ids


def _collect_blogger_authors(
    selected_ids: set[str], *, nodes: dict[str, dict]
) -> set[str]:
    authors: set[str] = set()
    for post_id in selected_ids:
        node = nodes.get(str(post_id)) or {}
        author = str(node.get("author") or "").strip()
        if author:
            authors.add(author)
    return authors


def _post_sort_key(row: dict[str, object]) -> tuple[datetime, str]:
    ts = to_ts(row.get("created_at")) or _timestamp_sort_fallback()
    return (ts, clean_id(row.get("platform_post_id")))


def _assertion_row_sort_key(row: dict[str, object]) -> tuple[str, int, datetime]:
    post_uid = str(row.get("post_uid") or "").strip()
    try:
        idx = int(str(row.get("idx") or "0").strip() or 0)
    except ValueError:
        idx = 0
    created_at = to_ts(row.get("created_at")) or _timestamp_sort_fallback()
    return (post_uid, idx, created_at)


def _prepare_posts(posts_all: list[dict[str, object]]) -> list[dict[str, object]]:
    base_cols = [
        "post_uid",
        "platform_post_id",
        "created_at",
        "author",
        "url",
        "raw_text",
    ]
    posts: list[dict[str, object]] = []
    seen_post_ids: set[str] = set()
    for raw_row in posts_all:
        row = {col: raw_row.get(col) for col in base_cols}
        platform_post_id = clean_id(row.get("platform_post_id"))
        if not platform_post_id:
            platform_post_id = clean_id(extract_platform_post_id(row.get("post_uid")))
        if not platform_post_id or platform_post_id in seen_post_ids:
            continue
        seen_post_ids.add(platform_post_id)
        row["platform_post_id"] = platform_post_id
        posts.append(row)
    return sorted(posts, key=_post_sort_key)


def _add_csv_parent_columns(
    posts: list[dict[str, object]],
) -> list[dict[str, object]]:
    enriched: list[dict[str, object]] = []
    for raw_row in posts:
        row = dict(raw_row)
        csv_fields = parse_weibo_csv_raw_fields(str(row.get("raw_text") or ""))
        row["csv_fields"] = csv_fields
        row["parent_post_id"] = clean_id(
            extract_parent_post_id(csv_fields=csv_fields)
            if isinstance(csv_fields, dict)
            else ""
        )
        enriched.append(row)
    return enriched


def _build_posts_text_index(posts_lookup: dict[str, dict]) -> dict[str, list[dict]]:
    """
    Build a tiny index for inferring repost parent when CSV fields are missing.
    key: normalized text prefix -> list of candidates (post_id, author, created_at_ts)
    """
    index: dict[str, list[dict]] = {}
    for post_id, row in posts_lookup.items():
        pid = clean_id(post_id)
        if not pid:
            continue
        raw_text = str(row.get("raw_text") or "").strip()
        if raw_text:
            key = match_key(raw_text)
            if not key:
                continue
            index.setdefault(key, []).append(
                {
                    "post_id": pid,
                    "author": str(row.get("author") or "").strip(),
                    "created_at": to_ts(row.get("created_at")),
                }
            )
    return index


def _infer_parent_post_id(
    *,
    child_post_id: str,
    child_created_at: object,
    raw_text: str,
    text_index: dict[str, list[dict]],
) -> str:
    """
    Try to infer repost parent post_id by parsing raw_text (no CSV marker) and
    matching the original text to an existing post in posts_all.
    """
    child_id = clean_id(child_post_id)
    if not child_id:
        return ""
    child_ts = to_ts(child_created_at)

    original_text = extract_forward_original_text(raw_text)
    author_hint = ""
    if not original_text:
        author_hint, original_text = extract_repost_original_text(raw_text)
    if not original_text:
        return ""

    key = match_key(original_text)
    if not key:
        return ""

    candidates = list(text_index.get(key, []))
    if author_hint:
        candidates = [
            c for c in candidates if str(c.get("author") or "").strip() == author_hint
        ]

    if child_ts is not None:
        filtered_candidates: list[dict] = []
        for candidate in candidates:
            candidate_ts = to_ts(candidate.get("created_at"))
            if candidate_ts is None or candidate_ts >= child_ts:
                continue
            filtered_candidates.append(candidate)
        candidates = filtered_candidates
    if not candidates:
        return ""

    best = max(
        candidates,
        key=lambda c: to_ts(c.get("created_at")) or _timestamp_sort_fallback(),
    )
    parent_id = clean_id(best.get("post_id"))
    if not parent_id or parent_id == child_id:
        return ""
    return parent_id


def _infer_missing_parent_ids(
    posts: list[dict[str, object]],
) -> list[dict[str, object]]:
    if not posts:
        return posts

    posts_lookup: dict[str, dict] = {
        clean_id(row.get("platform_post_id")): dict(row) for row in posts
    }
    text_index = _build_posts_text_index(posts_lookup) if posts_lookup else {}
    if not text_index:
        return posts

    inferred_rows: list[dict[str, object]] = []
    for raw_row in posts:
        row = dict(raw_row)
        current_parent = clean_id(row.get("parent_post_id"))
        if current_parent:
            inferred_rows.append(row)
            continue
        child_id = clean_id(row.get("platform_post_id"))
        inferred = _infer_parent_post_id(
            child_post_id=child_id,
            child_created_at=row.get("created_at"),
            raw_text=str(row.get("raw_text") or ""),
            text_index=text_index,
        )
        if inferred:
            row["parent_post_id"] = inferred
        inferred_rows.append(row)
    return inferred_rows


def _add_synthetic_sources(
    posts: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[str, dict[str, str]]]:
    # If a post quotes another speaker (raw_text has multi segments), but we still
    # can't find parent_id, create a synthetic "source" parent so root is correct.
    synthetic_sources: dict[str, dict[str, str]] = {}
    if not posts:
        return posts, synthetic_sources

    enriched_posts: list[dict[str, object]] = []
    for raw_row in posts:
        row = dict(raw_row)
        if clean_id(row.get("parent_post_id")):
            enriched_posts.append(row)
            continue
        post_author = str(row.get("author") or "").strip()
        raw_text = str(row.get("raw_text") or "").strip()
        segments = parse_thread_segments(
            raw_text,
            author=post_author,
            raw_text=raw_text,
        )
        if len(segments) < 2:
            enriched_posts.append(row)
            continue
        first_speaker = extract_speaker_name(segments[0])
        if not first_speaker or not post_author or first_speaker == post_author:
            enriched_posts.append(row)
            continue

        source_id = make_synthetic_source_id(segments[0])
        row["parent_post_id"] = source_id
        synthetic_sources.setdefault(
            source_id,
            {
                "author": first_speaker,
                "raw_text": segments[0],
            },
        )
        enriched_posts.append(row)
    return enriched_posts, synthetic_sources


def _attach_parent_info(
    posts: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[str, dict[str, str]]]:
    posts = _add_csv_parent_columns(posts)
    posts = _infer_missing_parent_ids(posts)
    return _add_synthetic_sources(posts)


def _build_assertions_by_post(
    view_rows: list[dict[str, object]],
) -> dict[str, list[dict]]:
    cols = ["idx", "action", "action_strength", "summary", "evidence", "confidence"]
    assertion_cols = [col for col in cols if any(col in row for row in view_rows)]
    if not assertion_cols:
        return {}

    if not any("post_uid" in row for row in view_rows):
        return {}
    deduped: list[dict[str, object]] = []
    seen_keys: set[tuple[object, ...]] = set()
    has_idx = any("idx" in row for row in view_rows)
    for row in view_rows:
        post_uid = str(row.get("post_uid") or "").strip()
        if not post_uid:
            continue
        dedupe_key: tuple[object, ...]
        if has_idx:
            dedupe_key = (post_uid, row.get("idx"))
        else:
            dedupe_key = tuple([post_uid, *[row.get(col) for col in assertion_cols]])
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped.append(dict(row))

    ordered = sorted(deduped, key=_assertion_row_sort_key)

    assertions_by_post: dict[str, list[dict]] = {}
    for row in ordered:
        post_uid = str(row.get("post_uid") or "").strip()
        if not post_uid:
            continue
        record = {col: row.get(col) for col in assertion_cols}
        assertions_by_post.setdefault(post_uid, []).append(record)
    return assertions_by_post


def _build_nodes(
    posts: list[dict[str, object]],
    *,
    assertions_by_post: dict[str, list[dict]],
    synthetic_sources: dict[str, dict[str, str]],
) -> dict[str, dict]:
    nodes: dict[str, dict] = {}
    for row in posts:
        platform_post_id = clean_id(row.get("platform_post_id"))
        if not platform_post_id:
            continue
        post_uid = str(row.get("post_uid") or "").strip()
        parent_post_id = clean_id(row.get("parent_post_id"))
        nodes[platform_post_id] = {
            "platform_post_id": platform_post_id,
            "post_uid": post_uid,
            "created_at": row.get("created_at"),
            "author": str(row.get("author") or "").strip(),
            "url": str(row.get("url") or "").strip(),
            "raw_text": str(row.get("raw_text") or ""),
            "csv_fields": row.get("csv_fields")
            if isinstance(row.get("csv_fields"), dict)
            else {},
            "parent_post_id": parent_post_id,
            "assertions": assertions_by_post.get(post_uid, []),
            "missing": False,
            "kind": "repost" if parent_post_id else "status",
        }

    for source_id, src in synthetic_sources.items():
        if source_id in nodes:
            continue
        nodes[source_id] = {
            "platform_post_id": source_id,
            "post_uid": "",
            "created_at": None,
            "author": str(src.get("author") or "").strip(),
            "url": "",
            "raw_text": str(src.get("raw_text") or ""),
            "csv_fields": {},
            "parent_post_id": "",
            "assertions": [],
            "missing": True,
            "kind": "source",
        }
    return nodes


def _weibo_url(user_id: object, bid: object) -> str:
    uid = clean_id(user_id)
    b = str(bid or "").strip()
    if not uid or not b:
        return ""
    return f"https://weibo.com/{uid}/{b}"


def _source_stub_from_child_csv(csv_fields: Dict[str, Any]) -> dict[str, object]:
    src_author = str(csv_fields.get("源用户昵称") or "").strip()
    src_text = str(csv_fields.get("源微博正文") or "").strip()
    src_created = (
        csv_fields.get("源微博完整日期") or csv_fields.get("源微博日期") or None
    )
    src_url = _weibo_url(csv_fields.get("源用户id"), csv_fields.get("源微博bid"))
    return {
        "author": src_author,
        "raw_text": src_text,
        "created_at": src_created,
        "url": src_url,
    }


def _ensure_parent_stubs(nodes: dict[str, dict]) -> None:
    queue: list[str] = list(nodes.keys())
    while queue:
        node_id = queue.pop()
        node = nodes.get(node_id) or {}
        parent_id = clean_id(node.get("parent_post_id"))
        if not parent_id or parent_id == node_id:
            continue
        if parent_id in nodes:
            continue

        raw_csv_fields = node.get("csv_fields")
        child_csv: Dict[str, Any]
        if isinstance(raw_csv_fields, dict):
            child_csv = cast(Dict[str, Any], raw_csv_fields)
        else:
            child_csv = {}
        source_stub = _source_stub_from_child_csv(child_csv)
        child_raw_text = str(node.get("raw_text") or "")
        if not str(source_stub.get("raw_text") or "").strip():
            forward_original = extract_forward_original_text(child_raw_text)
            if forward_original:
                source_stub["raw_text"] = forward_original
        child_segments = parse_thread_segments(
            child_raw_text,
            author=str(node.get("author") or "").strip(),
            raw_text=child_raw_text,
        )
        fallback_source = child_segments[0] if child_segments else ""
        if fallback_source and not str(source_stub.get("raw_text") or "").strip():
            source_stub["raw_text"] = fallback_source

        nodes[parent_id] = {
            "platform_post_id": parent_id,
            "post_uid": "",
            "created_at": source_stub.get("created_at"),
            "author": str(source_stub.get("author") or "").strip(),
            "url": str(source_stub.get("url") or "").strip(),
            "raw_text": str(source_stub.get("raw_text") or ""),
            "csv_fields": {},
            "parent_post_id": "",
            "assertions": [],
            "missing": True,
            "kind": "source",
        }


def _build_children_map(nodes: dict[str, dict]) -> dict[str, list[str]]:
    children: dict[str, list[str]] = {}
    for node_id, node in nodes.items():
        parent_id = clean_id(node.get("parent_post_id"))
        if parent_id and parent_id in nodes and parent_id != node_id:
            children.setdefault(parent_id, []).append(node_id)

    def node_sort_key(nid: str) -> tuple:
        ts = to_ts((nodes.get(nid) or {}).get("created_at"))
        ts_val = ts if ts is not None else _timestamp_sort_fallback()
        return (ts_val, nid)

    for parent_id, kids in children.items():
        kids.sort(key=node_sort_key)
        children[parent_id] = kids
    return children


def _find_root_id(start_id: str, *, nodes: dict[str, dict]) -> str:
    nid = clean_id(start_id)
    visited_local: set[str] = set()
    while nid and nid not in visited_local:
        visited_local.add(nid)
        node = nodes.get(nid) or {}
        pid = clean_id(node.get("parent_post_id"))
        if not pid or pid == nid or pid not in nodes:
            return nid
        nid = pid
    return nid or clean_id(start_id)


def _collect_relevant_roots(
    selected_ids: set[str], *, nodes: dict[str, dict]
) -> list[str]:
    relevant_roots: list[str] = []
    seen_roots: set[str] = set()
    for sid in selected_ids:
        if sid not in nodes:
            continue
        rid = _find_root_id(sid, nodes=nodes)
        if not rid or rid in seen_roots:
            continue
        seen_roots.add(rid)
        relevant_roots.append(rid)
    return relevant_roots


def _subtree_ids(root_id: str, *, children: dict[str, list[str]]) -> set[str]:
    stack = [root_id]
    subtree: set[str] = set()
    while stack:
        nid = stack.pop()
        if nid in subtree:
            continue
        subtree.add(nid)
        stack.extend(children.get(nid, []))
    return subtree


def _latest_activity(subtree: set[str], *, nodes: dict[str, dict]) -> datetime | None:
    latest: datetime | None = None
    for nid in subtree:
        ts = to_ts((nodes.get(nid) or {}).get("created_at"))
        if ts is None:
            continue
        latest = ts if latest is None else max(latest, ts)
    return latest


def _thread_label(
    root_id: str, *, nodes: dict[str, dict], children: dict[str, list[str]]
) -> str:
    root_node = nodes.get(root_id) or {}
    label_node_id = root_id
    if bool(root_node.get("missing")):
        label_node_id = (children.get(root_id) or [root_id])[0]
    label_node = nodes.get(label_node_id) or root_node
    label_author = str(label_node.get("author") or "").strip() or "未知"
    label_ts = format_ts(label_node.get("created_at"))
    label_preview = short_text(label_node.get("raw_text"), n=40)
    return f"{label_ts} · {label_author} · {label_preview}".strip(" ·")


def _build_thread(
    root_id: str,
    *,
    nodes: dict[str, dict],
    children: dict[str, list[str]],
    blogger_authors: set[str],
) -> dict:
    subtree = _subtree_ids(root_id, children=children)
    latest = _latest_activity(subtree, nodes=nodes)
    tree_text, order = render_ascii_tree(
        root_id,
        nodes=nodes,
        children=children,
        blogger_authors=blogger_authors,
    )
    label = _thread_label(root_id, nodes=nodes, children=children)
    return {
        "root_id": root_id,
        "label": label,
        "latest_activity": latest,
        "tree_text": tree_text,
        "order": order,
        "nodes": {nid: nodes[nid] for nid in subtree if nid in nodes},
    }


def build_weibo_thread_forest(
    view_rows: list[dict[str, object]], *, posts_all: list[dict[str, object]]
) -> list[dict]:
    """
    Build parent-child thread forest for a selected topic/board.

    Important: one tree must be complete.
    - view_df only tells us which posts belong to the current filter (selected_ids)
    - real tree nodes come from posts_all (full posts table), so we can include
      siblings/children even if they have no assertions.
    """
    if not view_rows or not any("post_uid" in row for row in view_rows):
        return []

    selected_ids = _collect_selected_ids(view_rows)
    if not selected_ids:
        return []

    posts = _prepare_posts(posts_all)
    if not posts:
        return []

    posts, synthetic_sources = _attach_parent_info(posts)
    assertions_by_post = _build_assertions_by_post(view_rows)
    nodes = _build_nodes(
        posts,
        assertions_by_post=assertions_by_post,
        synthetic_sources=synthetic_sources,
    )
    _ensure_parent_stubs(nodes)
    children = _build_children_map(nodes)
    blogger_authors = _collect_blogger_authors(selected_ids, nodes=nodes)

    roots = _collect_relevant_roots(selected_ids, nodes=nodes)
    threads = [
        _build_thread(
            root_id,
            nodes=nodes,
            children=children,
            blogger_authors=blogger_authors,
        )
        for root_id in roots
    ]
    threads.sort(
        key=lambda item: to_ts(item.get("latest_activity"))
        or _timestamp_sort_fallback(),
        reverse=True,
    )
    return threads


__all__ = ["build_weibo_thread_forest"]
