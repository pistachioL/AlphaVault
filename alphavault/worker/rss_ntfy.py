from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import requests

from alphavault.ai.analyze import ALLOWED_ACTIONS
from alphavault.constants import (
    DEFAULT_RSS_NTFY_NOTIFY_TIMEOUT_SECONDS,
    DEFAULT_RSS_NTFY_SENT_TTL_SECONDS,
    ENV_RSS_NTFY_RULES_JSON,
)
from alphavault.db.source_queue import CloudPost
from alphavault.logging_config import get_logger

RSS_NTFY_MODE_TRADE_ONLY = "trade_only"
RSS_NTFY_MODE_INVEST_ALL = "invest_all"
RSS_NTFY_MODE_CUSTOM_ACTIONS = "custom_actions"
RSS_NTFY_ALLOWED_MODES = {
    RSS_NTFY_MODE_TRADE_ONLY,
    RSS_NTFY_MODE_INVEST_ALL,
    RSS_NTFY_MODE_CUSTOM_ACTIONS,
}
RSS_NTFY_TRADE_ACTIONS = {
    "trade.buy",
    "trade.add",
    "trade.reduce",
    "trade.sell",
    "trade.hold",
    "trade.watch",
}
RSS_NTFY_TITLE_DEFAULT_PREFIX = "AlphaVault RSS"
RSS_NTFY_SENT_KEY_PREFIX = "rss_ntfy_sent"
RSS_NTFY_MAX_SUMMARY_COUNT = 3
RSS_NTFY_MAX_RAW_TEXT_CHARS = 240
logger = get_logger(__name__)


@dataclass(frozen=True)
class RSSNtfyRuleMatch:
    source_names: tuple[str, ...]
    rss_url_contains: tuple[str, ...]
    authors: tuple[str, ...]


@dataclass(frozen=True)
class RSSNtfyTarget:
    publish_url: str
    token: str
    title_prefix: str


@dataclass(frozen=True)
class RSSNtfyRule:
    rule_id: str
    enabled: bool
    mode: str
    match: RSSNtfyRuleMatch
    actions: tuple[str, ...]
    min_invest_score: float
    ntfy: RSSNtfyTarget


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _sha1_short(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:20]


def _coerce_bool(value: object, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = _clean_text(value).lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _coerce_score(value: object, *, field: str) -> float:
    try:
        score = float(_clean_text(value) or "0")
    except Exception as exc:
        raise RuntimeError(f"invalid_rss_ntfy_{field}") from exc
    if score < 0.0 or score > 1.0:
        raise RuntimeError(f"invalid_rss_ntfy_{field}")
    return float(score)


def _normalize_text_tuple(
    value: object,
    *,
    field: str,
    lowercase: bool = False,
) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise RuntimeError(f"invalid_rss_ntfy_{field}")
    seen: set[str] = set()
    out: list[str] = []
    for item in value:
        text = _clean_text(item)
        if lowercase:
            text = text.lower()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return tuple(out)


def _parse_match(raw_match: object) -> RSSNtfyRuleMatch:
    if raw_match is None:
        return RSSNtfyRuleMatch((), (), ())
    if not isinstance(raw_match, dict):
        raise RuntimeError("invalid_rss_ntfy_match")
    return RSSNtfyRuleMatch(
        source_names=_normalize_text_tuple(
            raw_match.get("source_names"),
            field="source_names",
            lowercase=True,
        ),
        rss_url_contains=_normalize_text_tuple(
            raw_match.get("rss_url_contains"),
            field="rss_url_contains",
        ),
        authors=_normalize_text_tuple(
            raw_match.get("authors"),
            field="authors",
        ),
    )


def _parse_target(raw_target: object) -> RSSNtfyTarget:
    if not isinstance(raw_target, dict):
        raise RuntimeError("invalid_rss_ntfy_target")
    publish_url = _clean_text(raw_target.get("publish_url"))
    if not publish_url.startswith(("http://", "https://")):
        raise RuntimeError("invalid_rss_ntfy_publish_url")
    return RSSNtfyTarget(
        publish_url=publish_url,
        token=_clean_text(raw_target.get("token")),
        title_prefix=_clean_text(raw_target.get("title_prefix"))
        or RSS_NTFY_TITLE_DEFAULT_PREFIX,
    )


def _parse_rule(raw_rule: object) -> RSSNtfyRule:
    if not isinstance(raw_rule, dict):
        raise RuntimeError("invalid_rss_ntfy_rule")
    rule_id = _clean_text(raw_rule.get("rule_id"))
    if not rule_id:
        raise RuntimeError("invalid_rss_ntfy_rule_id")
    mode = _clean_text(raw_rule.get("mode")).lower()
    if mode not in RSS_NTFY_ALLOWED_MODES:
        raise RuntimeError(f"invalid_rss_ntfy_mode:{rule_id}")
    actions = _normalize_text_tuple(
        raw_rule.get("actions"),
        field="actions",
        lowercase=True,
    )
    if mode == RSS_NTFY_MODE_CUSTOM_ACTIONS:
        if not actions:
            raise RuntimeError(f"invalid_rss_ntfy_actions:{rule_id}")
        unknown_actions = [
            action for action in actions if action not in ALLOWED_ACTIONS
        ]
        if unknown_actions:
            raise RuntimeError(f"invalid_rss_ntfy_actions:{rule_id}")
    return RSSNtfyRule(
        rule_id=rule_id,
        enabled=_coerce_bool(raw_rule.get("enabled"), default=True),
        mode=mode,
        match=_parse_match(raw_rule.get("match")),
        actions=actions,
        min_invest_score=_coerce_score(
            raw_rule.get("min_invest_score"),
            field="min_invest_score",
        ),
        ntfy=_parse_target(raw_rule.get("ntfy")),
    )


@lru_cache(maxsize=8)
def _parse_rules_json(raw_rules: str) -> tuple[RSSNtfyRule, ...]:
    text = _clean_text(raw_rules)
    if not text:
        return ()
    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise RuntimeError("invalid_rss_ntfy_rules_json") from exc
    if not isinstance(parsed, list):
        raise RuntimeError("invalid_rss_ntfy_rules_json")
    seen_rule_ids: set[str] = set()
    rules: list[RSSNtfyRule] = []
    for raw_rule in parsed:
        rule = _parse_rule(raw_rule)
        if rule.rule_id in seen_rule_ids:
            raise RuntimeError(f"duplicate_rss_ntfy_rule_id:{rule.rule_id}")
        seen_rule_ids.add(rule.rule_id)
        rules.append(rule)
    return tuple(rules)


def load_rss_ntfy_rules_from_env() -> tuple[RSSNtfyRule, ...]:
    return _parse_rules_json(os.getenv(ENV_RSS_NTFY_RULES_JSON, ""))


def _matches_text_options(options: tuple[str, ...], value: str) -> bool:
    if not options:
        return True
    return value in options


def _matches_substrings(options: tuple[str, ...], value: str) -> bool:
    if not options:
        return True
    return any(option in value for option in options)


def _rule_matches(
    *,
    rule: RSSNtfyRule,
    source_name: str,
    feed_url: str,
    author: str,
) -> bool:
    if not rule.enabled:
        return False
    normalized_source_name = _clean_text(source_name).lower()
    normalized_author = _clean_text(author)
    normalized_feed_url = _clean_text(feed_url)
    return (
        _matches_text_options(rule.match.source_names, normalized_source_name)
        and _matches_substrings(rule.match.rss_url_contains, normalized_feed_url)
        and _matches_text_options(rule.match.authors, normalized_author)
    )


def _assertion_actions(rows: list[dict[str, object]]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for row in rows:
        action = _clean_text(row.get("action")).lower()
        if not action or action in seen:
            continue
        seen.add(action)
        out.append(action)
    return tuple(out)


def _rule_triggered(
    *,
    rule: RSSNtfyRule,
    final_status: str,
    invest_score: float,
    actions: tuple[str, ...],
) -> bool:
    if _clean_text(final_status) != "relevant":
        return False
    if float(invest_score) < float(rule.min_invest_score):
        return False
    if rule.mode == RSS_NTFY_MODE_INVEST_ALL:
        return True
    if rule.mode == RSS_NTFY_MODE_TRADE_ONLY:
        return any(action in RSS_NTFY_TRADE_ACTIONS for action in actions)
    if rule.mode == RSS_NTFY_MODE_CUSTOM_ACTIONS:
        return any(action in rule.actions for action in actions)
    return False


def _build_ntfy_title(
    *,
    rule: RSSNtfyRule,
    post: CloudPost,
    actions: tuple[str, ...],
) -> str:
    action_text = actions[0] if actions else "relevant"
    return (
        f"{rule.ntfy.title_prefix} {post.author or post.platform} {action_text}".strip()
    )


def _truncate_text(value: str, *, limit: int) -> str:
    text = _clean_text(value)
    if len(text) <= int(limit):
        return text
    return text[: max(0, int(limit) - 3)].rstrip() + "..."


def _build_body(
    *,
    rule: RSSNtfyRule,
    post: CloudPost,
    source_name: str,
    invest_score: float,
    actions: tuple[str, ...],
    rows: list[dict[str, object]],
) -> str:
    body_lines = [
        f"作者: {post.author or '(empty)'}",
        f"来源: {source_name or post.platform}",
        f"时间: {post.created_at or '(empty)'}",
        f"规则: {rule.rule_id}",
        f"模式: {rule.mode}",
        f"分数: {float(invest_score):.2f}",
        f"动作: {', '.join(actions) if actions else '(empty)'}",
    ]
    for row in rows[:RSS_NTFY_MAX_SUMMARY_COUNT]:
        summary = _clean_text(row.get("summary"))
        if summary:
            body_lines.append(f"- {summary}")
    excerpt = _truncate_text(post.raw_text or "", limit=RSS_NTFY_MAX_RAW_TEXT_CHARS)
    if excerpt:
        body_lines.append("")
        body_lines.append(excerpt)
    if post.url:
        body_lines.append("")
        body_lines.append(f"原文链接: {post.url}")
    if post.feed_url:
        body_lines.append(f"RSS: {post.feed_url}")
    return "\n".join(body_lines)


def _notification_dedup_key(
    *, redis_queue_key: str, rule_id: str, post_uid: str
) -> str:
    return (
        f"{redis_queue_key}:{RSS_NTFY_SENT_KEY_PREFIX}:"
        f"{_sha1_short(f'{rule_id}:{post_uid}')}"
    )


def _mark_notification_sent(
    *,
    redis_client: Any,
    redis_queue_key: str,
    rule_id: str,
    post_uid: str,
) -> bool:
    if not redis_client or not _clean_text(redis_queue_key):
        return False
    dedup_key = _notification_dedup_key(
        redis_queue_key=redis_queue_key,
        rule_id=rule_id,
        post_uid=post_uid,
    )
    try:
        ok = redis_client.set(
            dedup_key,
            "1",
            nx=True,
            ex=max(1, int(DEFAULT_RSS_NTFY_SENT_TTL_SECONDS)),
        )
    except Exception as err:
        logger.warning(
            "[rss_ntfy] dedup_error rule_id=%s post_uid=%s error=%s: %s",
            rule_id,
            post_uid,
            type(err).__name__,
            err,
        )
        return False
    return bool(ok)


def _clear_notification_sent_marker(
    *,
    redis_client: Any,
    redis_queue_key: str,
    rule_id: str,
    post_uid: str,
) -> None:
    if not redis_client or not _clean_text(redis_queue_key):
        return
    dedup_key = _notification_dedup_key(
        redis_queue_key=redis_queue_key,
        rule_id=rule_id,
        post_uid=post_uid,
    )
    try:
        redis_client.delete(dedup_key)
    except Exception:
        return


def _publish_ntfy(
    *,
    rule: RSSNtfyRule,
    title: str,
    body: str,
    click_url: str,
) -> None:
    headers = {
        "Title": title,
        "Priority": "default",
        "Tags": "money_with_wings,chart_with_upwards_trend",
    }
    if click_url:
        headers["Click"] = click_url
    if rule.ntfy.token:
        headers["Authorization"] = f"Bearer {rule.ntfy.token}"
    response = requests.post(
        rule.ntfy.publish_url,
        data=body.encode("utf-8"),
        headers=headers,
        timeout=float(DEFAULT_RSS_NTFY_NOTIFY_TIMEOUT_SECONDS),
    )
    response.raise_for_status()


def maybe_publish_rss_ntfy_notifications(
    *,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str,
    post: CloudPost,
    final_status: str,
    invest_score: float,
    rows: list[dict[str, object]],
) -> None:
    feed_url = _clean_text(post.feed_url)
    if not feed_url:
        return
    rules = load_rss_ntfy_rules_from_env()
    if not rules:
        return
    if not redis_client or not _clean_text(redis_queue_key):
        return
    actions = _assertion_actions(rows)
    for rule in rules:
        if not _rule_matches(
            rule=rule,
            source_name=source_name,
            feed_url=feed_url,
            author=str(post.author or ""),
        ):
            continue
        if not _rule_triggered(
            rule=rule,
            final_status=final_status,
            invest_score=invest_score,
            actions=actions,
        ):
            continue
        logger.info(
            "[rss_ntfy] rule_hit rule_id=%s post_uid=%s source=%s",
            rule.rule_id,
            post.post_uid,
            source_name,
        )
        if not _mark_notification_sent(
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            rule_id=rule.rule_id,
            post_uid=post.post_uid,
        ):
            continue
        title = _build_ntfy_title(rule=rule, post=post, actions=actions)
        body = _build_body(
            rule=rule,
            post=post,
            source_name=source_name,
            invest_score=invest_score,
            actions=actions,
            rows=rows,
        )
        try:
            _publish_ntfy(
                rule=rule,
                title=title,
                body=body,
                click_url=str(post.url or "").strip(),
            )
        except Exception as err:
            _clear_notification_sent_marker(
                redis_client=redis_client,
                redis_queue_key=redis_queue_key,
                rule_id=rule.rule_id,
                post_uid=post.post_uid,
            )
            logger.warning(
                "[rss_ntfy] publish_error rule_id=%s post_uid=%s error=%s: %s",
                rule.rule_id,
                post.post_uid,
                type(err).__name__,
                err,
            )


__all__ = [
    "RSSNtfyRule",
    "RSSNtfyRuleMatch",
    "RSSNtfyTarget",
    "load_rss_ntfy_rules_from_env",
    "maybe_publish_rss_ntfy_notifications",
]
