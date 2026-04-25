from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import os
from pathlib import Path

from alphavault.ai._client import _call_ai_with_litellm
from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
)
from alphavault.ai.post_context_prompt import POST_CONTEXT_PROMPT_VERSION
from alphavault.ai.tag_validate import validate_post_context_ai_result
from alphavault.constants import (
    ENV_AI_CONTEXT_API_KEY,
    ENV_AI_CONTEXT_API_MODE,
    ENV_AI_CONTEXT_BASE_URL,
    ENV_AI_CONTEXT_MAX_INFLIGHT,
    ENV_AI_CONTEXT_MODEL,
    ENV_AI_CONTEXT_REASONING_EFFORT,
    ENV_AI_CONTEXT_RETRIES,
    ENV_AI_CONTEXT_RPM,
    ENV_AI_CONTEXT_TEMPERATURE,
    ENV_AI_CONTEXT_TIMEOUT_SEC,
)
from alphavault.db.source_queue import CloudPost
from alphavault.domains.common.assertion_entities import (
    ASSERTION_ENTITY_TYPE_STOCK,
    build_assertion_entities,
)
from alphavault.domains.entity_match import (
    EntityMatchResult,
    load_entity_match_lookup_maps,
    resolve_assertion_mentions,
)
from alphavault.infra.ai.runtime_config import (
    AiRuntimeConfig,
    ai_runtime_config_from_env,
)
from alphavault.logging_config import get_logger
from alphavault.rss.utils import now_str
from alphavault.weibo.topic_prompt_tree import (
    MAX_TOPIC_PROMPT_CHARS,
    thread_root_info_for_post,
)

from .post_context_prompt import build_post_context_prompt_with_prompt_chars_limit


_TEXT_EXCERPT_LIMIT = 220
logger = get_logger(__name__)


@dataclass(frozen=True)
class PostContextResult:
    model: str
    prompt_version: str
    processed_at: str
    mentions: list[dict[str, object]]
    entities: list[dict[str, object]]
    entity_match_result: EntityMatchResult


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _env_optional_text(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    return str(raw).strip()


def post_context_ai_runtime_config_from_env(
    *,
    timeout_seconds_default: float,
) -> AiRuntimeConfig:
    fallback = ai_runtime_config_from_env(
        timeout_seconds_default=timeout_seconds_default
    )
    reasoning_effort = _env_optional_text(ENV_AI_CONTEXT_REASONING_EFFORT)
    return AiRuntimeConfig(
        api_key=os.getenv(ENV_AI_CONTEXT_API_KEY, "").strip() or fallback.api_key,
        model=os.getenv(ENV_AI_CONTEXT_MODEL, DEFAULT_MODEL).strip() or fallback.model,
        base_url=os.getenv(ENV_AI_CONTEXT_BASE_URL, "").strip() or fallback.base_url,
        api_mode=os.getenv(ENV_AI_CONTEXT_API_MODE, DEFAULT_AI_MODE).strip()
        or fallback.api_mode,
        temperature=_env_float(
            ENV_AI_CONTEXT_TEMPERATURE,
            fallback.temperature or DEFAULT_AI_TEMPERATURE,
        ),
        reasoning_effort=(
            fallback.reasoning_effort if reasoning_effort is None else reasoning_effort
        ),
        timeout_seconds=max(
            1.0,
            _env_float(
                ENV_AI_CONTEXT_TIMEOUT_SEC,
                fallback.timeout_seconds or timeout_seconds_default,
            ),
        ),
        retries=max(
            0,
            _env_int(
                ENV_AI_CONTEXT_RETRIES,
                fallback.retries or DEFAULT_AI_RETRY_COUNT,
            ),
        ),
        ai_rpm=max(0.0, _env_float(ENV_AI_CONTEXT_RPM, fallback.ai_rpm)),
        ai_max_inflight=max(
            1,
            _env_int(
                ENV_AI_CONTEXT_MAX_INFLIGHT,
                fallback.ai_max_inflight or 1,
            ),
        ),
    )


def _clip_text(value: object, *, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, int(limit))].rstrip()


def _build_current_post_row(post: CloudPost) -> dict[str, object]:
    return {
        "post_uid": post.post_uid,
        "platform_post_id": post.platform_post_id,
        "author": post.author,
        "created_at": post.created_at,
        "url": post.url,
        "raw_text": post.raw_text,
        "processed_at": "",
        "ai_status": "running",
        "ai_retry_count": int(post.ai_retry_count or 0),
    }


def _normalize_mentions(parsed: dict[str, object]) -> list[dict[str, object]]:
    mentions_raw = parsed.get("mentions")
    if not isinstance(mentions_raw, list):
        raise RuntimeError("ai_context_mentions_missing")
    mentions: list[dict[str, object]] = []
    for raw_mention in mentions_raw:
        if not isinstance(raw_mention, dict):
            continue
        mention_text = str(raw_mention.get("mention_text") or "").strip()
        mention_type = str(raw_mention.get("mention_type") or "").strip()
        if not mention_text or not mention_type:
            continue
        mentions.append(
            {
                "mention_text": mention_text,
                "mention_norm": mention_text,
                "mention_type": mention_type,
                "evidence": str(raw_mention.get("evidence") or "").strip(),
                "confidence": raw_mention.get("confidence"),
            }
        )
    return mentions


def _dedupe_parsed_mentions_in_place(parsed: dict[str, object]) -> None:
    mentions_raw = parsed.get("mentions")
    if not isinstance(mentions_raw, list):
        return
    deduped_mentions: list[object] = []
    seen_mention_texts: set[str] = set()
    for raw_mention in mentions_raw:
        if not isinstance(raw_mention, dict):
            deduped_mentions.append(raw_mention)
            continue
        mention_text = str(raw_mention.get("mention_text") or "").strip()
        if not mention_text:
            deduped_mentions.append(raw_mention)
            continue
        if mention_text in seen_mention_texts:
            continue
        seen_mention_texts.add(mention_text)
        deduped_mentions.append(raw_mention)
    parsed["mentions"] = deduped_mentions


def _sanitize_and_validate_post_context_ai_result(parsed: dict[str, object]) -> None:
    _dedupe_parsed_mentions_in_place(parsed)
    validate_post_context_ai_result(parsed)


def _resolve_context_entities(
    engine_or_conn,
    *,
    post_uid: str,
    mentions: list[dict[str, object]],
    source_text_excerpt: str,
) -> EntityMatchResult:
    stock_name_texts: list[str] = []
    stock_alias_texts: list[str] = []
    seen_stock_names: set[str] = set()
    seen_stock_aliases: set[str] = set()
    for item in mentions:
        mention_text = str(item.get("mention_text") or "").strip()
        mention_type = str(item.get("mention_type") or "").strip()
        if not mention_text:
            continue
        if mention_type == "stock_name" and mention_text not in seen_stock_names:
            seen_stock_names.add(mention_text)
            stock_name_texts.append(mention_text)
            continue
        if mention_type == "stock_alias" and mention_text not in seen_stock_aliases:
            seen_stock_aliases.add(mention_text)
            stock_alias_texts.append(mention_text)
    stock_name_targets: dict[str, str] = {}
    stock_alias_targets: dict[str, str] = {}
    if stock_name_texts or stock_alias_texts:
        from alphavault.research_workbench.service import (
            get_research_workbench_engine_from_env,
        )

        standard_engine = get_research_workbench_engine_from_env()
        stock_name_targets, stock_alias_targets = load_entity_match_lookup_maps(
            standard_engine,
            stock_name_texts=stock_name_texts,
            stock_alias_texts=stock_alias_texts,
        )
    sample_evidence = ""
    if mentions:
        sample_evidence = str(mentions[0].get("evidence") or "").strip()
    return resolve_assertion_mentions(
        engine_or_conn,
        assertion_mentions=mentions,
        stock_name_targets=stock_name_targets,
        stock_alias_targets=stock_alias_targets,
        alias_task_sample={
            "sample_post_uid": str(post_uid or "").strip(),
            "sample_evidence": sample_evidence,
            "sample_raw_text_excerpt": str(source_text_excerpt or "").strip(),
        },
    )


def extract_stock_entity_keys_from_entities(
    entities: list[dict[str, object]],
) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for raw_entity in entities:
        if not isinstance(raw_entity, dict):
            continue
        entity_key = str(raw_entity.get("entity_key") or "").strip()
        entity_type = str(raw_entity.get("entity_type") or "").strip()
        if entity_type != ASSERTION_ENTITY_TYPE_STOCK:
            continue
        if not entity_key.startswith("stock:") or entity_key in seen:
            continue
        seen.add(entity_key)
        keys.append(entity_key)
    return sorted(keys)


def extract_post_context_result(
    engine_or_conn,
    *,
    post: CloudPost,
    runtime_config: AiRuntimeConfig,
    request_gate: Callable[[], None] | None = None,
    trace_out: Path | None = None,
) -> PostContextResult:
    focus = str(post.author or "").strip()
    root_key, root_segment, root_content_key = thread_root_info_for_post(
        raw_text=post.raw_text or "",
        author=focus,
    )
    current_row = _build_current_post_row(post)
    (
        _runtime_context,
        _truncated_nodes,
        prompt,
        _prompt_chars,
        _node_chars,
        _compact_json,
        _include_comments,
    ) = build_post_context_prompt_with_prompt_chars_limit(
        root_key=root_key,
        root_segment=root_segment,
        root_content_key=root_content_key,
        focus_username=focus,
        posts=[current_row],
        max_prompt_chars=MAX_TOPIC_PROMPT_CHARS,
    )
    parsed = _call_ai_with_litellm(
        prompt=prompt,
        api_mode=str(runtime_config.api_mode or DEFAULT_AI_MODE),
        ai_stream=False,
        model_name=str(runtime_config.model or DEFAULT_MODEL),
        base_url=str(runtime_config.base_url or ""),
        api_key=str(runtime_config.api_key or ""),
        timeout_seconds=float(runtime_config.timeout_seconds),
        retry_count=int(runtime_config.retries),
        temperature=float(runtime_config.temperature),
        reasoning_effort=str(runtime_config.reasoning_effort or "").strip(),
        trace_out=trace_out,
        trace_label=f"context:{root_key}",
        validator=_sanitize_and_validate_post_context_ai_result,
        request_gate=request_gate,
    )
    if not isinstance(parsed, dict):
        raise RuntimeError("ai_context_invalid_json_root")
    mentions = _normalize_mentions(parsed)
    base_entities = build_assertion_entities(mentions)
    match_result = _resolve_context_entities(
        engine_or_conn,
        post_uid=str(post.post_uid or "").strip(),
        mentions=mentions,
        source_text_excerpt=_clip_text(post.raw_text, limit=_TEXT_EXCERPT_LIMIT),
    )
    entities = match_result.entities or base_entities
    processed_at = now_str()
    logger.info(
        "[ai_context] done post_uid=%s mentions=%s entities=%s model=%s",
        str(post.post_uid or "").strip(),
        len(mentions),
        len(entities),
        str(runtime_config.model or "").strip(),
    )
    return PostContextResult(
        model=str(runtime_config.model or "").strip(),
        prompt_version=POST_CONTEXT_PROMPT_VERSION,
        processed_at=processed_at,
        mentions=mentions,
        entities=entities,
        entity_match_result=EntityMatchResult(
            entities=entities,
            relation_candidates=match_result.relation_candidates,
            alias_task_keys=match_result.alias_task_keys,
            alias_task_samples=match_result.alias_task_samples,
        ),
    )


__all__ = [
    "POST_CONTEXT_PROMPT_VERSION",
    "PostContextResult",
    "extract_post_context_result",
    "extract_stock_entity_keys_from_entities",
    "post_context_ai_runtime_config_from_env",
]
