from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Callable

import pandas as pd

from alphavault.ai.analyze import _call_ai_with_litellm
from alphavault.ai.topic_cluster_suggest import ai_is_configured
from alphavault.domains.stock.key_match import is_stock_code_value, normalize_stock_code
from alphavault.domains.stock.keys import (
    STOCK_KEY_PREFIX,
    normalize_stock_key,
    stock_value,
)
from alphavault.domains.stock.object_index import (
    StockObjectIndex,
    build_stock_object_index,
    ensure_stock_columns,
    pick_unresolved_stock_alias_keys,
)

from .runtime_config import AiRuntimeConfig, ai_runtime_config_from_env


MAX_AI_ALIAS_CANDIDATES = 5


def build_ai_stock_alias_map(
    assertions: pd.DataFrame,
    *,
    stock_relations: pd.DataFrame | None = None,
    alias_keys: list[str] | None = None,
    runtime_config: AiRuntimeConfig | None = None,
    max_alias_keys: int | None = None,
    stats_out: dict[str, int] | None = None,
    ai_max_inflight: int = 1,
    should_continue: Callable[[], bool] | None = None,
    acquire_low_priority_slot: Callable[[], bool] | None = None,
    release_low_priority_slot: Callable[[], None] | None = None,
    attempted_aliases_out: list[str] | None = None,
) -> dict[str, str]:
    enriched = ensure_stock_columns(assertions)
    if enriched.empty:
        return {}
    ok, _err = ai_is_configured()
    if not ok:
        return {}

    base_index = build_stock_object_index(enriched, stock_relations=stock_relations)
    unresolved = pick_unresolved_stock_alias_keys(
        enriched,
        stock_relations=stock_relations,
        alias_keys=alias_keys,
        base_index=base_index,
    )
    unresolved_total = len(unresolved)
    if stats_out is not None:
        stats_out["unresolved_total"] = int(unresolved_total)
    if not unresolved:
        if stats_out is not None:
            stats_out["processed_aliases"] = 0
            stats_out["remaining_aliases"] = 0
        return {}

    unresolved_to_process = unresolved
    if max_alias_keys is not None and int(max_alias_keys) > 0:
        unresolved_to_process = unresolved[: int(max_alias_keys)]
    should_continue_fn = should_continue or (lambda: True)
    acquire_slot_fn = acquire_low_priority_slot or (lambda: True)
    release_slot_fn = release_low_priority_slot or (lambda: None)
    effective_max_inflight = min(
        max(1, int(ai_max_inflight)),
        max(1, int(len(unresolved_to_process))),
    )

    out: dict[str, str] = {}
    attempted_aliases: list[str] = []
    next_submit_index = 0

    def _can_submit_more() -> bool:
        try:
            return bool(should_continue_fn())
        except Exception:
            return False

    def _can_acquire_slot() -> bool:
        try:
            return bool(acquire_slot_fn())
        except Exception:
            return False

    def _release_slot() -> None:
        try:
            release_slot_fn()
        except Exception:
            return

    def _submit_more(
        executor: ThreadPoolExecutor,
        futures: dict[Future[str], str],
    ) -> None:
        nonlocal next_submit_index
        while next_submit_index < len(unresolved_to_process) and len(futures) < int(
            effective_max_inflight
        ):
            if not _can_submit_more():
                break
            alias_key = str(unresolved_to_process[next_submit_index] or "").strip()
            if not alias_key:
                next_submit_index += 1
                continue
            if not _can_acquire_slot():
                break
            next_submit_index += 1
            attempted_aliases.append(alias_key)

            def _run_one(target_alias_key: str) -> str:
                try:
                    return _resolve_single_alias_with_ai(
                        enriched,
                        alias_key=target_alias_key,
                        base_index=base_index,
                        runtime_config=runtime_config,
                    )
                finally:
                    _release_slot()

            try:
                future = executor.submit(
                    _run_one,
                    alias_key,
                )
            except Exception:
                _release_slot()
                raise
            futures[future] = alias_key

    with ThreadPoolExecutor(max_workers=int(effective_max_inflight)) as executor:
        inflight_futures: dict[Future[str], str] = {}
        _submit_more(executor, inflight_futures)
        while inflight_futures:
            done, _pending = wait(
                set(inflight_futures.keys()),
                return_when=FIRST_COMPLETED,
            )
            for done_future in done:
                alias_key = inflight_futures.pop(done_future, "")
                if not alias_key:
                    continue
                target_key = ""
                try:
                    target_key = str(done_future.result() or "").strip()
                except Exception:
                    target_key = ""
                if target_key:
                    out[alias_key] = target_key
            _submit_more(executor, inflight_futures)

    processed_aliases = int(len(attempted_aliases))
    remaining_aliases = max(0, int(len(unresolved) - processed_aliases))
    if stats_out is not None:
        stats_out["processed_aliases"] = int(processed_aliases)
        stats_out["remaining_aliases"] = int(remaining_aliases)
    if attempted_aliases_out is not None:
        attempted_aliases_out.extend(attempted_aliases)
    return out


def _resolve_single_alias_with_ai(
    assertions: pd.DataFrame,
    *,
    alias_key: str,
    base_index: StockObjectIndex,
    runtime_config: AiRuntimeConfig | None = None,
) -> str:
    alias_rows = _filter_alias_rows(assertions, alias_key=alias_key)
    if alias_rows.empty:
        return ""
    candidates = _build_ai_alias_candidates(
        assertions, alias_rows=alias_rows, base_index=base_index
    )
    if not candidates:
        return ""

    config = runtime_config or ai_runtime_config_from_env(timeout_seconds_default=60.0)
    alias_value = stock_value(alias_key)
    alias_examples = _format_alias_examples(alias_rows)
    candidate_lines = [
        (
            f"- object_key={item['object_key']}; name={item['display_name']}; "
            f"author_overlap={item['author_overlap']}; sector_overlap={item['sector_overlap']}; "
            f"recent_summary={item['recent_summary']}"
        )
        for item in candidates
    ]
    prompt = f"""
你是个股简称判断助手。请判断 alias_key 是否指向候选列表里的某个正式个股对象。

alias_key: {alias_key}
alias_text: {alias_value}

alias_examples:
{alias_examples}

candidates:
{chr(10).join(candidate_lines)}

输出严格 JSON：
{{
  "target_object_key": "候选里的 object_key，没把握就填空字符串",
  "ai_reason": "一句话理由"
}}

规则：
- 只能从 candidates 里选，禁止编造。
- 不要只因为字符串像就下结论，要看作者、板块、上下文摘要。
- 没把握就返回空字符串。
""".strip()

    parsed = _call_ai_with_litellm(
        prompt=prompt,
        api_mode=config.api_mode,
        ai_stream=False,
        model_name=config.model,
        base_url=config.base_url,
        api_key=config.api_key,
        timeout_seconds=float(config.timeout_seconds),
        retry_count=int(config.retries),
        temperature=float(config.temperature),
        reasoning_effort=str(config.reasoning_effort),
        trace_out=None,
        trace_label=f"stock_alias_resolve:{alias_key}",
    )
    if not isinstance(parsed, dict):
        return ""
    target_object_key = str(parsed.get("target_object_key") or "").strip()
    valid_keys = {item["object_key"] for item in candidates}
    if target_object_key not in valid_keys:
        return ""
    return target_object_key


def _filter_alias_rows(assertions: pd.DataFrame, *, alias_key: str) -> pd.DataFrame:
    alias_value = stock_value(alias_key)
    if not alias_value:
        return assertions.head(0).copy()

    def _matches(row: pd.Series) -> bool:
        topic_key = normalize_stock_key(str(row.get("topic_key") or "").strip())
        if topic_key == alias_key:
            return True
        for name in row.get("stock_names") or []:
            if str(name or "").strip() == alias_value:
                return True
        return False

    mask = assertions.apply(_matches, axis=1)
    return assertions[mask].copy()


def _build_ai_alias_candidates(
    assertions: pd.DataFrame,
    *,
    alias_rows: pd.DataFrame,
    base_index: StockObjectIndex,
) -> list[dict[str, str]]:
    alias_authors = _collect_overlap_values(alias_rows.get("author"))
    alias_sectors = _collect_sector_values(alias_rows)
    rows: list[dict[str, str]] = []
    scored_rows: list[tuple[int, dict[str, str]]] = []
    for object_key in sorted(base_index.member_keys_by_object_key.keys()):
        if not is_stock_code_value(stock_value(object_key)):
            continue
        object_rows = _filter_rows_for_object(
            assertions, base_index=base_index, object_key=object_key
        )
        if object_rows.empty:
            continue
        author_overlap = len(
            alias_authors & _collect_overlap_values(object_rows.get("author"))
        )
        sector_overlap = len(alias_sectors & _collect_sector_values(object_rows))
        if author_overlap <= 0 and sector_overlap <= 0:
            continue
        rows.append(
            {
                "object_key": object_key,
                "display_name": base_index.display_name(object_key),
                "author_overlap": str(author_overlap),
                "sector_overlap": str(sector_overlap),
                "recent_summary": _latest_summary(object_rows),
            }
        )
        scored_rows.append((author_overlap + sector_overlap, rows[-1]))
    scored_rows.sort(
        key=lambda item: (
            -int(item[0]),
            item[1]["display_name"],
            item[1]["object_key"],
        )
    )
    return [row for _, row in scored_rows[:MAX_AI_ALIAS_CANDIDATES]]


def _filter_rows_for_object(
    assertions: pd.DataFrame,
    *,
    base_index: StockObjectIndex,
    object_key: str,
) -> pd.DataFrame:
    def _matches(row: pd.Series) -> bool:
        for member in _row_stock_member_keys(row):
            if base_index.resolve(member) == object_key:
                return True
        return False

    mask = assertions.apply(_matches, axis=1)
    return assertions[mask].copy()


def _collect_overlap_values(series: object) -> set[str]:
    if series is None:
        return set()
    items: list[object]
    if isinstance(series, pd.Series):
        items = series.tolist()
    elif isinstance(series, (list, tuple, set)):
        items = list(series)
    else:
        return set()
    values: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if text:
            values.add(text)
    return values


def _collect_sector_values(frame: pd.DataFrame) -> set[str]:
    values: set[str] = set()
    for item in frame.get("cluster_keys", pd.Series(dtype=object)).tolist():
        if isinstance(item, list):
            for raw in item:
                text = str(raw or "").strip()
                if text:
                    values.add(text)
        else:
            text = str(item or "").strip()
            if text:
                values.add(text)
    return values


def _latest_summary(frame: pd.DataFrame) -> str:
    if frame.empty or "summary" not in frame.columns:
        return ""
    view = frame.copy()
    if "created_at" in view.columns:
        view = view.sort_values(by="created_at", ascending=False, na_position="last")
    return str(view.iloc[0].get("summary") or "").strip()


def _format_alias_examples(alias_rows: pd.DataFrame) -> str:
    lines: list[str] = []
    view = alias_rows.copy()
    if "created_at" in view.columns:
        view = view.sort_values(by="created_at", ascending=False, na_position="last")
    for _, row in view.head(5).iterrows():
        lines.append(
            f"- author={str(row.get('author') or '').strip()}; "
            f"summary={str(row.get('summary') or '').strip()}; "
            f"sectors={','.join(sorted(_collect_sector_values(pd.DataFrame([row]))))}"
        )
    return "\n".join(lines) if lines else "- 无"


def _row_stock_member_keys(row: pd.Series) -> list[str]:
    keys: list[str] = []
    topic_key = normalize_stock_key(str(row.get("topic_key") or "").strip())
    if topic_key:
        keys.append(topic_key)
    for raw in row.get("stock_codes") or []:
        code = normalize_stock_code(str(raw or "").strip())
        if code:
            keys.append(f"{STOCK_KEY_PREFIX}{code}")
    for raw in row.get("stock_names") or []:
        name = str(raw or "").strip()
        if name:
            keys.append(f"{STOCK_KEY_PREFIX}{name}")
    seen: set[str] = set()
    out: list[str] = []
    for key in keys:
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


__all__ = [
    "MAX_AI_ALIAS_CANDIDATES",
    "build_ai_stock_alias_map",
]
