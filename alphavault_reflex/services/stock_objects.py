from __future__ import annotations

from dataclasses import dataclass
import os

import pandas as pd

from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
    _call_ai_with_litellm,
)
from alphavault.ai.topic_cluster_suggest import ai_is_configured
from alphavault.constants import (
    ENV_AI_API_KEY,
    ENV_AI_API_MODE,
    ENV_AI_BASE_URL,
    ENV_AI_MODEL,
    ENV_AI_REASONING_EFFORT,
    ENV_AI_RETRIES,
    ENV_AI_TEMPERATURE,
    ENV_AI_TIMEOUT_SEC,
)
from alphavault.ui.follow_pages_key_match import (
    build_grouped_key_candidates,
    canonicalize_key,
    is_stock_code_value,
    normalize_stock_code,
    parse_json_list,
)
from alphavault_reflex.services.research_models import canonical_stock_key

STOCK_KEY_PREFIX = "stock:"
RELATION_TYPE_STOCK_ALIAS = "stock_alias"
RELATION_LABEL_ALIAS = "alias_of"
MAX_AI_ALIAS_CANDIDATES = 5


@dataclass(frozen=True)
class AiRuntimeConfig:
    api_key: str
    model: str
    base_url: str
    api_mode: str
    temperature: float
    reasoning_effort: str
    timeout_seconds: float
    retries: int


@dataclass(frozen=True)
class StockObjectIndex:
    object_key_by_member: dict[str, str]
    member_keys_by_object_key: dict[str, set[str]]
    display_name_by_object_key: dict[str, str]
    search_text_by_object_key: dict[str, str]

    def resolve(self, stock_key: str) -> str:
        key = _normalize_stock_key(stock_key)
        if not key:
            return ""
        return str(self.object_key_by_member.get(key) or key).strip()

    def display_name(self, stock_key: str) -> str:
        key = self.resolve(stock_key)
        return str(
            self.display_name_by_object_key.get(key) or _stock_value(key)
        ).strip()

    def header_title(self, stock_key: str) -> str:
        key = self.resolve(stock_key)
        display_name = self.display_name(key)
        stock_value = _stock_value(key)
        if (
            display_name
            and stock_value
            and display_name != stock_value
            and is_stock_code_value(stock_value)
        ):
            return f"{display_name} ({stock_value})"
        return display_name or stock_value

    def search_text(self, stock_key: str) -> str:
        key = self.resolve(stock_key)
        return str(
            self.search_text_by_object_key.get(key) or self.header_title(key)
        ).strip()


def build_stock_object_index(
    assertions: pd.DataFrame,
    *,
    stock_relations: pd.DataFrame | None = None,
    ai_alias_map: dict[str, str] | None = None,
) -> StockObjectIndex:
    enriched = _ensure_stock_columns(assertions)
    if enriched.empty:
        return StockObjectIndex({}, {}, {}, {})

    (
        grouped_counts,
        members_by_canon,
        stock_name_by_code,
        stock_key_to_code,
        stock_name_to_code,
    ) = build_grouped_key_candidates(enriched)
    if grouped_counts.empty:
        return StockObjectIndex({}, {}, {}, {})

    stock_canons = [
        str(item).strip()
        for item in grouped_counts.index.tolist()
        if str(item).strip().startswith(STOCK_KEY_PREFIX)
    ]
    if not stock_canons:
        return StockObjectIndex({}, {}, {}, {})

    parent = {key: key for key in stock_canons}

    def _find(key: str) -> str:
        root = parent.setdefault(key, key)
        while parent[root] != root:
            root = parent[root]
        while parent[key] != key:
            nxt = parent[key]
            parent[key] = root
            key = nxt
        return root

    def _union(left: str, right: str) -> None:
        left_root = _find(left)
        right_root = _find(right)
        if left_root == right_root:
            return
        parent[right_root] = left_root

    for row in _iter_stock_alias_relations(stock_relations):
        left = canonicalize_key(
            str(row.get("left_key") or "").strip(),
            stock_key_to_code=stock_key_to_code,
            stock_name_to_code=stock_name_to_code,
        )
        right = canonicalize_key(
            str(row.get("right_key") or "").strip(),
            stock_key_to_code=stock_key_to_code,
            stock_name_to_code=stock_name_to_code,
        )
        if left in parent and right in parent:
            _union(left, right)

    for alias_key, target_key in (ai_alias_map or {}).items():
        left = canonicalize_key(
            str(alias_key or "").strip(),
            stock_key_to_code=stock_key_to_code,
            stock_name_to_code=stock_name_to_code,
        )
        right = canonicalize_key(
            str(target_key or "").strip(),
            stock_key_to_code=stock_key_to_code,
            stock_name_to_code=stock_name_to_code,
        )
        if left in parent and right in parent:
            _union(left, right)

    display_names_by_code_key = {
        f"{STOCK_KEY_PREFIX}{normalize_stock_code(code)}": str(name or "").strip()
        for code, name in stock_name_by_code.items()
        if str(code or "").strip() and str(name or "").strip()
    }

    canons_by_root: dict[str, set[str]] = {}
    for canon in stock_canons:
        canons_by_root.setdefault(_find(canon), set()).add(canon)

    object_key_by_member: dict[str, str] = {}
    member_keys_by_object_key: dict[str, set[str]] = {}
    display_name_by_object_key: dict[str, str] = {}
    search_text_by_object_key: dict[str, str] = {}

    for component in canons_by_root.values():
        object_key = _choose_object_key(component, grouped_counts)
        member_keys: set[str] = set()
        for canon in component:
            member_keys |= set(members_by_canon.get(canon, set()))
            member_keys.add(canon)
        for stock_name, code in stock_name_to_code.items():
            code_key = (
                f"{STOCK_KEY_PREFIX}{normalize_stock_code(str(code or '').strip())}"
            )
            if code_key in component and str(stock_name or "").strip():
                member_keys.add(f"{STOCK_KEY_PREFIX}{str(stock_name).strip()}")
        object_key_by_member[object_key] = object_key
        for member in member_keys:
            object_key_by_member[str(member).strip()] = object_key
        member_keys_by_object_key[object_key] = member_keys
        display_name_by_object_key[object_key] = _choose_display_name(
            object_key,
            member_keys,
            display_names_by_code_key,
        )
        search_text_by_object_key[object_key] = _build_search_text(
            object_key,
            member_keys,
            display_name_by_object_key[object_key],
        )

    return StockObjectIndex(
        object_key_by_member=object_key_by_member,
        member_keys_by_object_key=member_keys_by_object_key,
        display_name_by_object_key=display_name_by_object_key,
        search_text_by_object_key=search_text_by_object_key,
    )


def resolve_stock_object_key(
    assertions: pd.DataFrame,
    *,
    stock_key: str,
    stock_relations: pd.DataFrame | None = None,
    ai_alias_map: dict[str, str] | None = None,
) -> str:
    return build_stock_object_index(
        assertions,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    ).resolve(stock_key)


def filter_assertions_for_stock_object(
    assertions: pd.DataFrame,
    *,
    stock_key: str,
    stock_relations: pd.DataFrame | None = None,
    ai_alias_map: dict[str, str] | None = None,
    stock_index: StockObjectIndex | None = None,
) -> pd.DataFrame:
    enriched = _ensure_stock_columns(assertions)
    if enriched.empty:
        return enriched.head(0).copy()
    index = stock_index or build_stock_object_index(
        enriched,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    )
    target_key = index.resolve(stock_key)
    if not target_key:
        return enriched.head(0).copy()
    member_keys = index.member_keys_by_object_key.get(target_key)
    if not member_keys:
        member_keys = {target_key}
    if "topic_key" not in enriched.columns:
        return enriched.head(0).copy()
    topics = enriched["topic_key"].fillna("").astype(str).str.strip()
    return enriched[topics.isin(member_keys)].copy()


def build_stock_search_rows(
    assertions: pd.DataFrame,
    *,
    stock_relations: pd.DataFrame | None = None,
    ai_alias_map: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    index = build_stock_object_index(
        assertions,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    )
    ranked_keys = sorted(
        index.member_keys_by_object_key.keys(),
        key=lambda item: (index.display_name(item), item),
    )
    rows: list[dict[str, str]] = []
    for object_key in ranked_keys:
        rows.append(
            {
                "entity_type": "stock",
                "entity_key": object_key,
                "label": index.header_title(object_key),
                "search_text": index.search_text(object_key),
            }
        )
    return rows


def pick_unresolved_stock_alias_keys(
    assertions: pd.DataFrame,
    *,
    stock_relations: pd.DataFrame | None = None,
    alias_keys: list[str] | None = None,
) -> list[str]:
    enriched = _ensure_stock_columns(assertions)
    if enriched.empty:
        return []
    base_index = build_stock_object_index(enriched, stock_relations=stock_relations)
    return _pick_unresolved_alias_keys(
        enriched,
        base_index=base_index,
        alias_keys=alias_keys,
    )


def build_ai_stock_alias_map(
    assertions: pd.DataFrame,
    *,
    stock_relations: pd.DataFrame | None = None,
    alias_keys: list[str] | None = None,
    runtime_config: AiRuntimeConfig | None = None,
    max_alias_keys: int | None = None,
    stats_out: dict[str, int] | None = None,
) -> dict[str, str]:
    enriched = _ensure_stock_columns(assertions)
    if enriched.empty:
        return {}
    ok, _err = ai_is_configured()
    if not ok:
        return {}

    base_index = build_stock_object_index(enriched, stock_relations=stock_relations)
    unresolved = _pick_unresolved_alias_keys(
        enriched,
        base_index=base_index,
        alias_keys=alias_keys,
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
    remaining_aliases = max(0, len(unresolved) - len(unresolved_to_process))
    if stats_out is not None:
        stats_out["processed_aliases"] = int(len(unresolved_to_process))
        stats_out["remaining_aliases"] = int(remaining_aliases)

    out: dict[str, str] = {}
    for alias_key in unresolved_to_process:
        target_key = _resolve_single_alias_with_ai(
            enriched,
            alias_key=alias_key,
            base_index=base_index,
            runtime_config=runtime_config,
        )
        if target_key:
            out[alias_key] = target_key
    return out


def _ensure_stock_columns(assertions: pd.DataFrame) -> pd.DataFrame:
    if assertions.empty:
        return assertions.copy()
    out = assertions.copy()
    if "stock_codes" not in out.columns:
        raw_codes = (
            out["stock_codes_json"]
            if "stock_codes_json" in out.columns
            else pd.Series(["[]"] * len(out), index=out.index)
        )
        out["stock_codes"] = raw_codes.apply(parse_json_list)
    if "stock_names" not in out.columns:
        raw_names = (
            out["stock_names_json"]
            if "stock_names_json" in out.columns
            else pd.Series(["[]"] * len(out), index=out.index)
        )
        out["stock_names"] = raw_names.apply(parse_json_list)
    if "match_keys" not in out.columns:
        out["match_keys"] = out.apply(
            lambda row: _build_stock_match_keys(
                topic_key=row.get("topic_key"),
                stock_codes=row.get("stock_codes"),
            ),
            axis=1,
        )
    return out


def _build_stock_match_keys(*, topic_key: object, stock_codes: object) -> list[str]:
    keys: list[str] = []
    topic = _normalize_stock_key(str(topic_key or "").strip())
    if topic:
        keys.append(topic)
    codes = stock_codes if isinstance(stock_codes, list) else []
    for raw in codes:
        code = normalize_stock_code(str(raw or "").strip())
        if not code:
            continue
        keys.append(f"{STOCK_KEY_PREFIX}{code}")
    seen: set[str] = set()
    out: list[str] = []
    for key in keys:
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _iter_stock_alias_relations(
    stock_relations: pd.DataFrame | None,
) -> list[dict[str, str]]:
    if stock_relations is None or stock_relations.empty:
        return []
    rows: list[dict[str, str]] = []
    for _, row in stock_relations.iterrows():
        relation_type = str(row.get("relation_type") or "").strip()
        relation_label = str(row.get("relation_label") or "").strip()
        if (
            relation_type != RELATION_TYPE_STOCK_ALIAS
            and relation_label != RELATION_LABEL_ALIAS
        ):
            continue
        rows.append(
            {
                "left_key": str(row.get("left_key") or "").strip(),
                "right_key": str(row.get("right_key") or "").strip(),
            }
        )
    return rows


def _pick_unresolved_alias_keys(
    assertions: pd.DataFrame,
    *,
    base_index: StockObjectIndex,
    alias_keys: list[str] | None,
) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    raw_candidates = (
        [str(key or "").strip() for key in alias_keys]
        if alias_keys
        else [
            str(item or "").strip()
            for item in assertions.get("topic_key", pd.Series(dtype=str)).tolist()
        ]
    )

    for raw_key in raw_candidates:
        if not raw_key:
            continue
        if (not alias_keys) and (not raw_key.startswith(STOCK_KEY_PREFIX)):
            continue
        stock_key = _normalize_stock_key(raw_key)
        if not stock_key or not stock_key.startswith(STOCK_KEY_PREFIX):
            continue
        stock_value = _stock_value(stock_key)
        if ":" in stock_value:
            continue
        if is_stock_code_value(stock_value):
            continue
        if base_index.resolve(stock_key) != stock_key:
            continue
        if stock_key in seen:
            continue
        seen.add(stock_key)
        candidates.append(stock_key)
    return candidates


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

    config = runtime_config or _get_ai_runtime_config()
    alias_value = _stock_value(alias_key)
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
    alias_value = _stock_value(alias_key)
    if not alias_value:
        return assertions.head(0).copy()

    def _matches(row: pd.Series) -> bool:
        topic_key = _normalize_stock_key(str(row.get("topic_key") or "").strip())
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
        if not is_stock_code_value(_stock_value(object_key)):
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


def _get_ai_runtime_config() -> AiRuntimeConfig:
    return AiRuntimeConfig(
        api_key=os.getenv(ENV_AI_API_KEY, "").strip(),
        model=os.getenv(ENV_AI_MODEL, DEFAULT_MODEL).strip() or DEFAULT_MODEL,
        base_url=os.getenv(ENV_AI_BASE_URL, "").strip(),
        api_mode=os.getenv(ENV_AI_API_MODE, DEFAULT_AI_MODE).strip() or DEFAULT_AI_MODE,
        temperature=float(
            os.getenv(ENV_AI_TEMPERATURE, str(DEFAULT_AI_TEMPERATURE)).strip()
            or DEFAULT_AI_TEMPERATURE
        ),
        reasoning_effort=os.getenv(
            ENV_AI_REASONING_EFFORT,
            DEFAULT_AI_REASONING_EFFORT,
        ).strip()
        or DEFAULT_AI_REASONING_EFFORT,
        timeout_seconds=float(os.getenv(ENV_AI_TIMEOUT_SEC, "60").strip() or 60),
        retries=int(
            os.getenv(ENV_AI_RETRIES, str(DEFAULT_AI_RETRY_COUNT)).strip()
            or DEFAULT_AI_RETRY_COUNT
        ),
    )


def _choose_object_key(component: set[str], grouped_counts: pd.Series) -> str:
    code_members = sorted(
        key for key in component if is_stock_code_value(_stock_value(key))
    )
    if code_members:
        return code_members[0]
    return sorted(
        component,
        key=lambda key: (-int(grouped_counts.get(key, 0)), key),
    )[0]


def _choose_display_name(
    object_key: str,
    member_keys: set[str],
    display_names_by_code_key: dict[str, str],
) -> str:
    display_name = str(display_names_by_code_key.get(object_key) or "").strip()
    if display_name:
        return display_name
    non_code_members = sorted(
        (
            _stock_value(member)
            for member in member_keys
            if not is_stock_code_value(_stock_value(member))
        ),
        key=lambda value: (-len(value), value),
    )
    if non_code_members:
        return non_code_members[0]
    return _stock_value(object_key)


def _build_search_text(
    object_key: str, member_keys: set[str], display_name: str
) -> str:
    parts = [display_name, object_key, _stock_value(object_key)]
    parts.extend(sorted(_stock_value(member) for member in member_keys))
    seen: set[str] = set()
    out: list[str] = []
    for item in parts:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return " ".join(out)


def _row_stock_member_keys(row: pd.Series) -> list[str]:
    keys: list[str] = []
    topic_key = _normalize_stock_key(str(row.get("topic_key") or "").strip())
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


def _normalize_stock_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    key = text if text.startswith(STOCK_KEY_PREFIX) else f"{STOCK_KEY_PREFIX}{text}"
    return canonical_stock_key(key)


def _stock_value(stock_key: str) -> str:
    key = str(stock_key or "").strip()
    if key.startswith(STOCK_KEY_PREFIX):
        return key[len(STOCK_KEY_PREFIX) :].strip()
    return key


__all__ = [
    "StockObjectIndex",
    "build_ai_stock_alias_map",
    "build_stock_object_index",
    "build_stock_search_rows",
    "filter_assertions_for_stock_object",
    "pick_unresolved_stock_alias_keys",
    "resolve_stock_object_key",
]
