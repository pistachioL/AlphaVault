from __future__ import annotations

from dataclasses import dataclass

from alphavault.domains.common.json_list import parse_json_list
from alphavault.domains.stock.key_match import (
    build_grouped_key_candidates,
    canonicalize_key,
    is_stock_code_value,
    normalize_stock_code,
)
from alphavault.domains.stock.keys import (
    STOCK_KEY_PREFIX,
    normalize_stock_key,
    stock_value,
)

RELATION_TYPE_STOCK_ALIAS = "stock_alias"
RELATION_LABEL_ALIAS = "alias_of"


@dataclass(frozen=True)
class StockObjectIndex:
    object_key_by_member: dict[str, str]
    member_keys_by_object_key: dict[str, set[str]]
    display_name_by_object_key: dict[str, str]
    search_text_by_object_key: dict[str, str]

    def resolve(self, stock_key: str) -> str:
        key = normalize_stock_key(stock_key)
        if not key:
            return ""
        return str(self.object_key_by_member.get(key) or key).strip()

    def display_name(self, stock_key: str) -> str:
        key = self.resolve(stock_key)
        return str(self.display_name_by_object_key.get(key) or stock_value(key)).strip()

    def page_title(self, stock_key: str) -> str:
        key = self.resolve(stock_key)
        display_name = self.display_name(key)
        value = stock_value(key)
        if (
            display_name
            and value
            and display_name != value
            and is_stock_code_value(value)
        ):
            return f"{display_name} ({value})"
        return display_name or value

    def search_text(self, stock_key: str) -> str:
        key = self.resolve(stock_key)
        return str(
            self.search_text_by_object_key.get(key) or self.page_title(key)
        ).strip()


def build_stock_object_index(
    assertions: list[dict[str, object]],
    *,
    stock_relations: list[dict[str, object]] | None = None,
    ai_alias_map: dict[str, str] | None = None,
) -> StockObjectIndex:
    enriched = ensure_stock_columns(assertions)
    if not enriched:
        return StockObjectIndex({}, {}, {}, {})

    (
        grouped_counts,
        members_by_canon,
        stock_name_by_code,
        stock_key_to_code,
        stock_name_to_code,
    ) = build_grouped_key_candidates(enriched)
    if not grouped_counts:
        return StockObjectIndex({}, {}, {}, {})

    stock_canons = [
        str(item).strip()
        for item in grouped_counts.keys()
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
    assertions: list[dict[str, object]],
    *,
    stock_key: str,
    stock_relations: list[dict[str, object]] | None = None,
    ai_alias_map: dict[str, str] | None = None,
) -> str:
    return build_stock_object_index(
        assertions,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    ).resolve(stock_key)


def filter_assertions_for_stock_object(
    assertions: list[dict[str, object]],
    *,
    stock_key: str,
    stock_relations: list[dict[str, object]] | None = None,
    ai_alias_map: dict[str, str] | None = None,
    stock_index: StockObjectIndex | None = None,
) -> list[dict[str, object]]:
    enriched = ensure_stock_columns(assertions)
    if not enriched:
        return []
    index = stock_index or build_stock_object_index(
        enriched,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    )
    target_key = index.resolve(stock_key)
    if not target_key:
        return []
    member_keys = index.member_keys_by_object_key.get(target_key)
    if not member_keys:
        member_keys = {target_key}
    return [
        dict(row)
        for row in enriched
        if str(row.get("entity_key") or "").strip() in member_keys
    ]


def build_stock_search_rows(
    assertions: list[dict[str, object]],
    *,
    stock_relations: list[dict[str, object]] | None = None,
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
                "label": index.page_title(object_key),
                "search_text": index.search_text(object_key),
            }
        )
    return rows


def pick_unresolved_stock_alias_keys(
    assertions: list[dict[str, object]],
    *,
    stock_relations: list[dict[str, object]] | None = None,
    alias_keys: list[str] | None = None,
    base_index: StockObjectIndex | None = None,
) -> list[str]:
    enriched = ensure_stock_columns(assertions)
    if not enriched:
        return []
    index = base_index or build_stock_object_index(
        enriched, stock_relations=stock_relations
    )
    return _pick_unresolved_alias_keys(
        enriched,
        base_index=index,
        alias_keys=alias_keys,
    )


def ensure_stock_columns(
    assertions: list[dict[str, object]],
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for row in assertions:
        payload = dict(row)
        payload["stock_codes"] = parse_json_list(payload.get("stock_codes"))
        payload["stock_names"] = parse_json_list(payload.get("stock_names"))
        if not isinstance(payload.get("match_keys"), list):
            payload["match_keys"] = _build_stock_match_keys(
                resolved_entity_key=payload.get("resolved_entity_key"),
                entity_key=payload.get("entity_key"),
                stock_codes=payload.get("stock_codes"),
            )
        out.append(payload)
    return out


def _build_stock_match_keys(
    *, resolved_entity_key: object, entity_key: object, stock_codes: object
) -> list[str]:
    keys: list[str] = []
    resolved_key = normalize_stock_key(str(resolved_entity_key or "").strip())
    if resolved_key:
        keys.append(resolved_key)
    row_entity_key = normalize_stock_key(str(entity_key or "").strip())
    if row_entity_key:
        keys.append(row_entity_key)
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
    stock_relations: list[dict[str, object]] | None,
) -> list[dict[str, str]]:
    if not stock_relations:
        return []
    rows: list[dict[str, str]] = []
    for row in stock_relations:
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
    assertions: list[dict[str, object]],
    *,
    base_index: StockObjectIndex,
    alias_keys: list[str] | None,
) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    raw_candidates = (
        [str(key or "").strip() for key in alias_keys]
        if alias_keys
        else [str(row.get("entity_key") or "").strip() for row in assertions]
    )

    for raw_key in raw_candidates:
        if not raw_key:
            continue
        if (not alias_keys) and (not raw_key.startswith(STOCK_KEY_PREFIX)):
            continue
        stock_key = normalize_stock_key(raw_key)
        if not stock_key or not stock_key.startswith(STOCK_KEY_PREFIX):
            continue
        value = stock_value(stock_key)
        if ":" in value:
            continue
        if is_stock_code_value(value):
            continue
        if base_index.resolve(stock_key) != stock_key:
            continue
        if stock_key in seen:
            continue
        seen.add(stock_key)
        candidates.append(stock_key)
    return candidates


def _choose_object_key(component: set[str], grouped_counts: dict[str, int]) -> str:
    code_members = sorted(
        key for key in component if is_stock_code_value(stock_value(key))
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
            stock_value(member)
            for member in member_keys
            if not is_stock_code_value(stock_value(member))
        ),
        key=lambda value: (-len(value), value),
    )
    if non_code_members:
        return non_code_members[0]
    return stock_value(object_key)


def _build_search_text(
    object_key: str, member_keys: set[str], display_name: str
) -> str:
    parts = [display_name, object_key, stock_value(object_key)]
    parts.extend(sorted(stock_value(member) for member in member_keys))
    seen: set[str] = set()
    out: list[str] = []
    for item in parts:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return " ".join(out)


__all__ = [
    "StockObjectIndex",
    "build_stock_object_index",
    "build_stock_search_rows",
    "ensure_stock_columns",
    "filter_assertions_for_stock_object",
    "pick_unresolved_stock_alias_keys",
    "resolve_stock_object_key",
]
