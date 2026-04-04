from __future__ import annotations

from alphavault.domains.stock.keys import normalize_stock_key, stock_value
from alphavault.worker.redis_queue import try_get_redis

ENTITY_MATCH_STOCK_DICT_KEY = "av:entity_match:stock_dict"
_NAME_FIELD_PREFIX = "name:"
_ALIAS_FIELD_PREFIX = "alias:"


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def normalize_entity_match_lookup_text(value: object) -> str:
    return _clean_text(value).casefold()


def _unique_clean_texts(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = _clean_text(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _name_field(official_name: str) -> str:
    official_name_norm = normalize_entity_match_lookup_text(official_name)
    if not official_name_norm:
        return ""
    return f"{_NAME_FIELD_PREFIX}{official_name_norm}"


def _alias_field(alias_key: str) -> str:
    alias_value = stock_value(normalize_stock_key(alias_key))
    alias_norm = normalize_entity_match_lookup_text(alias_value)
    if not alias_norm:
        return ""
    return f"{_ALIAS_FIELD_PREFIX}{alias_norm}"


def _resolve_shadow_payload(
    items: dict[str, str],
    *,
    field_builder,
) -> dict[str, str]:
    stock_keys_by_field: dict[str, set[str]] = {}
    for raw_text, raw_stock_key in items.items():
        field = field_builder(raw_text)
        stock_key = normalize_stock_key(raw_stock_key)
        if not field or not stock_key.startswith("stock:"):
            continue
        stock_keys_by_field.setdefault(field, set()).add(stock_key)
    return {
        field: next(iter(stock_keys))
        for field, stock_keys in stock_keys_by_field.items()
        if len(stock_keys) == 1
    }


def _set_shadow_field_best_effort(
    client,
    *,
    field: str,
    stock_key: str,
    previous_field: str = "",
) -> bool:
    if not field or not stock_key.startswith("stock:"):
        return False
    try:
        if previous_field and previous_field != field:
            client.hdel(ENTITY_MATCH_STOCK_DICT_KEY, previous_field)
        existing_value = client.hmget(ENTITY_MATCH_STOCK_DICT_KEY, [field])[0]
        existing_stock_key = normalize_stock_key(_clean_text(existing_value))
        if existing_stock_key and existing_stock_key != stock_key:
            client.hdel(ENTITY_MATCH_STOCK_DICT_KEY, field)
            return True
        client.hset(ENTITY_MATCH_STOCK_DICT_KEY, field, stock_key)
    except Exception:
        return False
    return True


def load_stock_dict_targets_best_effort(
    *,
    official_names: list[str],
    alias_texts: list[str],
) -> tuple[dict[str, str], dict[str, str]]:
    client, _queue_key = try_get_redis()
    if not client:
        return {}, {}

    request_specs: list[tuple[str, str, str]] = []
    for official_name in _unique_clean_texts(official_names):
        field = _name_field(official_name)
        if field:
            request_specs.append(("name", official_name, field))
    for alias_text in _unique_clean_texts(alias_texts):
        field = _alias_field(alias_text)
        if field:
            request_specs.append(("alias", alias_text, field))
    if not request_specs:
        return {}, {}

    try:
        values = client.hmget(
            ENTITY_MATCH_STOCK_DICT_KEY,
            [field for _kind, _text, field in request_specs],
        )
    except Exception:
        return {}, {}

    official_targets: dict[str, str] = {}
    alias_targets: dict[str, str] = {}
    for (kind, original_text, _field), raw_value in zip(
        request_specs,
        values,
        strict=False,
    ):
        stock_key = normalize_stock_key(_clean_text(raw_value))
        if not stock_key.startswith("stock:"):
            continue
        if kind == "name":
            official_targets[original_text] = stock_key
            continue
        alias_targets[original_text] = stock_key
    return official_targets, alias_targets


def sync_stock_name_shadow_dict_best_effort(
    *,
    stock_key: str,
    official_name: str,
    previous_official_name: str = "",
) -> bool:
    resolved_stock_key = normalize_stock_key(stock_key)
    field = _name_field(official_name)
    previous_field = _name_field(previous_official_name)
    if not resolved_stock_key.startswith("stock:") or not field:
        return False
    client, _queue_key = try_get_redis()
    if not client:
        return False
    return _set_shadow_field_best_effort(
        client,
        field=field,
        stock_key=resolved_stock_key,
        previous_field=previous_field,
    )


def sync_stock_alias_shadow_dict_best_effort(*, stock_key: str, alias_key: str) -> bool:
    resolved_stock_key = normalize_stock_key(stock_key)
    field = _alias_field(alias_key)
    if not resolved_stock_key.startswith("stock:") or not field:
        return False
    client, _queue_key = try_get_redis()
    if not client:
        return False
    return _set_shadow_field_best_effort(
        client,
        field=field,
        stock_key=resolved_stock_key,
    )


def replace_stock_dict_shadow_best_effort(
    *,
    official_name_targets: dict[str, str],
    alias_targets: dict[str, str],
) -> bool:
    client, _queue_key = try_get_redis()
    if not client:
        return False

    payload = _resolve_shadow_payload(
        official_name_targets,
        field_builder=_name_field,
    )
    payload.update(
        _resolve_shadow_payload(
            alias_targets,
            field_builder=_alias_field,
        )
    )

    try:
        client.delete(ENTITY_MATCH_STOCK_DICT_KEY)
        if not payload:
            return True
        try:
            client.hset(ENTITY_MATCH_STOCK_DICT_KEY, mapping=payload)
        except TypeError:
            for field, stock_key in payload.items():
                client.hset(ENTITY_MATCH_STOCK_DICT_KEY, field, stock_key)
    except Exception:
        return False
    return True


__all__ = [
    "ENTITY_MATCH_STOCK_DICT_KEY",
    "load_stock_dict_targets_best_effort",
    "normalize_entity_match_lookup_text",
    "replace_stock_dict_shadow_best_effort",
    "sync_stock_name_shadow_dict_best_effort",
    "sync_stock_alias_shadow_dict_best_effort",
]
