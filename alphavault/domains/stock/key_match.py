from __future__ import annotations

import re

from alphavault.domains.common.json_list import parse_json_list


MAX_KEY_CANDIDATES = 500

BAD_STOCK_VALUE_SEPARATORS = {",", "，", "、"}

# Keep the stock code formats aligned with alphavault.ai.tag_validate (minimal + strict).
STOCK_CODE_CN_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)$", re.IGNORECASE)
STOCK_CODE_HK_RE = re.compile(r"^\d{4,5}\.HK$", re.IGNORECASE)
STOCK_CODE_US_RE = re.compile(r"^[A-Z][A-Z0-9]{0,9}\.US$", re.IGNORECASE)
_PREFIXED_CN_STOCK_CODE_RE = re.compile(
    r"^(SH|SZ|BJ)(\d{6})(?:\.US)?$",
    re.IGNORECASE,
)


def has_bad_stock_separator(value: str) -> bool:
    s = str(value or "")
    return any(sep in s for sep in BAD_STOCK_VALUE_SEPARATORS)


def is_stock_code_value(value: str) -> bool:
    v = str(value or "").strip().upper()
    return bool(
        STOCK_CODE_CN_RE.match(v)
        or STOCK_CODE_HK_RE.match(v)
        or STOCK_CODE_US_RE.match(v)
    )


def normalize_stock_code(value: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    matched = _PREFIXED_CN_STOCK_CODE_RE.match(text)
    if matched is None:
        return text
    market, code = matched.groups()
    return f"{code}.{market}"


def split_stock_value_tokens(value: str) -> list[str]:
    # For "bad key" like "长电,紫金" -> ["长电", "紫金"]
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = re.split(r"[，,、/\s]+", raw)
    out: list[str] = []
    for p in parts:
        s = str(p or "").strip()
        if s:
            out.append(s)
    return out


def build_stock_key_to_code(
    assertions_filtered: list[dict[str, object]],
) -> dict[str, str]:
    """
    Map 'stock:<name/bad>' key -> stock code, using *same-row* co-occurrence.

    This is conservative: only map when the row has exactly 1 stock code key.
    """
    if not assertions_filtered:
        return {}

    code_counts: dict[str, dict[str, int]] = {}
    for row in assertions_filtered:
        mk = row.get("match_keys")
        if not isinstance(mk, list):
            continue
        codes: list[str] = []
        stock_keys: list[str] = []
        for raw in mk:
            s = str(raw or "").strip()
            if not s.startswith("stock:"):
                continue
            v = s[len("stock:") :].strip()
            if not v:
                continue
            stock_keys.append(f"stock:{v}")
            if is_stock_code_value(v):
                codes.append(normalize_stock_code(v))

        uniq_codes = sorted(set(codes))
        if len(uniq_codes) != 1:
            continue
        code = uniq_codes[0]

        for k in stock_keys:
            v = k[len("stock:") :].strip()
            if not v or is_stock_code_value(v):
                continue
            code_counts.setdefault(k, {})
            code_counts[k][code] = int(code_counts[k].get(code, 0)) + 1

    out: dict[str, str] = {}
    for stock_key, counts in code_counts.items():
        best = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[0][0]
        out[str(stock_key or "").strip()] = normalize_stock_code(best)
    return out


def build_stock_name_by_code(
    assertions_filtered: list[dict[str, object]],
) -> dict[str, str]:
    if not assertions_filtered:
        return {}
    counts: dict[str, dict[str, int]] = {}
    for row in assertions_filtered:
        codes = row.get("stock_codes")
        names = row.get("stock_names")
        if not isinstance(codes, list) or not isinstance(names, list):
            continue
        codes = [str(x).strip() for x in codes if str(x).strip()]
        names = [str(x).strip() for x in names if str(x).strip()]
        if len(codes) != 1 or len(names) != 1:
            continue
        code = codes[0]
        name = names[0]
        if not code or not name:
            continue
        counts.setdefault(code, {})
        counts[code][name] = int(counts[code].get(name, 0)) + 1

    out: dict[str, str] = {}
    for code, name_counts in counts.items():
        best = sorted(name_counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[0][
            0
        ]
        out[str(code).strip()] = str(best).strip()
    return out


def build_stock_name_to_code(
    assertions_filtered: list[dict[str, object]],
    *,
    stock_name_by_code: dict[str, str],
) -> dict[str, str]:
    """
    Best-effort name/alias -> code mapping.

    Sources (in order of confidence):
    - 1:1 (stock_code, stock_name) pairs from AI fields
    - inverse(stock_name_by_code)
    - entity_key stock:<alias> + stock_names[0] -> code (conservative)
    """
    counts: dict[str, dict[str, int]] = {}

    # 1) AI parsed (stock_codes, stock_names) pairs (only when both are 1:1)
    for row in assertions_filtered:
        codes = row.get("stock_codes")
        names = row.get("stock_names")
        if not isinstance(codes, list) or not isinstance(names, list):
            continue
        if len(codes) != 1 or len(names) != 1:
            continue
        code = normalize_stock_code(codes[0])
        name = str(names[0] or "").strip()
        if not code or not name:
            continue
        counts.setdefault(name, {})
        counts[name][code] = int(counts[name].get(code, 0)) + 1

    # 2) inverse(stock_name_by_code)
    for code, name in (stock_name_by_code or {}).items():
        c = normalize_stock_code(code)
        n = str(name or "").strip()
        if not c or not n:
            continue
        counts.setdefault(n, {})
        counts[n][c] = int(counts[n].get(c, 0)) + 1

    # Build a base lookup (full_name -> code) first.
    base_best_code_by_name: dict[str, str] = {}
    for name, code_counts in counts.items():
        best = sorted(code_counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[0][
            0
        ]
        base_best_code_by_name[str(name or "").strip()] = normalize_stock_code(best)

    # 3) entity_key stock:<alias> + stock_names[0] -> code (v3 often has empty stock_codes)
    for row in assertions_filtered:
        entity_key = row.get("entity_key")
        stock_names = row.get("stock_names")
        tk = str(entity_key or "").strip()
        if not tk.startswith("stock:"):
            continue
        alias = tk[len("stock:") :].strip()
        if (
            not alias
            or is_stock_code_value(alias)
            or has_bad_stock_separator(alias)
            or ":" in alias
        ):
            continue
        if not isinstance(stock_names, list):
            continue
        names = [str(x).strip() for x in stock_names if str(x).strip()]
        if len(names) != 1:
            continue
        full_name = str(names[0] or "").strip()
        code = str(base_best_code_by_name.get(full_name, "") or "").strip()
        if not code:
            continue
        counts.setdefault(alias, {})
        counts[alias][code] = int(counts[alias].get(code, 0)) + 1

    out: dict[str, str] = {}
    for name, code_counts in counts.items():
        best = sorted(code_counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[0][
            0
        ]
        out[str(name or "").strip()] = normalize_stock_code(best)
    return out


def canonicalize_key(
    raw_key: str,
    *,
    stock_key_to_code: dict[str, str],
    stock_name_to_code: dict[str, str],
) -> str:
    k = str(raw_key or "").strip()
    if not k:
        return ""
    if not k.startswith("stock:"):
        return k

    value = k[len("stock:") :].strip()
    if not value:
        return k

    # 1) Already a stock code
    if is_stock_code_value(value):
        return f"stock:{normalize_stock_code(value)}"

    # 2) Same-row mapping (most reliable, can cover bad keys too)
    mapped = str(stock_key_to_code.get(f"stock:{value}", "") or "").strip()
    if mapped:
        return f"stock:{normalize_stock_code(mapped)}"

    # 3) Clean name -> code mapping
    if not has_bad_stock_separator(value):
        mapped2 = str(stock_name_to_code.get(value, "") or "").strip()
        if mapped2:
            return f"stock:{normalize_stock_code(mapped2)}"
        return k

    # 4) Bad key: try split tokens; only map if it points to exactly 1 code
    tokens = split_stock_value_tokens(value)
    codes = sorted(
        {
            normalize_stock_code(stock_name_to_code.get(t, ""))
            for t in tokens
            if str(stock_name_to_code.get(t, "") or "").strip()
        }
    )
    if len(codes) == 1:
        return f"stock:{codes[0]}"
    return k


def format_key_label(key: str, *, stock_name_by_code: dict[str, str]) -> str:
    k = str(key or "").strip()
    if not k:
        return ""
    if k.startswith("stock:"):
        code = k[len("stock:") :].strip()
        name = str(stock_name_by_code.get(code, "") or "").strip()
        return f"个股：{name} ({code})" if name else f"个股：{code}"
    if k.startswith("industry:"):
        return f"行业：{k[len('industry:') :].strip()}"
    if k.startswith("commodity:"):
        return f"商品：{k[len('commodity:') :].strip()}"
    if k.startswith("index:"):
        return f"指数：{k[len('index:') :].strip()}"
    # Keep raw key for other types (portfolio/macro/method/...).
    return k


def key_candidates(assertions_filtered: list[dict[str, object]]) -> dict[str, int]:
    if not assertions_filtered:
        return {}
    has_match_keys = any("match_keys" in row for row in assertions_filtered)
    if has_match_keys:
        flat: list[str] = []
        for row in assertions_filtered:
            item = row.get("match_keys")
            if not isinstance(item, list):
                continue
            for k in item:
                s = str(k).strip()
                if s:
                    flat.append(s)
        match_key_counts: dict[str, int] = {}
        for key in flat:
            match_key_counts[key] = int(match_key_counts.get(key, 0)) + 1
        return match_key_counts

    entity_key_counts: dict[str, int] = {}
    for row in assertions_filtered:
        key = str(row.get("entity_key") or "").strip()
        if not key:
            continue
        entity_key_counts[key] = int(entity_key_counts.get(key, 0)) + 1
    return entity_key_counts


def build_grouped_key_candidates(
    assertions_filtered: list[dict[str, object]],
) -> tuple[
    dict[str, int], dict[str, set[str]], dict[str, str], dict[str, str], dict[str, str]
]:
    """
    Group keys to avoid stock key fragmentation:
    - stock:601899.SH + stock:紫金矿业 -> one candidate (canonical: stock:601899.SH)

    Returns:
    - grouped_counts: Series indexed by canonical key (desc by count)
    - members_by_canonical: canonical -> raw keys set
    - stock_name_by_code: code -> best name
    - stock_key_to_code: stock:<name/bad> -> code
    - stock_name_to_code: name -> code
    """
    raw_counts = key_candidates(assertions_filtered)
    if not raw_counts:
        return raw_counts, {}, {}, {}, {}

    stock_name_by_code = build_stock_name_by_code(assertions_filtered)
    stock_key_to_code = build_stock_key_to_code(assertions_filtered)
    stock_name_to_code = build_stock_name_to_code(
        assertions_filtered,
        stock_name_by_code=stock_name_by_code,
    )

    grouped: dict[str, int] = {}
    members: dict[str, set[str]] = {}
    for raw_key, count in raw_counts.items():
        raw = str(raw_key or "").strip()
        if not raw:
            continue
        canon = canonicalize_key(
            raw,
            stock_key_to_code=stock_key_to_code,
            stock_name_to_code=stock_name_to_code,
        )
        if not canon:
            continue
        grouped[canon] = int(grouped.get(canon, 0)) + int(count)
        members.setdefault(canon, set()).add(raw)
        members[canon].add(canon)

    if not grouped:
        return (
            {},
            {},
            stock_name_by_code,
            stock_key_to_code,
            stock_name_to_code,
        )

    return (
        grouped,
        members,
        stock_name_by_code,
        stock_key_to_code,
        stock_name_to_code,
    )


def filter_assertions_by_follow_key(
    assertions_filtered: list[dict[str, object]], *, follow_key: str
) -> list[dict[str, object]]:
    want = str(follow_key or "").strip()
    if not assertions_filtered or not want:
        return []

    has_match_keys = any("match_keys" in row for row in assertions_filtered)
    if not has_match_keys:
        return [
            dict(row)
            for row in assertions_filtered
            if str(row.get("entity_key") or "").strip() == want
        ]

    (
        _grouped_counts,
        members_by_canon,
        _stock_name_by_code,
        stock_key_to_code,
        stock_name_to_code,
    ) = build_grouped_key_candidates(assertions_filtered)

    canon_want = canonicalize_key(
        want,
        stock_key_to_code=stock_key_to_code,
        stock_name_to_code=stock_name_to_code,
    )

    want_keys: set[str] = set()
    for k in [want, canon_want]:
        kk = str(k or "").strip()
        if not kk:
            continue
        if kk in members_by_canon:
            want_keys |= set(members_by_canon[kk])
        else:
            want_keys.add(kk)

    # Extra safety for historical "bad stock key": stock:长电,紫金
    # Use token -> code mapping to avoid relying on exact alias strings.
    target_stock_code = ""
    if canon_want.startswith("stock:"):
        v = canon_want[len("stock:") :].strip()
        if is_stock_code_value(v):
            target_stock_code = normalize_stock_code(v)

    def _match_row(mk: object) -> bool:
        if not isinstance(mk, list):
            return False
        keys = [str(x).strip() for x in mk if str(x).strip()]
        if not keys:
            return False
        if any(k in want_keys for k in keys):
            return True
        if not target_stock_code:
            return False
        for raw in keys:
            if not raw.startswith("stock:"):
                continue
            vv = raw[len("stock:") :].strip()
            if not vv or not has_bad_stock_separator(vv):
                continue
            tokens = split_stock_value_tokens(vv)
            for t in tokens:
                mapped = str(stock_name_to_code.get(t, "") or "").strip()
                if mapped and normalize_stock_code(mapped) == target_stock_code:
                    return True
        return False

    return [
        dict(row) for row in assertions_filtered if _match_row(row.get("match_keys"))
    ]


def filter_assertions_by_follow_keys(
    assertions_filtered: list[dict[str, object]], *, follow_keys: list[str]
) -> list[dict[str, object]]:
    if not assertions_filtered:
        return []
    keys = [str(k).strip() for k in (follow_keys or []) if str(k).strip()]
    if not keys:
        return []
    want: set[str] = set(keys)

    has_match_keys = any("match_keys" in row for row in assertions_filtered)
    if not has_match_keys:
        return [
            dict(row)
            for row in assertions_filtered
            if str(row.get("entity_key") or "").strip() in want
        ]

    def _match_row(mk: object) -> bool:
        if not isinstance(mk, list):
            return False
        for x in mk:
            s = str(x).strip()
            if s and s in want:
                return True
        return False

    return [
        dict(row) for row in assertions_filtered if _match_row(row.get("match_keys"))
    ]


__all__ = [
    "MAX_KEY_CANDIDATES",
    "build_grouped_key_candidates",
    "canonicalize_key",
    "filter_assertions_by_follow_key",
    "filter_assertions_by_follow_keys",
    "format_key_label",
    "has_bad_stock_separator",
    "is_stock_code_value",
    "key_candidates",
    "normalize_stock_code",
    "parse_json_list",
    "split_stock_value_tokens",
]
