from __future__ import annotations

from typing import Iterable


def pragma_table_info(table: str) -> str:
    return f"PRAGMA table_info({table})"


def make_in_placeholders(*, prefix: str, count: int) -> str:
    n = max(0, int(count))
    if n <= 0:
        return ""
    return ", ".join([f":{prefix}{i}" for i in range(n)])


def make_in_params(*, prefix: str, values: Iterable[object]) -> dict[str, object]:
    return {f"{prefix}{idx}": value for idx, value in enumerate(values)}
