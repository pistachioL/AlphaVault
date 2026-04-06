from __future__ import annotations

import time
from typing import Callable


def memoize_bool_with_ttl(
    *,
    resolver: Callable[[], bool],
    ttl_seconds: float,
) -> Callable[[], bool]:
    ttl = max(0.0, float(ttl_seconds))
    cache_value = False
    cache_expires_at = 0.0
    has_cache = False

    def _cached() -> bool:
        nonlocal cache_value, cache_expires_at, has_cache
        now = time.time()
        if has_cache and now < cache_expires_at:
            return bool(cache_value)
        value = bool(resolver())
        cache_value = bool(value)
        has_cache = True
        cache_expires_at = now + ttl
        return bool(cache_value)

    return _cached


__all__ = ["memoize_bool_with_ttl"]
