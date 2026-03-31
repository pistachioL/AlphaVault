from __future__ import annotations

import threading
import time
from dataclasses import dataclass
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


AUTHOR_RECENT_LOCAL_CACHE_TTL_SECONDS = 30.0
AUTHOR_RECENT_LOCAL_CACHE_MAX_AUTHORS = 32
AUTHOR_RECENT_LOCAL_CACHE_MAX_ROWS = 80


@dataclass
class _CacheEntry:
    expires_at: float
    rows: list[dict[str, object]]
    marked_empty: bool


class AuthorRecentLocalCache:
    def __init__(
        self,
        *,
        ttl_seconds: float = AUTHOR_RECENT_LOCAL_CACHE_TTL_SECONDS,
        max_authors: int = AUTHOR_RECENT_LOCAL_CACHE_MAX_AUTHORS,
        max_rows: int = AUTHOR_RECENT_LOCAL_CACHE_MAX_ROWS,
    ) -> None:
        self._ttl_seconds = max(0.0, float(ttl_seconds))
        self._max_authors = max(1, int(max_authors))
        self._max_rows = max(1, int(max_rows))
        self._lock = threading.Lock()
        self._cache: dict[str, _CacheEntry] = {}

    def get(
        self, *, queue_key: str, author: str
    ) -> tuple[list[dict[str, object]], bool, bool]:
        key = self._cache_key(queue_key=queue_key, author=author)
        if not key or key == "::":
            return [], False, False
        now = time.time()
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return [], False, False
            if now >= float(entry.expires_at):
                self._cache.pop(key, None)
                return [], False, False
            self._cache.pop(key, None)
            self._cache[key] = entry
            return (
                [dict(row) for row in entry.rows if isinstance(row, dict)],
                bool(entry.marked_empty),
                True,
            )

    def set(
        self,
        *,
        queue_key: str,
        author: str,
        rows: list[dict[str, object]],
        marked_empty: bool,
    ) -> None:
        key = self._cache_key(queue_key=queue_key, author=author)
        if not key or key == "::":
            return
        trimmed_rows = self._trim_rows(rows)
        resolved_marked_empty = bool(marked_empty and not bool(trimmed_rows))
        entry = _CacheEntry(
            expires_at=time.time() + float(self._ttl_seconds),
            rows=list(trimmed_rows),
            marked_empty=bool(resolved_marked_empty),
        )
        with self._lock:
            self._cache.pop(key, None)
            self._cache[key] = entry
            while len(self._cache) > int(self._max_authors):
                oldest_key = next(iter(self._cache.keys()), "")
                if not oldest_key:
                    break
                self._cache.pop(oldest_key, None)

    def _cache_key(self, *, queue_key: str, author: str) -> str:
        return f"{str(queue_key or '').strip()}::{str(author or '').strip().lower()}"

    def _trim_rows(self, rows: list[dict[str, object]]) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            out.append(dict(row))
            if len(out) >= int(self._max_rows):
                break
        return out


__all__ = [
    "AUTHOR_RECENT_LOCAL_CACHE_MAX_AUTHORS",
    "AUTHOR_RECENT_LOCAL_CACHE_MAX_ROWS",
    "AUTHOR_RECENT_LOCAL_CACHE_TTL_SECONDS",
    "AuthorRecentLocalCache",
    "memoize_bool_with_ttl",
]
