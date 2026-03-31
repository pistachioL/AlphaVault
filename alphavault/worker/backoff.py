from __future__ import annotations


def backoff_seconds(retry_count: int) -> int:
    retries = max(1, int(retry_count))
    delay = 30 * (2 ** max(0, retries - 1))
    return int(min(3600, delay))


__all__ = ["backoff_seconds"]
