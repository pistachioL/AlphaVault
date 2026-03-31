from __future__ import annotations

from typing import Callable


def clear_registered_caches(*cache_clear_fns: Callable[[], None]) -> None:
    for clear_fn in cache_clear_fns:
        try:
            clear_fn()
        except Exception:
            continue
    try:
        from alphavault_reflex.services.stock_hot_read import (
            clear_stock_hot_read_caches,
        )

        clear_stock_hot_read_caches()
    except Exception:
        pass


__all__ = ["clear_registered_caches"]
