from __future__ import annotations

from alphavault.logging_config import get_logger

from alphavault.db.postgres_db import (
    PostgresEngine,
    postgres_connect_autocommit,
)

logger = get_logger(__name__)


def maybe_dispose_turso_engine_on_transient_error(
    *,
    engine: PostgresEngine,
    err: BaseException,
) -> None:
    del engine, err


def ensure_turso_ready(
    *,
    engine: PostgresEngine,
    turso_ready: bool,
    source_name: str = "",
    fatal_exceptions: tuple[type[BaseException], ...] = (
        KeyboardInterrupt,
        SystemExit,
        GeneratorExit,
    ),
) -> bool:
    if turso_ready:
        return True
    prefix = f"[turso:{source_name}]" if source_name else "[turso]"
    try:
        with postgres_connect_autocommit(engine):
            pass
        logger.info("%s ready", prefix)
        return True
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        maybe_dispose_turso_engine_on_transient_error(
            engine=engine,
            err=err,
        )
        logger.warning("%s not_ready %s: %s", prefix, type(err).__name__, err)
        return False


__all__ = [
    "ensure_turso_ready",
    "maybe_dispose_turso_engine_on_transient_error",
]
