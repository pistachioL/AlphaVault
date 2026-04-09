from __future__ import annotations


from alphavault.db.postgres_db import (
    PostgresEngine,
    postgres_connect_autocommit,
)


def maybe_dispose_turso_engine_on_transient_error(
    *,
    engine: PostgresEngine,
    err: BaseException,
    verbose: bool,
) -> None:
    del engine, err, verbose


def ensure_turso_ready(
    *,
    engine: PostgresEngine,
    verbose: bool,
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
        print(f"{prefix} ready", flush=True)
        return True
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        maybe_dispose_turso_engine_on_transient_error(
            engine=engine,
            err=err,
            verbose=bool(verbose),
        )
        if verbose:
            print(f"{prefix} not_ready {type(err).__name__}: {err}", flush=True)
        return False


__all__ = [
    "ensure_turso_ready",
    "maybe_dispose_turso_engine_on_transient_error",
]
