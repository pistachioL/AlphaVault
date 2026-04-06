from __future__ import annotations


from alphavault.db.turso_db import (
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
)


def maybe_dispose_turso_engine_on_transient_error(
    *,
    engine: TursoEngine,
    err: BaseException,
    verbose: bool,
) -> None:
    reason = ""
    if is_turso_stream_not_found_error(err):
        reason = "stream_not_found"
    elif is_turso_libsql_panic_error(err):
        reason = "libsql_panic"
    else:
        return
    try:
        engine.dispose()
        if verbose:
            print(f"[turso] disposed_engine reason={reason}", flush=True)
    except Exception as dispose_err:
        if verbose:
            print(
                f"[turso] dispose_engine_failed {type(dispose_err).__name__}: {dispose_err}",
                flush=True,
            )


def ensure_turso_ready(
    *,
    engine: TursoEngine,
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
        with turso_connect_autocommit(engine):
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
