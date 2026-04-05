from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
)

RESEARCH_SECURITY_MASTER_TABLE = "security_master"
RESEARCH_RELATIONS_TABLE = "relations"
RESEARCH_RELATION_CANDIDATES_TABLE = "relation_candidates"
RESEARCH_ALIAS_RESOLVE_TASKS_TABLE = "alias_resolve_tasks"

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


@contextmanager
def use_conn(
    engine_or_conn: TursoEngine | TursoConnection,
) -> Iterator[TursoConnection]:
    if isinstance(engine_or_conn, TursoConnection):
        yield engine_or_conn
        return
    with turso_connect_autocommit(engine_or_conn) as conn:
        yield conn


def handle_turso_error(
    engine_or_conn: TursoEngine | TursoConnection, err: BaseException
) -> None:
    if isinstance(err, _FATAL_BASE_EXCEPTIONS):
        raise err
    engine = (
        engine_or_conn._engine
        if isinstance(engine_or_conn, TursoConnection)
        else engine_or_conn
    )
    if engine is not None and (
        is_turso_stream_not_found_error(err) or is_turso_libsql_panic_error(err)
    ):
        engine.dispose()
    raise err


__all__ = [
    "RESEARCH_ALIAS_RESOLVE_TASKS_TABLE",
    "RESEARCH_RELATION_CANDIDATES_TABLE",
    "RESEARCH_RELATIONS_TABLE",
    "RESEARCH_SECURITY_MASTER_TABLE",
    "handle_turso_error",
    "use_conn",
]
