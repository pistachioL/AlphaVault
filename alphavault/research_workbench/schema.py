from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from alphavault.constants import SCHEMA_STANDARD
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    postgres_connect_autocommit,
)

RESEARCH_SECURITY_MASTER_TABLE = f"{SCHEMA_STANDARD}.security_master"
RESEARCH_RELATIONS_TABLE = f"{SCHEMA_STANDARD}.relations"
RESEARCH_RELATION_CANDIDATES_TABLE = f"{SCHEMA_STANDARD}.relation_candidates"
RESEARCH_ALIAS_RESOLVE_TASKS_TABLE = f"{SCHEMA_STANDARD}.alias_resolve_tasks"

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


@contextmanager
def use_conn(
    engine_or_conn: PostgresEngine | PostgresConnection | Any,
) -> Iterator[Any]:
    if isinstance(engine_or_conn, PostgresEngine):
        with postgres_connect_autocommit(engine_or_conn) as conn:
            yield conn
        return
    yield engine_or_conn


def handle_db_error(
    engine_or_conn: PostgresEngine | PostgresConnection | Any, err: BaseException
) -> None:
    del engine_or_conn
    if isinstance(err, _FATAL_BASE_EXCEPTIONS):
        raise err
    raise err


def handle_turso_error(
    engine_or_conn: PostgresEngine | PostgresConnection | Any, err: BaseException
) -> None:
    handle_db_error(engine_or_conn, err)


__all__ = [
    "RESEARCH_ALIAS_RESOLVE_TASKS_TABLE",
    "RESEARCH_RELATION_CANDIDATES_TABLE",
    "RESEARCH_RELATIONS_TABLE",
    "RESEARCH_SECURITY_MASTER_TABLE",
    "handle_db_error",
    "handle_turso_error",
    "use_conn",
]
