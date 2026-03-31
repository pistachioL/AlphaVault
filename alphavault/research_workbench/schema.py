from __future__ import annotations

from contextlib import contextmanager
import threading
from typing import Iterator

from alphavault.db.sql.research_workbench import (
    create_research_alias_resolve_tasks_index,
    create_research_alias_resolve_tasks_table,
    create_research_object_index,
    create_research_objects_table,
    create_research_relation_candidate_index,
    create_research_relation_candidate_left_key_index,
    create_research_relation_candidates_table,
    create_research_relation_index,
    create_research_relations_table,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
)

RESEARCH_OBJECTS_TABLE = "research_objects"
RESEARCH_RELATIONS_TABLE = "research_relations"
RESEARCH_RELATION_CANDIDATES_TABLE = "research_relation_candidates"
RESEARCH_ALIAS_RESOLVE_TASKS_TABLE = "research_alias_resolve_tasks"

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_SCHEMA_READY_LOCK = threading.RLock()
_SCHEMA_READY_KEYS: set[str] = set()


@contextmanager
def use_conn(
    engine_or_conn: TursoEngine | TursoConnection,
) -> Iterator[TursoConnection]:
    if isinstance(engine_or_conn, TursoConnection):
        yield engine_or_conn
        return
    with turso_connect_autocommit(engine_or_conn) as conn:
        yield conn


def schema_cache_key(engine_or_conn: TursoEngine | TursoConnection) -> str:
    engine = (
        engine_or_conn._engine
        if isinstance(engine_or_conn, TursoConnection)
        else engine_or_conn
    )
    if engine is None:
        return ""
    return (
        f"{str(engine.remote_url or '').strip()}|{str(engine.auth_token or '').strip()}"
    )


def clear_schema_ready(engine_or_conn: TursoEngine | TursoConnection) -> None:
    cache_key = schema_cache_key(engine_or_conn)
    if not cache_key:
        return
    with _SCHEMA_READY_LOCK:
        _SCHEMA_READY_KEYS.discard(cache_key)


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
        clear_schema_ready(engine_or_conn)
        engine.dispose()
    raise err


def run_schema_ddl(engine_or_conn: TursoEngine | TursoConnection) -> None:
    with use_conn(engine_or_conn) as conn:
        conn.execute(create_research_objects_table(RESEARCH_OBJECTS_TABLE))
        conn.execute(create_research_relations_table(RESEARCH_RELATIONS_TABLE))
        conn.execute(
            create_research_relation_candidates_table(
                RESEARCH_RELATION_CANDIDATES_TABLE
            )
        )
        conn.execute(
            create_research_alias_resolve_tasks_table(
                RESEARCH_ALIAS_RESOLVE_TASKS_TABLE
            )
        )
        conn.execute(create_research_object_index(RESEARCH_OBJECTS_TABLE))
        conn.execute(create_research_relation_index(RESEARCH_RELATIONS_TABLE))
        conn.execute(
            create_research_relation_candidate_index(RESEARCH_RELATION_CANDIDATES_TABLE)
        )
        conn.execute(
            create_research_relation_candidate_left_key_index(
                RESEARCH_RELATION_CANDIDATES_TABLE
            )
        )
        conn.execute(
            create_research_alias_resolve_tasks_index(
                RESEARCH_ALIAS_RESOLVE_TASKS_TABLE
            )
        )


def _ensure_schema_ready(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    schema_cache_key_fn,
    schema_ready_lock,
    schema_ready_keys,
    run_schema_ddl_fn,
    handle_turso_error_fn,
) -> None:
    cache_key = schema_cache_key_fn(engine_or_conn)
    if cache_key:
        with schema_ready_lock:
            if cache_key in schema_ready_keys:
                return
            try:
                run_schema_ddl_fn(engine_or_conn)
            except BaseException as err:
                handle_turso_error_fn(engine_or_conn, err)
            schema_ready_keys.add(cache_key)
        return

    try:
        run_schema_ddl_fn(engine_or_conn)
    except BaseException as err:
        handle_turso_error_fn(engine_or_conn, err)


def ensure_research_workbench_schema(
    engine_or_conn: TursoEngine | TursoConnection,
) -> None:
    _ensure_schema_ready(
        engine_or_conn,
        schema_cache_key_fn=schema_cache_key,
        schema_ready_lock=_SCHEMA_READY_LOCK,
        schema_ready_keys=_SCHEMA_READY_KEYS,
        run_schema_ddl_fn=run_schema_ddl,
        handle_turso_error_fn=handle_turso_error,
    )


__all__ = [
    "RESEARCH_ALIAS_RESOLVE_TASKS_TABLE",
    "RESEARCH_OBJECTS_TABLE",
    "RESEARCH_RELATION_CANDIDATES_TABLE",
    "RESEARCH_RELATIONS_TABLE",
    "clear_schema_ready",
    "ensure_research_workbench_schema",
    "handle_turso_error",
    "run_schema_ddl",
    "schema_cache_key",
    "use_conn",
]
