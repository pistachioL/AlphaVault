from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from alphavault.constants import (
    DEFAULT_SPOOL_DIR,
    ENV_POSTGRES_DSN,
    ENV_STARTUP_HEALTHCHECK_TURSO_TARGET,
    ENV_REDIS_URL,
    ENV_SPOOL_DIR,
    SCHEMA_STANDARD,
    PLATFORM_WEIBO,
    PLATFORM_XUEQIU,
)
from alphavault.db.sql.scripts import (
    SELECT_ONE,
)
from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.env import load_dotenv_if_present
from alphavault.research_workbench.schema import (
    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
    RESEARCH_RELATION_CANDIDATES_TABLE,
    RESEARCH_RELATIONS_TABLE,
    RESEARCH_SECURITY_MASTER_TABLE,
)

load_dotenv_if_present()

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_STANDARD_TURSO_TARGET = SCHEMA_STANDARD
_DEFAULT_TURSO_TARGET = _STANDARD_TURSO_TARGET
_STANDARD_REQUIRED_TABLES = (
    RESEARCH_SECURITY_MASTER_TABLE,
    RESEARCH_RELATIONS_TABLE,
    RESEARCH_RELATION_CANDIDATES_TABLE,
    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
)


def _print(msg: str) -> None:
    print(msg, flush=True)


def _eprint(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _spool_dir_value() -> str:
    return os.getenv(ENV_SPOOL_DIR, "").strip() or DEFAULT_SPOOL_DIR


def _env_text(name: str) -> str:
    return os.getenv(name, "").strip()


def _check_spool_dir() -> None:
    path = Path(_spool_dir_value())
    _print(f"[healthcheck] spool start dir={path}")

    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"spool mkdir failed: {type(e).__name__}: {e}") from e

    if not path.exists() or not path.is_dir():
        raise RuntimeError("spool not a directory")

    # Simple write test.
    test_path = path / f".healthcheck.{os.getpid()}.{int(time.time())}.tmp"
    try:
        test_path.write_text("ok\n", encoding="utf-8")
        test_path.unlink(missing_ok=True)
    except Exception as e:
        try:
            test_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise RuntimeError(f"spool write failed: {type(e).__name__}: {e}") from e

    _print("[healthcheck] spool ok")


def _healthcheck_turso_target() -> str:
    target = _env_text(ENV_STARTUP_HEALTHCHECK_TURSO_TARGET).lower()
    if not target:
        return _DEFAULT_TURSO_TARGET
    if target in {PLATFORM_WEIBO, PLATFORM_XUEQIU, _STANDARD_TURSO_TARGET}:
        return target
    raise RuntimeError(
        "invalid STARTUP_HEALTHCHECK_TURSO_TARGET: "
        f"{target} (expected weibo, xueqiu, or standard)"
    )


def _resolve_turso_target() -> tuple[str, str]:
    target = _healthcheck_turso_target()
    dsn = _env_text(ENV_POSTGRES_DSN)
    if not dsn:
        raise RuntimeError(f"missing {ENV_POSTGRES_DSN}")
    return target, dsn


def _check_turso() -> None:
    def check_standard_tables(conn) -> None:  # type: ignore[no-untyped-def]
        for table_name in _STANDARD_REQUIRED_TABLES:
            conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()

    name, dsn = _resolve_turso_target()
    prefix = f"[healthcheck] postgres[{name}]"
    _print(f"{prefix} start")
    engine = ensure_postgres_engine(dsn, schema_name=name)

    try:
        with postgres_connect_autocommit(engine) as conn:
            conn.execute(SELECT_ONE).fetchone()
            try:
                if name == _STANDARD_TURSO_TARGET:
                    check_standard_tables(conn)
            except BaseException as e:
                if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                    raise
                raise RuntimeError(
                    f"postgres[{name}] schema check failed: {type(e).__name__}: {e}"
                ) from e
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        if isinstance(e, RuntimeError) and str(e).startswith(
            f"postgres[{name}] schema check failed:"
        ):
            raise e
        raise RuntimeError(
            f"postgres[{name}] connect failed: {type(e).__name__}: {e}"
        ) from e

    _print(f"{prefix} ok")


def _try_check_redis(redis_url: str) -> None:
    try:
        import redis  # type: ignore
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        raise RuntimeError(f"redis import failed: {type(e).__name__}: {e}") from e

    try:
        client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        client.ping()
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        raise RuntimeError(f"redis connect failed: {type(e).__name__}: {e}") from e

    key = f"alphavault:healthcheck:{os.getpid()}:{int(time.time())}"
    try:
        ok = client.set(key, "1", ex=30)
        if ok is not True:
            raise RuntimeError("redis set returned false")
        client.delete(key)
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        raise RuntimeError(f"redis write failed: {type(e).__name__}: {e}") from e


def _check_redis() -> None:
    redis_url = os.getenv(ENV_REDIS_URL, "").strip()
    if not redis_url:
        _print(f"[healthcheck] redis skip ({ENV_REDIS_URL} empty)")
        return

    _print("[healthcheck] redis start")
    _try_check_redis(redis_url)
    _print("[healthcheck] redis ok")


def run_startup_healthcheck() -> None:
    _check_spool_dir()
    _check_turso()
    _check_redis()


def main(argv: list[str]) -> int:
    if len(argv) > 1:
        _eprint("[healthcheck] error: no args supported")
        return 2
    try:
        run_startup_healthcheck()
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _eprint(f"[healthcheck] failed: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
