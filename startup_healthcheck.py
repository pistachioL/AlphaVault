from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from alphavault.constants import (
    DEFAULT_SPOOL_DIR,
    ENV_STARTUP_HEALTHCHECK_TURSO_TARGET,
    ENV_REDIS_URL,
    ENV_SPOOL_DIR,
    ENV_STANDARD_TURSO_AUTH_TOKEN,
    ENV_STANDARD_TURSO_DATABASE_URL,
    ENV_WEIBO_TURSO_AUTH_TOKEN,
    ENV_WEIBO_TURSO_DATABASE_URL,
    ENV_XUEQIU_TURSO_AUTH_TOKEN,
    ENV_XUEQIU_TURSO_DATABASE_URL,
    PLATFORM_WEIBO,
    PLATFORM_XUEQIU,
)
from alphavault.db.sql.scripts import (
    SELECT_ONE,
)
from alphavault.db.turso_db import (
    ensure_turso_engine,
    turso_connect_autocommit,
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
_STANDARD_TURSO_TARGET = "standard"
_DEFAULT_TURSO_TARGET = _STANDARD_TURSO_TARGET
_STANDARD_REQUIRED_TABLES = (
    RESEARCH_SECURITY_MASTER_TABLE,
    RESEARCH_RELATIONS_TABLE,
    RESEARCH_RELATION_CANDIDATES_TABLE,
    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
)
_TURSO_TARGET_ENV_MAP = {
    PLATFORM_WEIBO: (
        ENV_WEIBO_TURSO_DATABASE_URL,
        ENV_WEIBO_TURSO_AUTH_TOKEN,
    ),
    PLATFORM_XUEQIU: (
        ENV_XUEQIU_TURSO_DATABASE_URL,
        ENV_XUEQIU_TURSO_AUTH_TOKEN,
    ),
    _STANDARD_TURSO_TARGET: (
        ENV_STANDARD_TURSO_DATABASE_URL,
        ENV_STANDARD_TURSO_AUTH_TOKEN,
    ),
}


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
    if target in _TURSO_TARGET_ENV_MAP:
        return target
    raise RuntimeError(
        "invalid STARTUP_HEALTHCHECK_TURSO_TARGET: "
        f"{target} (expected weibo, xueqiu, or standard)"
    )


def _resolve_turso_target() -> tuple[str, str, str]:
    target = _healthcheck_turso_target()
    url_env, token_env = _TURSO_TARGET_ENV_MAP[target]
    url = _env_text(url_env)
    if not url:
        raise RuntimeError(f"missing {url_env}")
    return target, url, _env_text(token_env)


def _check_turso() -> None:
    def check_standard_tables(conn) -> None:  # type: ignore[no-untyped-def]
        for table_name in _STANDARD_REQUIRED_TABLES:
            conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()

    name, url, token = _resolve_turso_target()
    prefix = f"[healthcheck] turso[{name}]"
    _print(f"{prefix} start")
    engine = ensure_turso_engine(url, token)

    try:
        with turso_connect_autocommit(engine) as conn:
            conn.execute(SELECT_ONE).fetchone()
            try:
                if name == _STANDARD_TURSO_TARGET:
                    check_standard_tables(conn)
            except BaseException as e:
                if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                    raise
                raise RuntimeError(
                    f"turso[{name}] schema check failed: {type(e).__name__}: {e}"
                ) from e
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        if isinstance(e, RuntimeError) and str(e).startswith(
            f"turso[{name}] schema check failed:"
        ):
            raise e
        raise RuntimeError(
            f"turso[{name}] connect failed: {type(e).__name__}: {e}"
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
