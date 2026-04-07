from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from alphavault.constants import (
    DEFAULT_SPOOL_DIR,
    ENV_REDIS_URL,
    ENV_SPOOL_DIR,
    ENV_STANDARD_TURSO_AUTH_TOKEN,
    ENV_STANDARD_TURSO_DATABASE_URL,
    ENV_WEIBO_TURSO_AUTH_TOKEN,
    ENV_WEIBO_TURSO_DATABASE_URL,
    ENV_XUEQIU_TURSO_AUTH_TOKEN,
    ENV_XUEQIU_TURSO_DATABASE_URL,
)
from alphavault.db.sql.scripts import (
    SELECT_ONE,
)
from alphavault.db.turso_db import (
    ensure_turso_engine,
    turso_connect_autocommit,
)
from alphavault.env import load_dotenv_if_present

load_dotenv_if_present()

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


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


def _check_turso() -> None:
    def resolve_source_targets() -> list[tuple[str, str, str]]:
        targets: list[tuple[str, str, str]] = []
        for name, url_env, token_env in (
            ("weibo", ENV_WEIBO_TURSO_DATABASE_URL, ENV_WEIBO_TURSO_AUTH_TOKEN),
            ("xueqiu", ENV_XUEQIU_TURSO_DATABASE_URL, ENV_XUEQIU_TURSO_AUTH_TOKEN),
        ):
            url = _env_text(url_env)
            if not url:
                continue
            token = _env_text(token_env)
            targets.append((name, url, token))
        return targets

    source_targets = resolve_source_targets()
    if not source_targets:
        raise RuntimeError(
            f"missing {ENV_WEIBO_TURSO_DATABASE_URL} or {ENV_XUEQIU_TURSO_DATABASE_URL}"
        )
    standard_url = _env_text(ENV_STANDARD_TURSO_DATABASE_URL)
    if not standard_url:
        raise RuntimeError(f"missing {ENV_STANDARD_TURSO_DATABASE_URL}")
    targets = [
        *source_targets,
        ("standard", standard_url, _env_text(ENV_STANDARD_TURSO_AUTH_TOKEN)),
    ]

    for name, url, token in targets:
        prefix = f"[healthcheck] turso[{name}]"
        _print(f"{prefix} start")
        engine = ensure_turso_engine(url, token)

        try:
            with turso_connect_autocommit(engine) as conn:
                conn.execute(SELECT_ONE).fetchone()
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
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
