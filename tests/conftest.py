from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest


_DEFAULT_TEST_POSTGRES_PORT = "5433"


def _default_test_postgres_dsn() -> str:
    user = os.getenv("USER", "").strip()
    auth = f"{user}@" if user else ""
    return f"postgresql://{auth}127.0.0.1:{_DEFAULT_TEST_POSTGRES_PORT}/postgres"


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    return os.getenv("TEST_POSTGRES_DSN", _default_test_postgres_dsn()).strip()


@pytest.fixture()
def pg_conn(postgres_dsn: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(postgres_dsn) as conn:
        try:
            yield conn
        finally:
            if not conn.closed:
                conn.rollback()
