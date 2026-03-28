from __future__ import annotations

from typing import Any, cast

import libsql
import pytest

from alphavault.db import turso_queue
from alphavault.db.sql.turso_queue import (
    RECOVER_DONE_WITHOUT_PROCESSED_AT,
    RECOVER_STUCK_AI_TASKS,
    UPSERT_PENDING_POST,
)
from alphavault.db.turso_db import (
    TursoConnection,
    is_turso_stream_not_found_error,
    turso_savepoint,
)
from alphavault.worker.ingest import _build_raw_text


def test_is_turso_stream_not_found_error_true() -> None:
    err = ValueError(
        'Hrana: `api error: `status=404 Not Found, body={"error":"stream not found: abc"} ``'
    )
    assert is_turso_stream_not_found_error(err)


def test_is_turso_stream_not_found_error_true_wrapped() -> None:
    inner = ValueError("stream not found: xyz")
    try:
        raise RuntimeError("outer") from inner
    except RuntimeError as outer:
        assert is_turso_stream_not_found_error(outer)


def test_is_turso_stream_not_found_error_false() -> None:
    err = RuntimeError("connection reset by peer")
    assert not is_turso_stream_not_found_error(err)


def test_turso_connection_named_params_and_mappings() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute(
            "CREATE TABLE posts(post_uid TEXT PRIMARY KEY, author TEXT NOT NULL, score INTEGER NOT NULL)"
        )
        conn.execute(
            "INSERT INTO posts(post_uid, author, score) VALUES (:post_uid, :author, :score)",
            {"post_uid": "p1", "author": "alice", "score": 7},
        )
        row = (
            conn.execute(
                "SELECT post_uid, author, score FROM posts WHERE post_uid = :post_uid",
                {"post_uid": "p1"},
            )
            .mappings()
            .fetchone()
        )
        assert row == {"post_uid": "p1", "author": "alice", "score": 7}
    finally:
        conn.close()


def test_turso_connection_executemany_with_mapping_list() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute(
            "CREATE TABLE assertions(post_uid TEXT NOT NULL, idx INTEGER NOT NULL)"
        )
        conn.execute(
            "INSERT INTO assertions(post_uid, idx) VALUES (:post_uid, :idx)",
            [
                {"post_uid": "p1", "idx": 1},
                {"post_uid": "p1", "idx": 2},
                {"post_uid": "p2", "idx": 1},
            ],
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM assertions WHERE post_uid = :post_uid",
                {"post_uid": "p1"},
            ).scalar()
            == 2
        )
    finally:
        conn.close()


def test_turso_savepoint_commit_and_rollback() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")

        with turso_savepoint(conn):
            conn.execute(
                "INSERT INTO t(id, v) VALUES (:id, :v)",
                {"id": 1, "v": "a"},
            )

        assert conn.execute("SELECT COUNT(*) FROM t").scalar() == 1

        with pytest.raises(RuntimeError):
            with turso_savepoint(conn):
                conn.execute(
                    "INSERT INTO t(id, v) VALUES (:id, :v)",
                    {"id": 2, "v": "b"},
                )
                raise RuntimeError("boom")

        assert conn.execute("SELECT COUNT(*) FROM t").scalar() == 1
    finally:
        conn.close()


def test_named_params_support_escaped_colon_literal() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute(
            "CREATE TABLE posts(post_uid TEXT PRIMARY KEY, ai_status TEXT, ai_running_at INTEGER, ai_last_error TEXT, ai_next_retry_at INTEGER)"
        )
        conn.execute(
            "INSERT INTO posts(post_uid, ai_status, ai_running_at) VALUES (:post_uid, :ai_status, :ai_running_at)",
            {"post_uid": "p1", "ai_status": "running", "ai_running_at": 10},
        )

        res = conn.execute(
            RECOVER_STUCK_AI_TASKS,
            {"threshold": 10, "next_retry_at": 999},
        )
        assert int(res.rowcount or 0) == 1

        row = (
            conn.execute(
                "SELECT ai_status, ai_last_error, ai_next_retry_at FROM posts WHERE post_uid = :post_uid",
                {"post_uid": "p1"},
            )
            .mappings()
            .fetchone()
        )
        assert row == {
            "ai_status": "error",
            "ai_last_error": "ai:recovered_after_restart",
            "ai_next_retry_at": 999,
        }
    finally:
        conn.close()


def test_named_params_support_escaped_colon_literal_in_recover_done_without_processed_at() -> (
    None
):
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute(
            "CREATE TABLE posts(post_uid TEXT PRIMARY KEY, ai_status TEXT, processed_at TEXT, ai_last_error TEXT, ai_running_at INTEGER, ai_next_retry_at INTEGER)"
        )
        conn.execute(
            "INSERT INTO posts(post_uid, ai_status, processed_at) VALUES (:post_uid, :ai_status, :processed_at)",
            {"post_uid": "p1", "ai_status": "done", "processed_at": ""},
        )
        conn.execute(
            "INSERT INTO posts(post_uid, ai_status, processed_at) VALUES (:post_uid, :ai_status, :processed_at)",
            {"post_uid": "p2", "ai_status": "done", "processed_at": "2025-01-01"},
        )

        # NOTE: the worker uses an empty dict params object here, which previously
        # triggered sqlparams interpreting ":recovered_done_without_processed_at"
        # inside the SQL string literal as a named param.
        res = conn.execute(RECOVER_DONE_WITHOUT_PROCESSED_AT, {})
        assert int(res.rowcount or 0) == 1

        row1 = (
            conn.execute(
                "SELECT ai_status, ai_last_error FROM posts WHERE post_uid = :post_uid",
                {"post_uid": "p1"},
            )
            .mappings()
            .fetchone()
        )
        row2 = (
            conn.execute(
                "SELECT ai_status, ai_last_error FROM posts WHERE post_uid = :post_uid",
                {"post_uid": "p2"},
            )
            .mappings()
            .fetchone()
        )
        assert row1 == {
            "ai_status": "pending",
            "ai_last_error": "ai:recovered_done_without_processed_at",
        }
        assert row2 == {"ai_status": "done", "ai_last_error": None}
    finally:
        conn.close()


def test_upsert_pending_post_refreshes_author_for_processed_rows() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute(
            """
            CREATE TABLE posts(
                post_uid TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                platform_post_id TEXT NOT NULL,
                author TEXT NOT NULL,
                created_at TEXT NOT NULL,
                url TEXT NOT NULL,
                raw_text TEXT NOT NULL,
                display_md TEXT,
                final_status TEXT NOT NULL,
                invest_score REAL,
                processed_at TEXT,
                model TEXT,
                prompt_version TEXT,
                archived_at TEXT,
                ai_status TEXT NOT NULL DEFAULT 'done',
                ai_retry_count INTEGER NOT NULL DEFAULT 0,
                ai_next_retry_at INTEGER,
                ai_running_at INTEGER,
                ai_last_error TEXT,
                ai_result_json TEXT,
                ingested_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO posts(
                post_uid, platform, platform_post_id, author, created_at, url, raw_text,
                display_md, final_status, invest_score, processed_at, model,
                prompt_version, archived_at, ai_status, ai_retry_count,
                ai_next_retry_at, ai_running_at, ai_last_error, ai_result_json,
                ingested_at
            ) VALUES (
                :post_uid, :platform, :platform_post_id, :author, :created_at, :url, :raw_text,
                :display_md, :final_status, :invest_score, :processed_at, :model,
                :prompt_version, :archived_at, :ai_status, :ai_retry_count,
                :ai_next_retry_at, :ai_running_at, :ai_last_error, :ai_result_json,
                :ingested_at
            )
            """,
            {
                "post_uid": "xueqiu:123",
                "platform": "xueqiu",
                "platform_post_id": "123",
                "author": "旧作者",
                "created_at": "2025-01-01 10:00:00",
                "url": "https://xueqiu.com/123",
                "raw_text": "old text",
                "display_md": "old md",
                "final_status": "relevant",
                "invest_score": 0.8,
                "processed_at": "2025-01-01 10:05:00",
                "model": "gpt",
                "prompt_version": "v1",
                "archived_at": "2025-01-01 10:06:00",
                "ai_status": "done",
                "ai_retry_count": 0,
                "ai_next_retry_at": None,
                "ai_running_at": None,
                "ai_last_error": None,
                "ai_result_json": "{}",
                "ingested_at": 100,
            },
        )

        conn.execute(
            UPSERT_PENDING_POST,
            {
                "post_uid": "xueqiu:123",
                "platform": "xueqiu",
                "platform_post_id": "123",
                "author": "新作者",
                "created_at": "2025-01-02 10:00:00+08:00",
                "url": "https://xueqiu.com/123?updated=1",
                "raw_text": "new text",
                "display_md": "new md",
                "final_status": "irrelevant",
                "archived_at": "2025-01-02 10:06:00+08:00",
                "ai_status": "pending",
                "ingested_at": 200,
            },
        )

        row = (
            conn.execute(
                """
                SELECT author, created_at, archived_at, raw_text, processed_at, ingested_at
                FROM posts
                WHERE post_uid = :post_uid
                """,
                {"post_uid": "xueqiu:123"},
            )
            .mappings()
            .fetchone()
        )
        assert row == {
            "author": "新作者",
            "created_at": "2025-01-02 10:00:00+08:00",
            "archived_at": "2025-01-02 10:06:00+08:00",
            "raw_text": "new text",
            "processed_at": "2025-01-01 10:05:00",
            "ingested_at": 100,
        }
    finally:
        conn.close()


def test_upsert_pending_post_preserves_processed_weibo_raw_text() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute(
            """
            CREATE TABLE posts(
                post_uid TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                platform_post_id TEXT NOT NULL,
                author TEXT NOT NULL,
                created_at TEXT NOT NULL,
                url TEXT NOT NULL,
                raw_text TEXT NOT NULL,
                display_md TEXT,
                final_status TEXT NOT NULL,
                invest_score REAL,
                processed_at TEXT,
                model TEXT,
                prompt_version TEXT,
                archived_at TEXT,
                ai_status TEXT NOT NULL DEFAULT 'done',
                ai_retry_count INTEGER NOT NULL DEFAULT 0,
                ai_next_retry_at INTEGER,
                ai_running_at INTEGER,
                ai_last_error TEXT,
                ai_result_json TEXT,
                ingested_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO posts(
                post_uid, platform, platform_post_id, author, created_at, url, raw_text,
                display_md, final_status, invest_score, processed_at, model,
                prompt_version, archived_at, ai_status, ai_retry_count,
                ai_next_retry_at, ai_running_at, ai_last_error, ai_result_json,
                ingested_at
            ) VALUES (
                :post_uid, :platform, :platform_post_id, :author, :created_at, :url, :raw_text,
                :display_md, :final_status, :invest_score, :processed_at, :model,
                :prompt_version, :archived_at, :ai_status, :ai_retry_count,
                :ai_next_retry_at, :ai_running_at, :ai_last_error, :ai_result_json,
                :ingested_at
            )
            """,
            {
                "post_uid": "weibo:123",
                "platform": "weibo",
                "platform_post_id": "123",
                "author": "旧作者",
                "created_at": "2025-01-01 10:00:00",
                "url": "https://weibo.com/123",
                "raw_text": "old text",
                "display_md": "old md",
                "final_status": "relevant",
                "invest_score": 0.8,
                "processed_at": "2025-01-01 10:05:00",
                "model": "gpt",
                "prompt_version": "v1",
                "archived_at": "2025-01-01 10:06:00",
                "ai_status": "done",
                "ai_retry_count": 0,
                "ai_next_retry_at": None,
                "ai_running_at": None,
                "ai_last_error": None,
                "ai_result_json": "{}",
                "ingested_at": 100,
            },
        )

        conn.execute(
            UPSERT_PENDING_POST,
            {
                "post_uid": "weibo:123",
                "platform": "weibo",
                "platform_post_id": "123",
                "author": "新作者",
                "created_at": "2025-01-02 10:00:00",
                "url": "https://weibo.com/123?updated=1",
                "raw_text": "new text",
                "display_md": "new md",
                "final_status": "irrelevant",
                "archived_at": "2025-01-02 10:06:00",
                "ai_status": "pending",
                "ingested_at": 200,
            },
        )

        row = (
            conn.execute(
                """
                SELECT author, raw_text, processed_at, ingested_at
                FROM posts
                WHERE post_uid = :post_uid
                """,
                {"post_uid": "weibo:123"},
            )
            .mappings()
            .fetchone()
        )
        assert row == {
            "author": "新作者",
            "raw_text": "old text",
            "processed_at": "2025-01-01 10:05:00",
            "ingested_at": 100,
        }
    finally:
        conn.close()


def test_build_raw_text_prefers_content_without_title_prefix() -> None:
    assert _build_raw_text(title="雪球标题", content_text="正文内容") == "正文内容"
    assert _build_raw_text(title="仅标题", content_text="") == "仅标题"


def test_turso_connection_execute_disposes_engine_on_stream_not_found() -> None:
    class _Raw:
        def execute(self, _query, _params):  # type: ignore[no-untyped-def]
            raise ValueError("stream not found: abc")

    class _FakeEngine:
        def __init__(self) -> None:
            self.dispose_calls = 0

        def dispose(self) -> None:
            self.dispose_calls += 1

    engine = _FakeEngine()
    conn = TursoConnection(_Raw(), _engine=cast(Any, engine), _generation=1)
    with pytest.raises(ValueError):
        conn.execute("INSERT INTO t(id) VALUES (?)", (1,))
    assert engine.dispose_calls == 1


def test_turso_connection_execute_disposes_engine_on_libsql_panic() -> None:
    PanicException = type(
        "PanicException", (BaseException,), {"__module__": "pyo3_runtime"}
    )

    class _Raw:
        def execute(self, _query, _params):  # type: ignore[no-untyped-def]
            raise PanicException("Option::unwrap(None)")

    class _FakeEngine:
        def __init__(self) -> None:
            self.dispose_calls = 0

        def dispose(self) -> None:
            self.dispose_calls += 1

    engine = _FakeEngine()
    conn = TursoConnection(_Raw(), _engine=cast(Any, engine), _generation=1)
    with pytest.raises(PanicException):
        conn.execute("INSERT INTO t(id) VALUES (?)", (1,))
    assert engine.dispose_calls == 1


def test_upsert_pending_post_wraps_nonfatal_base_exception(monkeypatch) -> None:
    PanicException = type(
        "PanicException", (BaseException,), {"__module__": "pyo3_runtime"}
    )

    class _FakeConn:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, _exc_type, _exc, _tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, _query, _params):  # type: ignore[no-untyped-def]
            raise PanicException("Option::unwrap(None)")

    class _FakeEngine:
        def __init__(self) -> None:
            self.dispose_calls = 0

        def dispose(self) -> None:
            self.dispose_calls += 1

    monkeypatch.setattr(
        turso_queue, "turso_connect_autocommit", lambda _engine: _FakeConn()
    )
    engine = _FakeEngine()
    with pytest.raises(turso_queue.TursoWriteError) as err:
        turso_queue.upsert_pending_post(
            engine,  # type: ignore[arg-type]
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1",
            author="a",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/1",
            raw_text="text",
            display_md="text",
            archived_at="2026-03-28 10:01:00",
            ingested_at=1,
        )
    assert isinstance(err.value.__cause__, PanicException)
    assert engine.dispose_calls == 1


def test_upsert_pending_post_reraises_fatal_base_exception(monkeypatch) -> None:
    class _FakeConn:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, _exc_type, _exc, _tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, _query, _params):  # type: ignore[no-untyped-def]
            raise KeyboardInterrupt()

    class _FakeEngine:
        def __init__(self) -> None:
            self.dispose_calls = 0

        def dispose(self) -> None:
            self.dispose_calls += 1

    monkeypatch.setattr(
        turso_queue, "turso_connect_autocommit", lambda _engine: _FakeConn()
    )
    engine = _FakeEngine()
    with pytest.raises(KeyboardInterrupt):
        turso_queue.upsert_pending_post(
            engine,  # type: ignore[arg-type]
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1",
            author="a",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/1",
            raw_text="text",
            display_md="text",
            archived_at="2026-03-28 10:01:00",
            ingested_at=1,
        )
    assert engine.dispose_calls == 0
