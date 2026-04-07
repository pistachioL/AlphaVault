from __future__ import annotations

from contextlib import contextmanager
import inspect
from typing import Any, cast

import libsql
import pytest

from alphavault.db import turso_queue
from alphavault.db.sql.turso_queue import (
    INSERT_ASSERTION,
    INSERT_ASSERTION_ENTITY,
    INSERT_ASSERTION_MENTION,
    UPSERT_PENDING_POST,
)
from alphavault.db.turso_db import (
    TursoConnection,
    is_turso_stream_not_found_error,
    turso_savepoint,
)
from alphavault.domains.entity_match.resolve import EntityMatchResult
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
                final_status TEXT NOT NULL,
                invest_score REAL,
                processed_at TEXT,
                model TEXT,
                prompt_version TEXT,
                archived_at TEXT,
                ingested_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO posts(
                post_uid, platform, platform_post_id, author, created_at, url, raw_text,
                final_status, invest_score, processed_at, model, prompt_version,
                archived_at, ingested_at
            ) VALUES (
                :post_uid, :platform, :platform_post_id, :author, :created_at, :url, :raw_text,
                :final_status, :invest_score, :processed_at, :model, :prompt_version,
                :archived_at, :ingested_at
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
                "final_status": "relevant",
                "invest_score": 0.8,
                "processed_at": "2025-01-01 10:05:00",
                "model": "gpt",
                "prompt_version": "v1",
                "archived_at": "2025-01-01 10:06:00",
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
                "final_status": "irrelevant",
                "archived_at": "2025-01-02 10:06:00+08:00",
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
                final_status TEXT NOT NULL,
                invest_score REAL,
                processed_at TEXT,
                model TEXT,
                prompt_version TEXT,
                archived_at TEXT,
                ingested_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO posts(
                post_uid, platform, platform_post_id, author, created_at, url, raw_text,
                final_status, invest_score, processed_at, model, prompt_version,
                archived_at, ingested_at
            ) VALUES (
                :post_uid, :platform, :platform_post_id, :author, :created_at, :url, :raw_text,
                :final_status, :invest_score, :processed_at, :model, :prompt_version,
                :archived_at, :ingested_at
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
                "final_status": "relevant",
                "invest_score": 0.8,
                "processed_at": "2025-01-01 10:05:00",
                "model": "gpt",
                "prompt_version": "v1",
                "archived_at": "2025-01-01 10:06:00",
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
                "final_status": "irrelevant",
                "archived_at": "2025-01-02 10:06:00",
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
            archived_at="2026-03-28 10:01:00",
            ingested_at=1,
        )
    assert engine.dispose_calls == 0


def test_write_assertions_and_mark_done_has_no_outbox_params() -> None:
    params = inspect.signature(turso_queue.write_assertions_and_mark_done).parameters

    assert "outbox_source" not in params
    assert "outbox_author" not in params
    assert "outbox_event_json" not in params


def test_write_assertions_and_mark_done_writes_assertion_mentions(
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class _FakeConn:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, _exc_type, _exc, _tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, query, params=None):  # type: ignore[no-untyped-def]
            calls.append((str(query), params))
            return self

    @contextmanager
    def _fake_savepoint(_conn):  # type: ignore[no-untyped-def]
        yield

    monkeypatch.setattr(
        turso_queue, "turso_connect_autocommit", lambda _engine: _FakeConn()
    )
    monkeypatch.setattr(turso_queue, "turso_savepoint", _fake_savepoint)

    turso_queue.write_assertions_and_mark_done(
        cast(Any, object()),
        post_uid="weibo:2",
        final_status="relevant",
        invest_score=0.9,
        processed_at="2026-03-28 12:00:00",
        model="m",
        prompt_version="topic-prompt-v4",
        archived_at="2026-03-28 12:00:01",
        assertions=[
            {
                "assertion_id": "weibo:2#1",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "他说开始买了。",
                "evidence": "我今天开始买600519了",
                "created_at": "2026-03-28 11:59:00",
                "assertion_mentions": [
                    {
                        "mention_text": "600519",
                        "mention_norm": "600519",
                        "mention_type": "stock_code",
                        "evidence": "我今天开始买600519了",
                        "confidence": 0.95,
                    }
                ],
            }
        ],
    )

    mention_calls = [
        item for item in calls if item[0].strip() == INSERT_ASSERTION_MENTION.strip()
    ]
    assert len(mention_calls) == 1
    mention_params = cast(list[dict[str, object]], mention_calls[0][1])
    assert mention_params == [
        {
            "assertion_id": "weibo:2#1",
            "mention_seq": 1,
            "mention_text": "600519",
            "mention_norm": "600519",
            "mention_type": "stock_code",
            "evidence": "我今天开始买600519了",
            "confidence": 0.95,
        }
    ]

    assertion_calls = [
        item for item in calls if item[0].strip() == INSERT_ASSERTION.strip()
    ]
    assert len(assertion_calls) == 1
    assertion_params = cast(list[dict[str, object]], assertion_calls[0][1])
    assert assertion_params == [
        {
            "assertion_id": "weibo:2#1",
            "post_uid": "weibo:2",
            "idx": 1,
            "action": "trade.buy",
            "action_strength": 2,
            "summary": "他说开始买了。",
            "evidence": "我今天开始买600519了",
            "created_at": "2026-03-28 11:59:00",
        }
    ]


def test_write_assertions_and_mark_done_upserts_missing_prefetched_post_before_done(
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class _FakeConn:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, _exc_type, _exc, _tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, query, params=None):  # type: ignore[no-untyped-def]
            calls.append((str(query).strip(), params))
            return self

        def mappings(self):  # type: ignore[no-untyped-def]
            return self

        def fetchone(self):  # type: ignore[no-untyped-def]
            return None

    @contextmanager
    def _fake_savepoint(_conn):  # type: ignore[no-untyped-def]
        yield

    monkeypatch.setattr(
        turso_queue, "turso_connect_autocommit", lambda _engine: _FakeConn()
    )
    monkeypatch.setattr(turso_queue, "turso_savepoint", _fake_savepoint)

    turso_queue.write_assertions_and_mark_done(
        cast(Any, object()),
        post_uid="xueqiu:2",
        final_status="relevant",
        invest_score=0.9,
        processed_at="2026-03-28 12:00:00",
        model="m",
        prompt_version="topic-prompt-v4",
        archived_at="2026-03-28 12:00:01",
        assertions=[],
        prefetched_post=turso_queue.CloudPost(
            post_uid="xueqiu:2",
            platform="xueqiu",
            platform_post_id="xueqiu:2",
            author="泽元投资",
            created_at="2026-03-28 11:59:00",
            url="https://xueqiu.com/2",
            raw_text="泽元投资：[献花花][献花花]",
            ai_retry_count=0,
        ),
    )

    assert calls[0][0] == turso_queue.SELECT_POST_PROCESSED_AT.strip()
    assert calls[1][0] == UPSERT_PENDING_POST.strip()
    assert calls[1][1] == {
        "post_uid": "xueqiu:2",
        "platform": "xueqiu",
        "platform_post_id": "xueqiu:2",
        "author": "泽元投资",
        "created_at": "2026-03-28 11:59:00",
        "url": "https://xueqiu.com/2",
        "raw_text": "泽元投资：[献花花][献花花]",
        "final_status": "irrelevant",
        "archived_at": "2026-03-28 12:00:01",
        "ingested_at": 0,
    }
    assert calls[-1][0] == turso_queue.UPDATE_POST_DONE.strip()


def test_write_assertions_and_mark_done_writes_assertion_entities(
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class _FakeConn:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, _exc_type, _exc, _tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, query, params=None):  # type: ignore[no-untyped-def]
            calls.append((str(query), params))
            return self

    @contextmanager
    def _fake_savepoint(_conn):  # type: ignore[no-untyped-def]
        yield

    monkeypatch.setattr(
        turso_queue, "turso_connect_autocommit", lambda _engine: _FakeConn()
    )
    monkeypatch.setattr(turso_queue, "turso_savepoint", _fake_savepoint)

    turso_queue.write_assertions_and_mark_done(
        cast(Any, object()),
        post_uid="weibo:3",
        final_status="relevant",
        invest_score=0.9,
        processed_at="2026-03-28 12:00:00",
        model="m",
        prompt_version="topic-prompt-v4",
        archived_at="2026-03-28 12:00:01",
        assertions=[
            {
                "assertion_id": "weibo:3#1",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "他说开始买了。",
                "evidence": "我今天开始买600519了",
                "created_at": "2026-03-28 11:59:00",
                "assertion_entities": [
                    {
                        "entity_key": "stock:600519.SH",
                        "entity_type": "stock",
                        "match_source": "stock_code",
                        "is_primary": 1,
                    }
                ],
            }
        ],
    )

    entity_calls = [
        item for item in calls if item[0].strip() == INSERT_ASSERTION_ENTITY.strip()
    ]
    assert len(entity_calls) == 1
    entity_params = cast(list[dict[str, object]], entity_calls[0][1])
    assert entity_params == [
        {
            "assertion_id": "weibo:3#1",
            "entity_key": "stock:600519.SH",
            "entity_type": "stock",
            "match_source": "stock_code",
            "is_primary": 1,
        }
    ]


def test_write_assertions_and_mark_done_persists_entity_match_followups_before_done(
    monkeypatch,
) -> None:
    calls: list[str] = []
    followup_calls: list[EntityMatchResult] = []
    standard_engine = object()

    class _FakeConn:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, _exc_type, _exc, _tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, query, params=None):  # type: ignore[no-untyped-def]
            del params
            calls.append(str(query).strip())
            return self

    @contextmanager
    def _fake_savepoint(_conn):  # type: ignore[no-untyped-def]
        yield

    def _fake_persist(_engine, result):  # type: ignore[no-untyped-def]
        assert _engine is standard_engine
        followup_calls.append(result)
        calls.append("__persist_entity_match_followups__")

    monkeypatch.setattr(
        turso_queue, "turso_connect_autocommit", lambda _engine: _FakeConn()
    )
    monkeypatch.setattr(turso_queue, "turso_savepoint", _fake_savepoint)
    monkeypatch.setattr(
        turso_queue,
        "get_research_workbench_engine_from_env",
        lambda: standard_engine,
        raising=False,
    )
    monkeypatch.setattr(
        turso_queue,
        "persist_entity_match_followups",
        _fake_persist,
        raising=False,
    )

    turso_queue.write_assertions_and_mark_done(
        cast(Any, object()),
        post_uid="weibo:4",
        final_status="relevant",
        invest_score=0.9,
        processed_at="2026-03-28 12:00:00",
        model="m",
        prompt_version="topic-prompt-v4",
        archived_at="2026-03-28 12:00:01",
        assertions=[
            {
                "assertion_id": "weibo:4#1",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "他说开始买了。",
                "evidence": "我今天开始买600519了",
                "created_at": "2026-03-28 11:59:00",
                "assertion_entities": [
                    {
                        "entity_key": "stock:600519.SH",
                        "entity_type": "stock",
                        "match_source": "stock_code",
                        "is_primary": 1,
                    }
                ],
            }
        ],
        entity_match_results=[
            EntityMatchResult(
                entities=[],
                relation_candidates=[
                    {
                        "candidate_id": "stock_alias|stock:600519.SH|stock:茅台|alias_of",
                        "relation_type": "stock_alias",
                        "left_key": "stock:600519.SH",
                        "right_key": "stock:茅台",
                        "relation_label": "alias_of",
                        "suggestion_reason": "同条观点里代码和简称一起出现",
                        "evidence_summary": "同条观点里代码和简称一起出现",
                        "score": 0.9,
                        "ai_status": "skipped",
                    }
                ],
                alias_task_keys=[],
            )
        ],
    )

    assert len(followup_calls) == 1
    assert calls.index(INSERT_ASSERTION_ENTITY.strip()) < calls.index(
        "__persist_entity_match_followups__"
    )
    assert calls.index("__persist_entity_match_followups__") < calls.index(
        turso_queue.UPDATE_POST_DONE.strip()
    )


def test_write_assertions_and_mark_done_does_not_mark_done_when_followups_fail(
    monkeypatch,
) -> None:
    calls: list[str] = []

    class _FakeConn:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, _exc_type, _exc, _tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, query, params=None):  # type: ignore[no-untyped-def]
            del params
            calls.append(str(query).strip())
            return self

    @contextmanager
    def _fake_savepoint(_conn):  # type: ignore[no-untyped-def]
        yield

    monkeypatch.setattr(
        turso_queue, "turso_connect_autocommit", lambda _engine: _FakeConn()
    )
    monkeypatch.setattr(turso_queue, "turso_savepoint", _fake_savepoint)
    monkeypatch.setattr(
        turso_queue,
        "get_research_workbench_engine_from_env",
        lambda: object(),
        raising=False,
    )
    monkeypatch.setattr(
        turso_queue,
        "persist_entity_match_followups",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
        raising=False,
    )

    with pytest.raises(RuntimeError, match="boom"):
        turso_queue.write_assertions_and_mark_done(
            cast(Any, object()),
            post_uid="weibo:5",
            final_status="relevant",
            invest_score=0.9,
            processed_at="2026-03-28 12:00:00",
            model="m",
            prompt_version="topic-prompt-v4",
            archived_at="2026-03-28 12:00:01",
            assertions=[
                {
                    "assertion_id": "weibo:5#1",
                    "action": "trade.buy",
                    "action_strength": 2,
                    "summary": "他说开始买了。",
                    "evidence": "我今天开始买600519了",
                    "created_at": "2026-03-28 11:59:00",
                }
            ],
            entity_match_results=[
                EntityMatchResult(
                    entities=[],
                    relation_candidates=[],
                    alias_task_keys=["stock:茅台"],
                )
            ],
        )

    assert turso_queue.UPDATE_POST_DONE.strip() not in calls
