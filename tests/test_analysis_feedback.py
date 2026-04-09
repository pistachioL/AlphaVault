from __future__ import annotations


from alphavault.constants import SCHEMA_WEIBO
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection


def _source_conn(pg_conn) -> PostgresConnection:
    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)
    return PostgresConnection(pg_conn, schema_name=SCHEMA_WEIBO)


def _insert_post(conn: PostgresConnection, *, post_uid: str = "weibo:1") -> None:
    conn.execute(
        """
INSERT INTO weibo.posts(
  post_uid, platform, platform_post_id, author, created_at, url, raw_text,
  final_status, processed_at, model, prompt_version, archived_at, ingested_at
)
VALUES (
  :post_uid, 'weibo', '1001', '老王', '2026-04-09 10:00:00',
  'https://example.com/post/1001', '茅台我觉得可以买',
  'relevant', '2026-04-09 10:05:00', 'test-model', 'topic-prompt-v4',
  '2026-04-09 10:06:00', 1
)
""",
        {"post_uid": post_uid},
    )


def test_submit_post_analysis_feedback_supersedes_old_pending_and_keeps_new_pending(
    monkeypatch,
    pg_conn,
) -> None:
    from alphavault.db import analysis_feedback

    conn = _source_conn(pg_conn)
    _insert_post(conn)
    push_calls: list[dict[str, object]] = []
    now_values = iter(
        [
            "2026-04-09 11:00:00",
            "2026-04-09 11:01:00",
        ]
    )

    monkeypatch.setattr(
        analysis_feedback,
        "_source_engine_for_post_uid",
        lambda _post_uid: conn,
    )
    monkeypatch.setattr(
        analysis_feedback,
        "_load_feedback_redis_runtime",
        lambda *, source_name: (object(), f"queue:{source_name}"),
    )

    def _fake_push_status(_client, queue_key, **kwargs) -> str:  # type: ignore[no-untyped-def]
        push_calls.append({"queue_key": queue_key, **kwargs})
        return "pushed"

    monkeypatch.setattr(
        analysis_feedback,
        "redis_try_push_ai_dedup_status",
        _fake_push_status,
    )
    monkeypatch.setattr(
        analysis_feedback,
        "now_str",
        lambda: next(now_values),
    )

    first = analysis_feedback.submit_post_analysis_feedback(
        post_uid="weibo:1",
        feedback_tag="摘要错了",
        feedback_note="第一条备注",
        entrypoint="stock_research",
    )
    second = analysis_feedback.submit_post_analysis_feedback(
        post_uid="weibo:1",
        feedback_tag="动作错了",
        feedback_note="第二条备注",
        entrypoint="stock_research",
    )

    rows = (
        conn.execute(
            """
SELECT feedback_tag, feedback_note, feedback_status
FROM weibo.post_analysis_feedback
WHERE post_uid = :post_uid
ORDER BY submitted_at ASC
""",
            {"post_uid": "weibo:1"},
        )
        .mappings()
        .fetchall()
    )

    assert first["queue_status"] == "pushed"
    assert second["queue_status"] == "pushed"
    assert rows == [
        {
            "feedback_tag": "摘要错了",
            "feedback_note": "第一条备注",
            "feedback_status": "superseded",
        },
        {
            "feedback_tag": "动作错了",
            "feedback_note": "第二条备注",
            "feedback_status": "pending",
        },
    ]
    assert push_calls[0]["queue_key"] == "queue:weibo"
    assert push_calls[0]["post_uid"] == "weibo:1"
    push_payload = push_calls[0]["payload"]
    assert isinstance(push_payload, dict)
    assert push_payload["raw_text"] == "茅台我觉得可以买"


def test_submit_post_analysis_feedback_marks_queue_failed_on_push_error(
    monkeypatch,
    pg_conn,
) -> None:
    from alphavault.db import analysis_feedback

    conn = _source_conn(pg_conn)
    _insert_post(conn)

    monkeypatch.setattr(
        analysis_feedback,
        "_source_engine_for_post_uid",
        lambda _post_uid: conn,
    )
    monkeypatch.setattr(
        analysis_feedback,
        "_load_feedback_redis_runtime",
        lambda *, source_name: (object(), f"queue:{source_name}"),
    )
    monkeypatch.setattr(
        analysis_feedback,
        "redis_try_push_ai_dedup_status",
        lambda _client, _queue_key, **_kwargs: "error",
    )
    monkeypatch.setattr(
        analysis_feedback,
        "now_str",
        lambda: "2026-04-09 11:05:00",
    )

    result = analysis_feedback.submit_post_analysis_feedback(
        post_uid="weibo:1",
        feedback_tag="其他",
        feedback_note="Redis 挂了",
        entrypoint="stock_research",
    )

    row = (
        conn.execute(
            """
SELECT feedback_status
FROM weibo.post_analysis_feedback
WHERE post_uid = :post_uid
LIMIT 1
""",
            {"post_uid": "weibo:1"},
        )
        .mappings()
        .fetchone()
    )

    assert result["queue_status"] == "error"
    assert row == {"feedback_status": "queue_failed"}


def test_load_latest_pending_feedback_ignores_superseded_and_queue_failed(
    pg_conn,
) -> None:
    from alphavault.db import analysis_feedback

    conn = _source_conn(pg_conn)
    conn.execute(
        """
INSERT INTO weibo.post_analysis_feedback(
  feedback_id, post_uid, feedback_tag, feedback_note, feedback_status,
  entrypoint, submitted_at, applied_at
)
VALUES
  ('f1', 'weibo:1', '摘要错了', '旧提示', 'superseded', 'stock_research', '2026-04-09 10:00:00', ''),
  ('f2', 'weibo:1', '其他', '失败提示', 'queue_failed', 'stock_research', '2026-04-09 10:01:00', ''),
  ('f3', 'weibo:1', '动作错了', '最新提示', 'pending', 'stock_research', '2026-04-09 10:02:00', '')
"""
    )

    row = analysis_feedback.load_latest_pending_feedback(conn, post_uid="weibo:1")

    assert row == {
        "feedback_id": "f3",
        "post_uid": "weibo:1",
        "feedback_tag": "动作错了",
        "feedback_note": "最新提示",
        "feedback_status": "pending",
        "entrypoint": "stock_research",
        "submitted_at": "2026-04-09 10:02:00",
        "applied_at": "",
    }


def test_mark_feedback_applied_updates_status_and_applied_at(pg_conn) -> None:
    from alphavault.db import analysis_feedback

    conn = _source_conn(pg_conn)
    conn.execute(
        """
INSERT INTO weibo.post_analysis_feedback(
  feedback_id, post_uid, feedback_tag, feedback_note, feedback_status,
  entrypoint, submitted_at, applied_at
)
VALUES (
  'f1', 'weibo:1', '摘要错了', '待应用', 'pending',
  'stock_research', '2026-04-09 10:00:00', ''
)
"""
    )

    updated = analysis_feedback.mark_feedback_applied(
        conn,
        feedback_id="f1",
        applied_at="2026-04-09 10:03:00",
    )
    row = (
        conn.execute(
            """
SELECT feedback_status, applied_at
FROM weibo.post_analysis_feedback
WHERE feedback_id = :feedback_id
""",
            {"feedback_id": "f1"},
        )
        .mappings()
        .fetchone()
    )

    assert updated == 1
    assert row == {
        "feedback_status": "applied",
        "applied_at": "2026-04-09 10:03:00",
    }
