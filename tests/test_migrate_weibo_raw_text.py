from __future__ import annotations

import importlib

import libsql

from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.turso_db import TursoConnection


def _make_conn() -> TursoConnection:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    apply_cloud_schema(conn)
    return conn


def _insert_post(
    conn: TursoConnection,
    *,
    post_uid: str,
    platform: str,
    author: str,
    raw_text: str,
) -> None:
    conn.execute(
        """
INSERT INTO posts(
    post_uid,
    platform,
    platform_post_id,
    author,
    created_at,
    url,
    raw_text,
    final_status,
    archived_at,
    ingested_at
)
VALUES(
    :post_uid,
    :platform,
    :platform_post_id,
    :author,
    :created_at,
    :url,
    :raw_text,
    'relevant',
    '',
    0
)
""",
        {
            "post_uid": post_uid,
            "platform": platform,
            "platform_post_id": post_uid.rsplit(":", 1)[-1],
            "author": author,
            "created_at": "2026-04-05 00:00:00",
            "url": f"https://example.com/{post_uid}",
            "raw_text": raw_text,
        },
    )


def test_build_migration_rows_only_prepares_legacy_weibo_rows() -> None:
    script = importlib.import_module("migrate_weibo_raw_text")

    rows, found_uids = script._build_migration_rows(
        [
            {
                "post_uid": "weibo:1",
                "platform": "weibo",
                "author": "作者",
                "raw_text": "回复@甲:叶//@乙:根\n[图片] https://img.example.com/1.png",
            },
            {
                "post_uid": "weibo:2",
                "platform": "weibo",
                "author": "作者",
                "raw_text": "乙：根\n\n---\n\n作者：叶\n[图片] https://img.example.com/1.png",
            },
            {
                "post_uid": "xueqiu:3",
                "platform": "xueqiu",
                "author": "雪球作者",
                "raw_text": "雪球正文",
            },
        ]
    )

    assert rows == [
        {
            "post_uid": "weibo:1",
            "old_raw_text": "回复@甲:叶//@乙:根\n[图片] https://img.example.com/1.png",
            "new_raw_text": "乙：根\n\n---\n\n作者：叶\n[图片] https://img.example.com/1.png",
            "status": "pending",
            "reason": "",
        },
        {
            "post_uid": "weibo:2",
            "old_raw_text": "乙：根\n\n---\n\n作者：叶\n[图片] https://img.example.com/1.png",
            "new_raw_text": "乙：根\n\n---\n\n作者：叶\n[图片] https://img.example.com/1.png",
            "status": "skipped",
            "reason": "already_migrated",
        },
    ]
    assert found_uids == {"weibo:1", "weibo:2"}


def test_apply_pending_migrations_uses_compare_and_swap_writeback() -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn()
    try:
        script._ensure_migration_table(conn)
        _insert_post(
            conn,
            post_uid="weibo:1",
            platform="weibo",
            author="作者",
            raw_text="回复@甲:叶//@乙:根",
        )
        _insert_post(
            conn,
            post_uid="weibo:2",
            platform="weibo",
            author="作者",
            raw_text="回复@甲:第二条//@乙:第二根",
        )
        script._upsert_migration_rows(
            conn,
            rows=[
                {
                    "post_uid": "weibo:1",
                    "old_raw_text": "回复@甲:叶//@乙:根",
                    "new_raw_text": "乙：根\n\n---\n\n作者：叶",
                    "status": "pending",
                    "reason": "",
                },
                {
                    "post_uid": "weibo:2",
                    "old_raw_text": "回复@甲:第二条//@乙:第二根",
                    "new_raw_text": "乙：第二根\n\n---\n\n作者：第二条",
                    "status": "pending",
                    "reason": "",
                },
            ],
            updated_at="2026-04-05 00:00:00",
        )
        conn.execute(
            """
UPDATE posts
SET raw_text = :raw_text
WHERE post_uid = 'weibo:2'
""",
            {"raw_text": "这一条已经被别的流程改掉了"},
        )

        rows = script._select_pending_migration_rows(conn, batch_size=10)
        stats = script._apply_pending_migration_rows(
            conn,
            rows=rows,
            updated_at="2026-04-05 00:01:00",
        )

        assert stats == {"applied": 1, "conflict": 1}
        assert (
            conn.execute(
                "SELECT raw_text FROM posts WHERE post_uid = 'weibo:1'"
            ).scalar()
            == "乙：根\n\n---\n\n作者：叶"
        )
        assert (
            conn.execute(
                "SELECT raw_text FROM posts WHERE post_uid = 'weibo:2'"
            ).scalar()
            == "这一条已经被别的流程改掉了"
        )
        migration_rows = (
            conn.execute(
                """
SELECT post_uid, status, reason
FROM weibo_raw_text_migration
ORDER BY post_uid ASC
"""
            )
            .mappings()
            .all()
        )
        assert migration_rows == [
            {
                "post_uid": "weibo:1",
                "status": "applied",
                "reason": "",
            },
            {
                "post_uid": "weibo:2",
                "status": "conflict",
                "reason": "raw_text_changed",
            },
        ]
    finally:
        conn.close()
