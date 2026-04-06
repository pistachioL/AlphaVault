from __future__ import annotations

from contextlib import contextmanager
import importlib
from pathlib import Path
import sys

import libsql

from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.turso_db import TursoConnection


def _make_conn() -> TursoConnection:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    apply_cloud_schema(conn)
    return conn


def _make_file_conn(db_path: Path, *, apply_schema: bool) -> TursoConnection:
    conn = TursoConnection(libsql.connect(str(db_path), isolation_level=None))
    if apply_schema:
        apply_cloud_schema(conn)
    return conn


def _migration_table_exists(conn: TursoConnection) -> bool:
    return bool(
        conn.execute(
            """
SELECT 1
FROM sqlite_schema
WHERE type = 'table' AND name = 'weibo_raw_text_migration'
LIMIT 1
"""
        ).fetchone()
    )


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


def test_build_migration_rows_keeps_forward_original_for_plain_repost() -> None:
    script = importlib.import_module("migrate_weibo_raw_text")

    rows, _found_uids = script._build_migration_rows(
        [
            {
                "post_uid": "weibo:plain-repost",
                "platform": "weibo",
                "author": "挖地瓜的超级鹿鼎公",
                "raw_text": (
                    "转发微博\n\n"
                    "[转发原文]\n"
                    "能登地震两周年，1.8万多人还在临时房里凑合过日子。\n\n"
                    '[CSV原始字段]\n{"源用户昵称":"-西野呼啸鹰-","源微博正文":"能登地震两周年，1.8万多人还在临时房里凑合过日子。"}'
                ),
            }
        ]
    )

    assert rows == [
        {
            "post_uid": "weibo:plain-repost",
            "old_raw_text": (
                "转发微博\n\n"
                "[转发原文]\n"
                "能登地震两周年，1.8万多人还在临时房里凑合过日子。\n\n"
                '[CSV原始字段]\n{"源用户昵称":"-西野呼啸鹰-","源微博正文":"能登地震两周年，1.8万多人还在临时房里凑合过日子。"}'
            ),
            "new_raw_text": (
                "-西野呼啸鹰-：能登地震两周年，1.8万多人还在临时房里凑合过日子。\n\n"
                "---\n\n"
                "挖地瓜的超级鹿鼎公：转发微博"
            ),
            "status": "pending",
            "reason": "",
        }
    ]


def test_build_migration_rows_keeps_forward_original_before_comment() -> None:
    script = importlib.import_module("migrate_weibo_raw_text")

    rows, _found_uids = script._build_migration_rows(
        [
            {
                "post_uid": "weibo:comment-repost",
                "platform": "weibo",
                "author": "挖地瓜的超级鹿鼎公",
                "raw_text": (
                    "//@老张评论:你觉得有点不对劲，那就是有妖\n\n"
                    "[微博元信息]\n"
                    "@用户: 老张评论\n\n"
                    "[转发原文]\n"
                    "宋佳的金钱鼠尾辫子造型，提笼架鸟，很有意味儿。\n\n"
                    '[CSV原始字段]\n{"源用户昵称":"疆还是老的辣","源微博正文":"宋佳的金钱鼠尾辫子造型，提笼架鸟，很有意味儿。"}'
                ),
            }
        ]
    )

    assert rows == [
        {
            "post_uid": "weibo:comment-repost",
            "old_raw_text": (
                "//@老张评论:你觉得有点不对劲，那就是有妖\n\n"
                "[微博元信息]\n"
                "@用户: 老张评论\n\n"
                "[转发原文]\n"
                "宋佳的金钱鼠尾辫子造型，提笼架鸟，很有意味儿。\n\n"
                '[CSV原始字段]\n{"源用户昵称":"疆还是老的辣","源微博正文":"宋佳的金钱鼠尾辫子造型，提笼架鸟，很有意味儿。"}'
            ),
            "new_raw_text": (
                "疆还是老的辣：宋佳的金钱鼠尾辫子造型，提笼架鸟，很有意味儿。\n\n"
                "---\n\n"
                "老张评论：你觉得有点不对劲，那就是有妖\n\n"
                "---\n\n"
                "挖地瓜的超级鹿鼎公：转发"
            ),
            "status": "pending",
            "reason": "",
        }
    ]


def test_build_migration_rows_keeps_forward_original_image_for_plain_repost() -> None:
    script = importlib.import_module("migrate_weibo_raw_text")

    rows, _found_uids = script._build_migration_rows(
        [
            {
                "post_uid": "weibo:image-repost",
                "platform": "weibo",
                "author": "挖地瓜的超级鹿鼎公",
                "raw_text": (
                    "转发微博\n\n"
                    '[CSV原始字段]\n{"源用户昵称":"中华之鹰01","源微博正文":" ","源微博原始图片url":"https://img.example.com/source.png"}'
                ),
            }
        ]
    )

    assert rows == [
        {
            "post_uid": "weibo:image-repost",
            "old_raw_text": (
                "转发微博\n\n"
                '[CSV原始字段]\n{"源用户昵称":"中华之鹰01","源微博正文":" ","源微博原始图片url":"https://img.example.com/source.png"}'
            ),
            "new_raw_text": (
                "中华之鹰01：[图片] https://img.example.com/source.png\n\n"
                "---\n\n"
                "挖地瓜的超级鹿鼎公：转发微博"
            ),
            "status": "pending",
            "reason": "",
        }
    ]


def test_build_migration_rows_keeps_forward_original_image_before_comment() -> None:
    script = importlib.import_module("migrate_weibo_raw_text")

    rows, _found_uids = script._build_migration_rows(
        [
            {
                "post_uid": "weibo:image-comment-repost",
                "platform": "weibo",
                "author": "挖地瓜的超级鹿鼎公",
                "raw_text": (
                    "//@胜利主义章北海:？\n\n"
                    "[微博元信息]\n"
                    "@用户: 胜利主义章北海\n\n"
                    '[CSV原始字段]\n{"源用户昵称":"米浴厨","源微博正文":" ","源微博原始图片url":"https://img.example.com/source.png"}'
                ),
            }
        ]
    )

    assert rows == [
        {
            "post_uid": "weibo:image-comment-repost",
            "old_raw_text": (
                "//@胜利主义章北海:？\n\n"
                "[微博元信息]\n"
                "@用户: 胜利主义章北海\n\n"
                '[CSV原始字段]\n{"源用户昵称":"米浴厨","源微博正文":" ","源微博原始图片url":"https://img.example.com/source.png"}'
            ),
            "new_raw_text": (
                "米浴厨：[图片] https://img.example.com/source.png\n\n"
                "---\n\n"
                "胜利主义章北海：？\n\n"
                "---\n\n"
                "挖地瓜的超级鹿鼎公：转发"
            ),
            "status": "pending",
            "reason": "",
        }
    ]


def test_build_migration_rows_keeps_current_author_for_compact_plain_forward() -> None:
    script = importlib.import_module("migrate_weibo_raw_text")

    rows, _found_uids = script._build_migration_rows(
        [
            {
                "post_uid": "weibo:compact-forward",
                "platform": "weibo",
                "author": "挖地瓜的超级鹿鼎公",
                "raw_text": (
                    "//@宽容公正麦卡锡:心脏又不受影响\n\n"
                    "[微博元信息]\n"
                    "@用户: 宽容公正麦卡锡\n\n"
                    '[CSV原始字段]\n{"源用户昵称":"爱国青年刘战神","源微博正文":" ","源微博原始图片url":"https://img.example.com/source.png"}'
                ),
            }
        ]
    )

    assert rows == [
        {
            "post_uid": "weibo:compact-forward",
            "old_raw_text": (
                "//@宽容公正麦卡锡:心脏又不受影响\n\n"
                "[微博元信息]\n"
                "@用户: 宽容公正麦卡锡\n\n"
                '[CSV原始字段]\n{"源用户昵称":"爱国青年刘战神","源微博正文":" ","源微博原始图片url":"https://img.example.com/source.png"}'
            ),
            "new_raw_text": (
                "爱国青年刘战神：[图片] https://img.example.com/source.png\n\n"
                "---\n\n"
                "宽容公正麦卡锡：心脏又不受影响\n\n"
                "---\n\n"
                "挖地瓜的超级鹿鼎公：转发"
            ),
            "status": "pending",
            "reason": "",
        }
    ]


def test_build_migration_rows_keeps_current_post_image_from_csv_fields() -> None:
    script = importlib.import_module("migrate_weibo_raw_text")

    rows, _found_uids = script._build_migration_rows(
        [
            {
                "post_uid": "weibo:current-image",
                "platform": "weibo",
                "author": "挖地瓜的超级鹿鼎公",
                "raw_text": (
                    "不错\n\n"
                    '[CSV原始字段]\n{"原始图片url":"https://img.example.com/current.png"}'
                ),
            }
        ]
    )

    assert rows == [
        {
            "post_uid": "weibo:current-image",
            "old_raw_text": (
                "不错\n\n"
                '[CSV原始字段]\n{"原始图片url":"https://img.example.com/current.png"}'
            ),
            "new_raw_text": (
                "挖地瓜的超级鹿鼎公：不错\n[图片] https://img.example.com/current.png"
            ),
            "status": "pending",
            "reason": "",
        }
    ]


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


def test_prepare_scan_only_saves_legacy_weibo_rows_to_migration_table(
    monkeypatch,
) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn()

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield conn

    monkeypatch.setattr(script, "turso_connect_autocommit", _fake_connect)

    try:
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
            raw_text="乙：根\n\n---\n\n作者：叶",
        )

        script._prepare_scan(
            object(),
            batch_size=10,
            limit=0,
            sleep_sec=0.0,
            dry_run=False,
            verbose=False,
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
                "status": "pending",
                "reason": "",
            }
        ]
    finally:
        conn.close()


def test_prepare_scan_limit_counts_only_legacy_targets(
    monkeypatch,
    capsys,
) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn()

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield conn

    monkeypatch.setattr(script, "turso_connect_autocommit", _fake_connect)

    try:
        _insert_post(
            conn,
            post_uid="weibo:1",
            platform="weibo",
            author="作者",
            raw_text="乙：根\n\n---\n\n作者：叶",
        )
        _insert_post(
            conn,
            post_uid="weibo:2",
            platform="weibo",
            author="作者",
            raw_text="回复@甲:叶//@乙:根",
        )

        script._prepare_scan(
            object(),
            batch_size=1,
            limit=1,
            sleep_sec=0.0,
            dry_run=False,
            verbose=False,
        )

        out = capsys.readouterr().out
        assert "source_target=1" in out
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
                "post_uid": "weibo:2",
                "status": "pending",
                "reason": "",
            }
        ]
    finally:
        conn.close()


def test_apply_scan_dry_run_does_not_create_migration_table(monkeypatch) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn()

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield conn

    monkeypatch.setattr(script, "turso_connect_autocommit", _fake_connect)

    try:
        assert _migration_table_exists(conn) is False

        script._apply_scan(
            object(),
            batch_size=10,
            limit=0,
            dry_run=True,
            sleep_sec=0.0,
        )

        assert _migration_table_exists(conn) is False
    finally:
        conn.close()


def test_apply_by_post_uids_dry_run_does_not_create_migration_table(
    monkeypatch,
) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn()

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield conn

    monkeypatch.setattr(script, "turso_connect_autocommit", _fake_connect)

    try:
        assert _migration_table_exists(conn) is False

        script._apply_by_post_uids(
            object(),
            post_uids=["weibo:1"],
            batch_size=10,
            dry_run=True,
            sleep_sec=0.0,
            verbose=False,
        )

        assert _migration_table_exists(conn) is False
    finally:
        conn.close()


def test_main_db_path_updates_local_db_without_migration_table(
    monkeypatch,
    tmp_path,
) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    db_path = tmp_path / "weibo_rebuild.db"
    conn = _make_file_conn(db_path, apply_schema=True)

    try:
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
            raw_text="乙：根\n\n---\n\n作者：叶",
        )
    finally:
        conn.close()

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)

    def _fail_get_turso_engine():  # type: ignore[no-untyped-def]
        raise AssertionError("should_not_use_turso_env")

    monkeypatch.setattr(script, "get_turso_engine_from_env", _fail_get_turso_engine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "migrate_weibo_raw_text.py",
            "--db-path",
            str(db_path),
            "--batch-size",
            "10",
        ],
    )

    script.main()

    check_conn = _make_file_conn(db_path, apply_schema=False)
    try:
        assert (
            check_conn.execute(
                "SELECT raw_text FROM posts WHERE post_uid = 'weibo:1'"
            ).scalar()
            == "乙：根\n\n---\n\n作者：叶"
        )
        assert (
            check_conn.execute(
                "SELECT raw_text FROM posts WHERE post_uid = 'weibo:2'"
            ).scalar()
            == "乙：根\n\n---\n\n作者：叶"
        )
        assert _migration_table_exists(check_conn) is False
    finally:
        check_conn.close()
