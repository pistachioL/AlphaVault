from __future__ import annotations

from contextlib import contextmanager
import importlib
from pathlib import Path
import sys
from types import SimpleNamespace

import libsql
import psycopg

from alphavault.constants import SCHEMA_STANDARD, SCHEMA_WEIBO, SCHEMA_XUEQIU
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection
from alphavault.db.libsql_db import LibsqlConnection as TursoConnection


def _make_schema_conn(
    postgres_dsn: str,
    *,
    schema_name: str,
    target: str,
) -> PostgresConnection:
    conn = PostgresConnection(psycopg.connect(postgres_dsn), schema_name=schema_name)
    apply_cloud_schema(conn, target=target, schema_name=schema_name)
    conn.execute(f"SET search_path TO {schema_name}")
    return conn


def _make_conn(postgres_dsn: str) -> PostgresConnection:
    conn = _make_schema_conn(
        postgres_dsn,
        schema_name=SCHEMA_WEIBO,
        target="source",
    )
    conn.execute(f"SET search_path TO {SCHEMA_WEIBO}")
    return conn


def _make_file_conn(db_path: Path, *, apply_schema: bool) -> TursoConnection:
    conn = TursoConnection(libsql.connect(str(db_path), isolation_level=None))
    if apply_schema:
        _apply_local_posts_schema(conn)
    return conn


def _apply_local_posts_schema(conn: TursoConnection) -> None:
    conn.execute(
        """
CREATE TABLE IF NOT EXISTS posts(
    post_uid TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    platform_post_id TEXT NOT NULL,
    author TEXT NOT NULL,
    created_at TEXT NOT NULL,
    url TEXT NOT NULL,
    raw_text TEXT NOT NULL,
    final_status TEXT NOT NULL,
    archived_at TEXT NOT NULL,
    ingested_at INTEGER NOT NULL
)
"""
    )


def _apply_local_assertions_schema(conn: TursoConnection) -> None:
    conn.execute(
        """
CREATE TABLE IF NOT EXISTS assertions(
    assertion_id TEXT PRIMARY KEY,
    post_uid TEXT NOT NULL,
    idx INTEGER NOT NULL,
    action TEXT NOT NULL,
    action_strength INTEGER NOT NULL,
    summary TEXT NOT NULL,
    evidence TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT ''
)
"""
    )


def _apply_local_relations_schema(conn: TursoConnection) -> None:
    conn.execute(
        """
CREATE TABLE IF NOT EXISTS relations(
    relation_id TEXT PRIMARY KEY,
    relation_type TEXT NOT NULL,
    left_key TEXT NOT NULL,
    right_key TEXT NOT NULL,
    relation_label TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""
    )


def _migration_table_exists(conn: TursoConnection | PostgresConnection) -> bool:
    schema_name = str(getattr(conn, "schema_name", "") or "").strip()
    if schema_name:
        qualified_name = f"{schema_name}.weibo_raw_text_migration"
        return bool(
            conn.execute(
                "SELECT to_regclass(:qualified_name) IS NOT NULL",
                {"qualified_name": qualified_name},
            ).scalar()
        )
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
    conn: TursoConnection | PostgresConnection,
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


def _insert_assertion(
    conn: TursoConnection | PostgresConnection,
    *,
    assertion_id: str,
    post_uid: str,
    idx: int,
    summary: str,
) -> None:
    conn.execute(
        """
INSERT INTO assertions(
    assertion_id,
    post_uid,
    idx,
    action,
    action_strength,
    summary,
    evidence,
    created_at
)
VALUES(
    :assertion_id,
    :post_uid,
    :idx,
    'buy',
    2,
    :summary,
    'evidence',
    '2026-04-05 00:00:00'
)
""",
        {
            "assertion_id": assertion_id,
            "post_uid": post_uid,
            "idx": idx,
            "summary": summary,
        },
    )


def _insert_relation(
    conn: TursoConnection | PostgresConnection,
    *,
    relation_id: str,
    left_key: str,
    right_key: str,
) -> None:
    conn.execute(
        """
INSERT INTO relations(
    relation_id,
    relation_type,
    left_key,
    right_key,
    relation_label,
    source,
    created_at,
    updated_at
)
VALUES(
    :relation_id,
    'stock_alias',
    :left_key,
    :right_key,
    'alias_of',
    'weibo',
    '2026-04-05 00:00:00',
    '2026-04-05 00:00:00'
)
""",
        {
            "relation_id": relation_id,
            "left_key": left_key,
            "right_key": right_key,
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


def test_apply_pending_migrations_uses_compare_and_swap_writeback(
    postgres_dsn: str,
) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn(postgres_dsn)
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
    postgres_dsn: str,
) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn(postgres_dsn)

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield conn

    monkeypatch.setattr(script, "postgres_connect_autocommit", _fake_connect)

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
    postgres_dsn: str,
) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn(postgres_dsn)

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield conn

    monkeypatch.setattr(script, "postgres_connect_autocommit", _fake_connect)

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


def test_apply_scan_dry_run_does_not_create_migration_table(
    monkeypatch,
    postgres_dsn: str,
) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn(postgres_dsn)

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield conn

    monkeypatch.setattr(script, "postgres_connect_autocommit", _fake_connect)

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
    postgres_dsn: str,
) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    conn = _make_conn(postgres_dsn)

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield conn

    monkeypatch.setattr(script, "postgres_connect_autocommit", _fake_connect)

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

    def _fail_get_postgres_engine():  # type: ignore[no-untyped-def]
        raise AssertionError("should_not_use_postgres_env")

    monkeypatch.setattr(
        script,
        "get_postgres_engine_from_env",
        _fail_get_postgres_engine,
    )
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


def test_migrate_turso_to_postgres_copies_posts_assertions_and_relations(
    postgres_dsn: str,
    tmp_path,
) -> None:
    script = importlib.import_module("migrate_turso_to_postgres")
    weibo_db_path = tmp_path / "weibo_old.db"
    xueqiu_db_path = tmp_path / "xueqiu_old.db"

    weibo_old_conn = _make_file_conn(weibo_db_path, apply_schema=True)
    try:
        _apply_local_assertions_schema(weibo_old_conn)
        _apply_local_relations_schema(weibo_old_conn)
        _insert_post(
            weibo_old_conn,
            post_uid="weibo:1",
            platform="weibo",
            author="作者",
            raw_text="微博正文",
        )
        _insert_assertion(
            weibo_old_conn,
            assertion_id="assertion:1",
            post_uid="weibo:1",
            idx=1,
            summary="看多",
        )
        _insert_relation(
            weibo_old_conn,
            relation_id="rel:1",
            left_key="stock:600519.SH",
            right_key="stock:茅台",
        )
    finally:
        weibo_old_conn.close()

    xueqiu_old_conn = _make_file_conn(xueqiu_db_path, apply_schema=False)
    xueqiu_old_conn.close()

    summary = script.migrate_all(
        turso_weibo_url=str(weibo_db_path),
        turso_xueqiu_url=str(xueqiu_db_path),
        postgres_dsn=postgres_dsn,
    )

    weibo_new_conn = _make_schema_conn(
        postgres_dsn,
        schema_name=SCHEMA_WEIBO,
        target="source",
    )
    xueqiu_new_conn = _make_schema_conn(
        postgres_dsn,
        schema_name=SCHEMA_XUEQIU,
        target="source",
    )
    standard_conn = _make_schema_conn(
        postgres_dsn,
        schema_name=SCHEMA_STANDARD,
        target="standard",
    )
    try:
        assert summary["weibo_posts"] == 1
        assert summary["weibo_assertions"] == 1
        assert summary["standard_relations"] == 1
        assert weibo_new_conn.execute("SELECT count(*) FROM posts").scalar() == 1
        assert weibo_new_conn.execute("SELECT count(*) FROM assertions").scalar() == 1
        assert xueqiu_new_conn.execute("SELECT count(*) FROM posts").scalar() == 0
        assert standard_conn.execute("SELECT count(*) FROM relations").scalar() == 1
    finally:
        weibo_new_conn.close()
        xueqiu_new_conn.close()
        standard_conn.close()


def test_get_postgres_engine_from_env_uses_postgres_source(monkeypatch) -> None:
    script = importlib.import_module("migrate_weibo_raw_text")
    source = SimpleNamespace(dsn="postgresql://db", schema=SCHEMA_WEIBO)
    seen_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        script,
        "require_postgres_source_from_env",
        lambda _name: source,
        raising=False,
    )

    def _fake_ensure(dsn: str, *, schema_name: str = "") -> str:
        seen_calls.append((dsn, schema_name))
        return "engine"

    monkeypatch.setattr(script, "_ensure_postgres_engine", _fake_ensure, raising=False)

    assert script.get_postgres_engine_from_env() == "engine"
    assert seen_calls == [("postgresql://db", SCHEMA_WEIBO)]


def test_copy_table_group_reads_source_rows_in_batches(monkeypatch) -> None:
    script = importlib.import_module("migrate_turso_to_postgres")

    class _FakeSelectCursor:
        description = (("post_uid",), ("platform",))

        def __init__(self) -> None:
            self.fetchmany_calls: list[int] = []
            self._batches = [
                [("weibo:1", "weibo")],
                [("weibo:2", "weibo")],
                [],
            ]

        def fetchmany(self, size: int) -> list[tuple[str, str]]:
            self.fetchmany_calls.append(size)
            return self._batches.pop(0)

        def fetchall(self) -> list[tuple[str, str]]:
            raise AssertionError("should_not_fetchall")

    class _FakeSourceConn:
        def __init__(self, select_cursor: _FakeSelectCursor) -> None:
            self._select_cursor = select_cursor

        def execute(self, query: str, params=None):  # type: ignore[no-untyped-def]
            del params
            if "FROM sqlite_schema" in query:
                return SimpleNamespace(fetchone=lambda: (1,))
            if query == "PRAGMA table_info(posts)":
                return SimpleNamespace(
                    fetchall=lambda: [(0, "post_uid"), (1, "platform")]
                )
            if query.startswith("SELECT post_uid, platform FROM posts"):
                return self._select_cursor
            raise AssertionError(f"unexpected_query:{query}")

    class _FakeTargetConn:
        def __init__(self) -> None:
            self.executed: list[tuple[str, object]] = []

        def execute(self, query: str, params=None):  # type: ignore[no-untyped-def]
            self.executed.append((query, params))
            return SimpleNamespace(scalar=lambda: None)

    def _fake_run_postgres_transaction(conn, fn):  # type: ignore[no-untyped-def]
        return fn(conn)

    monkeypatch.setattr(
        script,
        "run_postgres_transaction",
        _fake_run_postgres_transaction,
    )

    select_cursor = _FakeSelectCursor()
    source_conn = _FakeSourceConn(select_cursor)
    target_conn = _FakeTargetConn()

    counts = script._copy_table_group(
        source_conn=source_conn,
        target_conn=target_conn,
        schema_name=SCHEMA_WEIBO,
        specs=(("posts", ("post_uid", "platform")),),
        batch_size=1,
    )

    assert counts == {"posts": 2}
    assert select_cursor.fetchmany_calls == [1, 1, 1]
    assert [
        params for _query, params in target_conn.executed if isinstance(params, list)
    ] == [
        [{"post_uid": "weibo:1", "platform": "weibo"}],
        [{"post_uid": "weibo:2", "platform": "weibo"}],
    ]
