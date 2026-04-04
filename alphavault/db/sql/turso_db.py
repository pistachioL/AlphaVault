from __future__ import annotations

SQL_BEGIN = "BEGIN"
SQL_COMMIT = "COMMIT"
SQL_ROLLBACK = "ROLLBACK"


def drop_table_if_exists(table: str) -> str:
    return f"DROP TABLE IF EXISTS {table}"


def create_topic_cluster_topics_v2_table(table: str) -> str:
    return f"""
CREATE TABLE {table} (
    topic_key TEXT NOT NULL,
    cluster_key TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    created_at TEXT NOT NULL,
    PRIMARY KEY (topic_key, cluster_key)
);
"""


def copy_topic_cluster_topics(src_table: str, dst_table: str) -> str:
    return f"""
INSERT OR IGNORE INTO {dst_table}(
    topic_key, cluster_key, source, confidence, created_at
)
SELECT topic_key, cluster_key, source, confidence, created_at
FROM {src_table}
"""


def select_count_as_n(table: str) -> str:
    return f"SELECT COUNT(1) AS n FROM {table}"


def drop_table(table: str) -> str:
    return f"DROP TABLE {table}"


def rename_table(src_table: str, dst_table: str) -> str:
    return f"ALTER TABLE {src_table} RENAME TO {dst_table}"


def create_topic_clusters_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    cluster_key TEXT PRIMARY KEY,
    cluster_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def create_topic_cluster_topics_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    topic_key TEXT NOT NULL,
    cluster_key TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    created_at TEXT NOT NULL,
    PRIMARY KEY (topic_key, cluster_key)
);
"""


def create_topic_cluster_post_overrides_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    post_uid TEXT PRIMARY KEY,
    cluster_key TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    created_at TEXT NOT NULL
);
"""


def topic_cluster_index_statements(
    topics_table: str, post_overrides_table: str
) -> list[str]:
    return [
        f"CREATE INDEX IF NOT EXISTS idx_{topics_table}_cluster_key ON {topics_table}(cluster_key)",
        f"CREATE INDEX IF NOT EXISTS idx_{post_overrides_table}_cluster_key ON {post_overrides_table}(cluster_key)",
    ]


CREATE_POSTS_TABLE = """
CREATE TABLE IF NOT EXISTS posts (
    post_uid TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    platform_post_id TEXT NOT NULL,
    author TEXT NOT NULL,
    created_at TEXT NOT NULL,
    url TEXT NOT NULL,
    raw_text TEXT NOT NULL,
    final_status TEXT NOT NULL CHECK (final_status IN ('relevant', 'irrelevant')),
    invest_score REAL,
    processed_at TEXT,
    model TEXT,
    prompt_version TEXT,
    archived_at TEXT NOT NULL
);
"""

CREATE_ASSERTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS assertions (
    post_uid TEXT NOT NULL,
    idx INTEGER NOT NULL CHECK (idx >= 1),
    speaker TEXT NOT NULL DEFAULT '',
    relation_to_topic TEXT NOT NULL DEFAULT 'new',
    topic_key TEXT NOT NULL,
    action TEXT NOT NULL,
    action_strength INTEGER NOT NULL CHECK (action_strength BETWEEN 0 AND 3),
    summary TEXT NOT NULL,
    evidence TEXT NOT NULL,
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    stock_codes_json TEXT NOT NULL DEFAULT '[]',
    stock_names_json TEXT NOT NULL DEFAULT '[]',
    industries_json TEXT NOT NULL DEFAULT '[]',
    commodities_json TEXT NOT NULL DEFAULT '[]',
    indices_json TEXT NOT NULL DEFAULT '[]',
    keywords_json TEXT NOT NULL DEFAULT '[]',
    UNIQUE(post_uid, idx)
);
"""

CREATE_ASSERTION_MENTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS assertion_mentions (
    post_uid TEXT NOT NULL,
    assertion_idx INTEGER NOT NULL CHECK (assertion_idx >= 1),
    mention_idx INTEGER NOT NULL CHECK (mention_idx >= 1),
    mention_text TEXT NOT NULL,
    mention_type TEXT NOT NULL,
    evidence TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    PRIMARY KEY (post_uid, assertion_idx, mention_idx)
);
"""

CREATE_ASSERTION_ENTITIES_TABLE = """
CREATE TABLE IF NOT EXISTS assertion_entities (
    post_uid TEXT NOT NULL,
    assertion_idx INTEGER NOT NULL CHECK (assertion_idx >= 1),
    entity_idx INTEGER NOT NULL CHECK (entity_idx >= 1),
    entity_key TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    source_mention_text TEXT NOT NULL DEFAULT '',
    source_mention_type TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    PRIMARY KEY (post_uid, assertion_idx, entity_idx)
);
"""

SELECT_ASSERTIONS_FOR_POST_UID = """
SELECT idx, speaker, relation_to_topic, topic_key, action, action_strength, summary,
       evidence, evidence_refs_json, confidence, stock_codes_json, stock_names_json,
       industries_json, commodities_json, indices_json, keywords_json
FROM assertions
WHERE post_uid = :post_uid
ORDER BY idx ASC
"""

SELECT_ASSERTION_MENTIONS_FOR_POST_UID = """
SELECT assertion_idx, mention_idx, mention_text, mention_type, evidence, confidence
FROM assertion_mentions
WHERE post_uid = :post_uid
ORDER BY assertion_idx ASC, mention_idx ASC
"""

SELECT_ASSERTION_ENTITIES_FOR_POST_UID = """
SELECT assertion_idx, entity_idx, entity_key, entity_type,
       source_mention_text, source_mention_type, confidence
FROM assertion_entities
WHERE post_uid = :post_uid
ORDER BY assertion_idx ASC, entity_idx ASC
"""

CLOUD_SCHEMA_INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_posts_author_created_at ON posts(author, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_posts_created_at_post_uid ON posts(created_at, post_uid)",
    "CREATE INDEX IF NOT EXISTS idx_posts_platform_post_id ON posts(platform_post_id)",
    "CREATE INDEX IF NOT EXISTS idx_assertions_topic_key ON assertions(topic_key)",
    "CREATE INDEX IF NOT EXISTS idx_assertions_action ON assertions(action)",
    "CREATE INDEX IF NOT EXISTS idx_assertions_topic_action_post_uid ON assertions(topic_key, action, post_uid)",
    "CREATE INDEX IF NOT EXISTS idx_assertion_mentions_text ON assertion_mentions(mention_text)",
    "CREATE INDEX IF NOT EXISTS idx_assertion_mentions_type_text ON assertion_mentions(mention_type, mention_text)",
    "CREATE INDEX IF NOT EXISTS idx_assertion_entities_key ON assertion_entities(entity_key)",
    "CREATE INDEX IF NOT EXISTS idx_assertion_entities_type_key ON assertion_entities(entity_type, entity_key)",
    (
        "CREATE INDEX IF NOT EXISTS idx_assertions_trade_stock_topic_key "
        "ON assertions(topic_key) "
        "WHERE action LIKE 'trade.%' AND topic_key LIKE 'stock:%'"
    ),
]


__all__ = [
    "SELECT_ASSERTIONS_FOR_POST_UID",
    "SELECT_ASSERTION_MENTIONS_FOR_POST_UID",
    "SELECT_ASSERTION_ENTITIES_FOR_POST_UID",
]
