-- AlphaVault source Turso schema
-- 给 weibo / xueqiu 这类 source 库使用

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
    archived_at TEXT NOT NULL DEFAULT '',
    ingested_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS assertions (
    assertion_id TEXT PRIMARY KEY,
    post_uid TEXT NOT NULL,
    idx INTEGER NOT NULL CHECK (idx >= 1),
    action TEXT NOT NULL,
    action_strength INTEGER NOT NULL CHECK (action_strength BETWEEN 0 AND 3),
    summary TEXT NOT NULL,
    evidence TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT '',
    UNIQUE(post_uid, idx)
);

CREATE TABLE IF NOT EXISTS assertion_mentions (
    assertion_id TEXT NOT NULL,
    mention_seq INTEGER NOT NULL CHECK (mention_seq >= 1),
    mention_text TEXT NOT NULL,
    mention_norm TEXT NOT NULL DEFAULT '',
    mention_type TEXT NOT NULL,
    evidence TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    PRIMARY KEY (assertion_id, mention_seq)
);

CREATE TABLE IF NOT EXISTS assertion_entities (
    assertion_id TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    match_source TEXT NOT NULL DEFAULT '',
    is_primary INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (assertion_id, entity_key)
);

CREATE TABLE IF NOT EXISTS topic_clusters (
    cluster_key TEXT PRIMARY KEY,
    cluster_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS topic_cluster_topics (
    topic_key TEXT NOT NULL,
    cluster_key TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    created_at TEXT NOT NULL,
    PRIMARY KEY (topic_key, cluster_key)
);

CREATE TABLE IF NOT EXISTS topic_cluster_post_overrides (
    post_uid TEXT PRIMARY KEY,
    cluster_key TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_page_snapshot (
    entity_key TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL DEFAULT '',
    header_json TEXT NOT NULL DEFAULT '{}',
    signal_top_json TEXT NOT NULL DEFAULT '[]',
    related_json TEXT NOT NULL DEFAULT '[]',
    counters_json TEXT NOT NULL DEFAULT '{}',
    content_hash TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_dirty (
    job_type TEXT NOT NULL,
    target_key TEXT NOT NULL,
    reason_mask INTEGER NOT NULL DEFAULT 0,
    dirty_since TEXT NOT NULL DEFAULT '',
    last_dirty_at TEXT NOT NULL DEFAULT '',
    claim_until TEXT NOT NULL DEFAULT '',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (job_type, target_key)
);

CREATE TABLE IF NOT EXISTS worker_cursor (
    state_key TEXT PRIMARY KEY,
    cursor TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_locks (
    lock_key TEXT PRIMARY KEY,
    locked_until INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_posts_created_at
    ON posts(created_at);

CREATE INDEX IF NOT EXISTS idx_posts_author_created_at
    ON posts(author, created_at);

CREATE INDEX IF NOT EXISTS idx_posts_created_at_post_uid
    ON posts(created_at, post_uid);

CREATE INDEX IF NOT EXISTS idx_posts_platform_post_id
    ON posts(platform_post_id);

CREATE INDEX IF NOT EXISTS idx_assertions_post_uid
    ON assertions(post_uid);

CREATE INDEX IF NOT EXISTS idx_assertions_action
    ON assertions(action);

CREATE INDEX IF NOT EXISTS idx_assertions_created_at
    ON assertions(created_at);

CREATE INDEX IF NOT EXISTS idx_assertion_mentions_text
    ON assertion_mentions(mention_text);

CREATE INDEX IF NOT EXISTS idx_assertion_mentions_type_norm
    ON assertion_mentions(mention_type, mention_norm);

CREATE INDEX IF NOT EXISTS idx_assertion_entities_key
    ON assertion_entities(entity_key);

CREATE INDEX IF NOT EXISTS idx_assertion_entities_type_key
    ON assertion_entities(entity_type, entity_key);

CREATE INDEX IF NOT EXISTS idx_topic_cluster_topics_cluster_key
    ON topic_cluster_topics(cluster_key);

CREATE INDEX IF NOT EXISTS idx_topic_cluster_post_overrides_cluster_key
    ON topic_cluster_post_overrides(cluster_key);

CREATE INDEX IF NOT EXISTS idx_entity_page_snapshot_updated
    ON entity_page_snapshot(updated_at);

CREATE INDEX IF NOT EXISTS idx_projection_dirty_claimable
    ON projection_dirty(job_type, claim_until, dirty_since, last_dirty_at, target_key);
