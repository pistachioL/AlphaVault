-- AlphaVault standard Postgres schema
-- 给 standard schema 使用

CREATE SCHEMA IF NOT EXISTS {{schema_name}};

CREATE TABLE IF NOT EXISTS {{schema_name}}.security_master (
    stock_key TEXT PRIMARY KEY,
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    official_name TEXT NOT NULL,
    official_name_norm TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {{schema_name}}.relations (
    relation_id TEXT PRIMARY KEY,
    relation_type TEXT NOT NULL,
    left_key TEXT NOT NULL,
    right_key TEXT NOT NULL,
    relation_label TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(relation_type, left_key, right_key, relation_label)
);

CREATE TABLE IF NOT EXISTS {{schema_name}}.relation_candidates (
    candidate_id TEXT PRIMARY KEY,
    relation_type TEXT NOT NULL,
    left_key TEXT NOT NULL,
    right_key TEXT NOT NULL,
    relation_label TEXT NOT NULL,
    suggestion_reason TEXT NOT NULL DEFAULT '',
    evidence_summary TEXT NOT NULL DEFAULT '',
    score REAL NOT NULL DEFAULT 0,
    ai_status TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {{schema_name}}.alias_resolve_tasks (
    alias_key TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    sample_post_uid TEXT NOT NULL DEFAULT '',
    sample_evidence TEXT NOT NULL DEFAULT '',
    sample_raw_text_excerpt TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {{schema_name}}.homework_trade_feed (
    view_key TEXT PRIMARY KEY,
    header_json TEXT NOT NULL DEFAULT '{}',
    items_json TEXT NOT NULL DEFAULT '[]',
    counters_json TEXT NOT NULL DEFAULT '{}',
    content_hash TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {{schema_name}}.follow_pages (
    page_key TEXT PRIMARY KEY,
    follow_type TEXT NOT NULL CHECK (follow_type IN ('topic', 'cluster')),
    follow_key TEXT NOT NULL,
    follow_keys_json TEXT NOT NULL DEFAULT '[]',
    page_name TEXT NOT NULL DEFAULT '',
    keywords_text TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_security_master_official_name_norm
    ON {{schema_name}}.security_master(official_name_norm);

CREATE INDEX IF NOT EXISTS idx_security_master_code_market
    ON {{schema_name}}.security_master(code, market);

CREATE INDEX IF NOT EXISTS idx_relations_lookup
    ON {{schema_name}}.relations(relation_type, left_key, relation_label, right_key);

CREATE INDEX IF NOT EXISTS idx_relation_candidates_pending
    ON {{schema_name}}.relation_candidates(status, relation_type, score, updated_at);

CREATE INDEX IF NOT EXISTS idx_relation_candidates_left_key_pending
    ON {{schema_name}}.relation_candidates(left_key, status, score, updated_at);

CREATE INDEX IF NOT EXISTS idx_alias_resolve_tasks_status
    ON {{schema_name}}.alias_resolve_tasks(status, updated_at);

CREATE INDEX IF NOT EXISTS idx_homework_trade_feed_updated
    ON {{schema_name}}.homework_trade_feed(updated_at);

CREATE INDEX IF NOT EXISTS idx_follow_pages_follow_type_key
    ON {{schema_name}}.follow_pages(follow_type, follow_key);
