#!/usr/bin/env bash

# Rebuild cloud-ready sqlite files from old posts-only databases.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_SQL="${SCHEMA_SQL:-$ROOT_DIR/alphavault/db/sql/source_schema.sql}"
OLD_WEIBO_FILE="${OLD_WEIBO_FILE:-$ROOT_DIR/old-weibo.db}"
NEW_WEIBO_FILE="${NEW_WEIBO_FILE:-$ROOT_DIR/weibo_rebuild.db}"
OLD_XUEQIU_FILE="${OLD_XUEQIU_FILE:-$ROOT_DIR/old-xueqiu.db}"
NEW_XUEQIU_FILE="${NEW_XUEQIU_FILE:-$ROOT_DIR/xueqiu_rebuild.db}"
WEIBO_TURSO_DB_NAME="${WEIBO_TURSO_DB_NAME:-weibo}"
XUEQIU_TURSO_DB_NAME="${XUEQIU_TURSO_DB_NAME:-xueqiu}"

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "missing_file:$path" >&2
    exit 1
  fi
}

reset_rebuild_db() {
  local db_path="$1"
  rm -f "$db_path" "${db_path}-wal" "${db_path}-shm"
  sqlite3 "$db_path" < "$SCHEMA_SQL"
  sqlite3 "$db_path" 'PRAGMA journal_mode = WAL' >/dev/null
}

sql_escape_single_quotes() {
  printf "%s" "$1" | sed "s/'/''/g"
}

import_posts_only() {
  local old_db="$1"
  local new_db="$2"
  local default_platform="$3"
  local old_db_sql

  old_db_sql="$(sql_escape_single_quotes "$old_db")"

  sqlite3 "$new_db" <<SQL
ATTACH DATABASE '${old_db_sql}' AS old;

INSERT INTO posts (
  post_uid,
  platform,
  platform_post_id,
  author,
  created_at,
  url,
  raw_text,
  final_status,
  invest_score,
  processed_at,
  model,
  prompt_version,
  archived_at,
  ingested_at
)
SELECT
  post_uid,
  COALESCE(NULLIF(platform, ''), '${default_platform}'),
  COALESCE(platform_post_id, ''),
  COALESCE(author, ''),
  COALESCE(created_at, ''),
  COALESCE(url, ''),
  COALESCE(raw_text, ''),
  'irrelevant',
  NULL,
  NULL,
  NULL,
  NULL,
  COALESCE(archived_at, ''),
  CAST(COALESCE(ingested_at, 0) AS INTEGER)
FROM old.posts;

DETACH DATABASE old;
SQL
}

count_rows() {
  local db_path="$1"
  local table_name="$2"
  sqlite3 "$db_path" "SELECT COUNT(*) FROM ${table_name};"
}

count_unprocessed_posts() {
  local db_path="$1"
  sqlite3 "$db_path" \
    "SELECT COUNT(*) FROM posts WHERE processed_at IS NULL OR TRIM(processed_at) = '';"
}

print_rebuild_summary() {
  local label="$1"
  local old_db="$2"
  local new_db="$3"
  local old_posts
  local new_posts
  local new_unprocessed_posts
  local new_assertions
  local new_assertion_mentions
  local new_assertion_entities

  old_posts="$(count_rows "$old_db" "posts")"
  new_posts="$(count_rows "$new_db" "posts")"
  new_unprocessed_posts="$(count_unprocessed_posts "$new_db")"
  new_assertions="$(count_rows "$new_db" "assertions")"
  new_assertion_mentions="$(count_rows "$new_db" "assertion_mentions")"
  new_assertion_entities="$(count_rows "$new_db" "assertion_entities")"

  printf '[verify] %s old_posts=%s new_posts=%s new_unprocessed_posts=%s assertions=%s assertion_mentions=%s assertion_entities=%s\n' \
    "$label" \
    "$old_posts" \
    "$new_posts" \
    "$new_unprocessed_posts" \
    "$new_assertions" \
    "$new_assertion_mentions" \
    "$new_assertion_entities"

  if [[ "$old_posts" != "$new_posts" ]]; then
    echo "posts_count_mismatch:${label}:old=${old_posts}:new=${new_posts}" >&2
    exit 1
  fi
}


require_file "$SCHEMA_SQL"
require_file "$OLD_WEIBO_FILE"
require_file "$OLD_XUEQIU_FILE"

reset_rebuild_db "$NEW_WEIBO_FILE"
reset_rebuild_db "$NEW_XUEQIU_FILE"

import_posts_only "$OLD_WEIBO_FILE" "$NEW_WEIBO_FILE" "weibo"
import_posts_only "$OLD_XUEQIU_FILE" "$NEW_XUEQIU_FILE" "xueqiu"

print_rebuild_summary "weibo" "$OLD_WEIBO_FILE" "$NEW_WEIBO_FILE"
print_rebuild_summary "xueqiu" "$OLD_XUEQIU_FILE" "$NEW_XUEQIU_FILE"

uv run python "$ROOT_DIR/migrate_weibo_raw_text.py" --db-path "$NEW_WEIBO_FILE" --batch-size 200 --verbose

turso db create "$WEIBO_TURSO_DB_NAME" --from-file "$NEW_WEIBO_FILE"
turso db create "$XUEQIU_TURSO_DB_NAME" --from-file "$NEW_XUEQIU_FILE"
