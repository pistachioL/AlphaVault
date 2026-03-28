-- 手动迁移：给老库补齐 turso_read 需要的列
-- 用法（示例）：
--   1) 连接目标库
--   2) 按顺序执行本文件 SQL，一次即可
--
-- 注意：
-- - 如果某列已经存在，会报 duplicate column name，这是预期情况。
-- - 老库只需要执行一次；新库用最新 CREATE TABLE 即可直接包含这些列。

ALTER TABLE posts ADD COLUMN display_md TEXT;
ALTER TABLE assertions ADD COLUMN author TEXT NOT NULL DEFAULT '';
ALTER TABLE assertions ADD COLUMN created_at TEXT NOT NULL DEFAULT '';
