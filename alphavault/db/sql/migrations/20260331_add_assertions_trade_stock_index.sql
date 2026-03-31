-- 手动迁移：给老库补齐 trade.* + stock:* 的加速 index
--
-- 用法（示例）：
--   1) 连接目标库
--   2) 执行本文件 SQL，一次即可
--
-- 注意：
-- - 如果 index 已存在，会跳过（IF NOT EXISTS）。

CREATE INDEX IF NOT EXISTS idx_assertions_trade_stock_topic_key
ON assertions(topic_key)
WHERE action LIKE 'trade.%' AND topic_key LIKE 'stock:%';
