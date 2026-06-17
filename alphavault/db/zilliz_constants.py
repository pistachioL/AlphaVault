"""
环境变量配置说明

Zilliz Cloud 向量数据库配置项
"""

# ============================================
# Zilliz Cloud 向量数据库配置
# ============================================

# Zilliz Cloud 集群 URI
# 获取方式: https://cloud.zilliz.com/ -> 集群详情 -> Cluster URI
ZILLIZ_CLOUD_URI = "ZILLIZ_CLOUD_URI"

# Zilliz Cloud API Token
# 获取方式: https://cloud.zilliz.com/ -> 集群详情 -> Generate API Key
ZILLIZ_CLOUD_TOKEN = "ZILLIZ_CLOUD_TOKEN"

# Zilliz Cloud 迁移模式（可选，默认 "dual_write"）
# - "dual_write": 同时写入 Postgres 和 Zilliz（迁移过渡期，默认）
# - "zilliz_only": 只写入 Zilliz（完全迁移后，可删除 Postgres semantic_docs 表）
# - "postgres_only": 只写入 Postgres（禁用 Zilliz，回滚兼容）
ZILLIZ_MIGRATION_MODE = "ZILLIZ_MIGRATION_MODE"
