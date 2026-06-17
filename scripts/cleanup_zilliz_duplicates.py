#!/usr/bin/env python3
"""清理 Zilliz 中的重复数据，重建集合"""

import os
import sys
from pymilvus import MilvusClient


def main() -> None:
    # 读取环境变量（支持两种命名）
    zilliz_uri = os.getenv("ZILLIZ_URI") or os.getenv("ZILLIZ_CLOUD_URI")
    zilliz_token = os.getenv("ZILLIZ_TOKEN") or os.getenv("ZILLIZ_CLOUD_TOKEN")

    if not zilliz_uri or not zilliz_token:
        print(
            "❌ 请设置环境变量: ZILLIZ_URI/ZILLIZ_CLOUD_URI, ZILLIZ_TOKEN/ZILLIZ_CLOUD_TOKEN"
        )
        sys.exit(1)

    print(f"连接 Zilliz Cloud: {zilliz_uri}")
    client = MilvusClient(uri=zilliz_uri, token=zilliz_token, timeout=10)

    collections = ["alphavault_xueqiu_semantic", "alphavault_weibo_semantic"]

    for collection_name in collections:
        if client.has_collection(collection_name):
            print(f"\n删除集合: {collection_name}")
            client.drop_collection(collection_name)
            print(f"✓ 已删除 {collection_name}")
        else:
            print(f"\n集合 {collection_name} 不存在，跳过")

    print("\n✓ 清理完成！")
    print("\n下一步:")
    print("  1. 删除状态文件: rm .zilliz_migration_state.json")
    print("  2. 重新运行迁移: uv run python scripts/migrate_to_zilliz.py")


if __name__ == "__main__":
    main()
