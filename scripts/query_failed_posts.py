#!/usr/bin/env python3
"""查询 posts 表中失败记录的统计"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.env import load_dotenv_if_present


def main():
    load_dotenv_if_present()

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        print("❌ POSTGRES_DSN not found in .env")
        return

    engine = ensure_postgres_engine(dsn)

    print("\n" + "=" * 70)
    print("Postgres posts 表失败记录统计")
    print("=" * 70)

    with postgres_connect_autocommit(engine) as conn:
        for schema in ["xueqiu", "weibo"]:
            try:
                result = conn.execute(f"""
                    SELECT
                        COUNT(*) as total_count,
                        COUNT(*) FILTER (WHERE final_status = 'failed') as failed_count,
                        COUNT(*) FILTER (WHERE final_status = 'relevant') as relevant_count,
                        COUNT(*) FILTER (WHERE final_status = 'irrelevant') as irrelevant_count,
                        COUNT(*) FILTER (WHERE final_status IS NULL OR final_status = '') as null_count
                    FROM {schema}.posts
                """)
                row = result.fetchone()

                print(f"\n{'=' * 70}")
                print(f"📊 Schema: {schema}")
                print(f"{'=' * 70}")
                print(f"总记录数:           {row[0]:,}")
                print(f"失败 (failed):      {row[1]:,}")
                print(f"相关 (relevant):    {row[2]:,}")
                print(f"无关 (irrelevant):  {row[3]:,}")
                print(f"未处理 (null):      {row[4]:,}")

                if row[1] > 0:
                    print(f"\n⚠️  失败率: {row[1] / row[0] * 100:.2f}%")
                else:
                    print("\n✓ 无失败记录")
            except Exception as e:
                print(f"\n❌ 查询 {schema} 失败: {e}")

    print(f"\n{'=' * 70}\n")


if __name__ == "__main__":
    main()
