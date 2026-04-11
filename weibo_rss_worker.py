"""
Weibo RSS -> Redis -> AI -> Postgres (single instance).

Design goals:
- No local sqlite queue required (no paid docker volume needed).
- RSS items are pushed to Redis first, then AI runs and writes final rows.
- If Redis is temporarily down, items are spooled to local files for retry.
"""

from __future__ import annotations

from alphavault.env import load_dotenv_if_present
from alphavault.worker.worker import main


def _main() -> None:
    load_dotenv_if_present()
    main()


if __name__ == "__main__":
    _main()
