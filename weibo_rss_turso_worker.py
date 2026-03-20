"""
Weibo RSS -> Turso -> AI -> Turso (single instance).

Design goals:
- No local sqlite queue required (no paid docker volume needed).
- RSS items are inserted to Turso first (as pending), then AI runs and updates rows.
- If Turso is temporarily down, items are spooled to local files and (optionally) Redis.
"""

from __future__ import annotations

from alphavault.worker.worker import main


if __name__ == "__main__":
    main()

