from __future__ import annotations

import argparse
import re
import time
from datetime import datetime

from alphavault.constants import DATETIME_FMT, PLATFORM_WEIBO
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql.scripts import (
    CREATE_WEIBO_RAW_TEXT_MIGRATION_STATUS_INDEX,
    CREATE_WEIBO_RAW_TEXT_MIGRATION_TABLE,
    SELECT_PENDING_WEIBO_RAW_TEXT_MIGRATION_BATCH,
    SELECT_TOTAL_POSTS,
    UPDATE_POST_RAW_TEXT_IF_UNCHANGED,
    UPDATE_WEIBO_RAW_TEXT_MIGRATION_STATUS,
    UPSERT_WEIBO_RAW_TEXT_MIGRATION_ROW,
    WEIBO_RAW_TEXT_MIGRATION_TABLE,
    max_legacy_weibo_raw_text_post_uid,
    select_legacy_weibo_raw_text_batch,
    select_legacy_weibo_raw_text_rows_by_post_uids,
    select_pending_weibo_raw_text_migration_rows_by_post_uids,
)
from alphavault.db.turso_db import (
    TursoConnection,
    get_turso_engine_from_env,
    turso_connect_autocommit,
    turso_savepoint,
)
from alphavault.env import load_dotenv_if_present
from alphavault.weibo.display import (
    IMAGE_LABEL_PREFIX,
    SEGMENT_SEPARATOR,
    format_weibo_thread_text,
)


DEFAULT_BATCH_SIZE = 200
STATUS_PENDING = "pending"
STATUS_APPLIED = "applied"
STATUS_CONFLICT = "conflict"
STATUS_SKIPPED = "skipped"
REASON_ALREADY_MIGRATED = "already_migrated"
REASON_EMPTY_MIGRATED_TEXT = "empty_migrated_text"
REASON_RAW_TEXT_CHANGED = "raw_text_changed"
LEGACY_MARKERS = ("//@", "回复@", "[微博元信息]", "[转发原文]", "[CSV原始字段]")
SINGLE_SEGMENT_MIGRATED_RE = re.compile(r"^[^：\s\n][^：\n]{0,40}：\S")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare or apply Weibo raw_text migration"
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少条（0=不限）")
    parser.add_argument(
        "--sleep-sec", type=float, default=0.0, help="每批之间 sleep 秒数（默认 0）"
    )
    parser.add_argument(
        "--post-uids",
        type=str,
        default="",
        help="只处理指定 post_uid（逗号/换行分隔）",
    )
    parser.add_argument(
        "--apply-only",
        action="store_true",
        help="只把临时迁移表里 pending 行安全写回 posts.raw_text",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印进度，不写 DB")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _now_text() -> str:
    return datetime.now().strftime(DATETIME_FMT)


def _select_batch(
    conn: TursoConnection,
    *,
    batch_size: int,
    last_post_uid: str,
    stop_post_uid: str,
) -> list[dict]:
    limit = max(1, int(batch_size))
    query = select_legacy_weibo_raw_text_batch()
    params = {
        "limit": limit,
        "last_post_uid": str(last_post_uid or ""),
        "stop_post_uid": str(stop_post_uid or ""),
    }
    rows = conn.execute(query, params).mappings().fetchall()
    return [dict(row) for row in rows]


def _max_target_post_uid(conn: TursoConnection) -> str:
    query = max_legacy_weibo_raw_text_post_uid()
    return str(conn.execute(query).scalar() or "").strip()


def _parse_post_uids(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    items: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,\n]+", raw):
        uid = str(part or "").strip()
        if not uid:
            continue
        if uid.isdigit():
            uid = f"weibo:{uid}"
        if uid in seen:
            continue
        seen.add(uid)
        items.append(uid)
    return items


def _select_rows_by_post_uids(
    conn: TursoConnection, post_uids: list[str]
) -> list[dict]:
    if not post_uids:
        return []
    placeholders = make_in_placeholders(prefix="uid", count=len(post_uids))
    query = select_legacy_weibo_raw_text_rows_by_post_uids(placeholders)
    params = make_in_params(prefix="uid", values=post_uids)
    rows = conn.execute(query, params).mappings().fetchall()
    return [dict(row) for row in rows]


def _chunks(items: list[str], size: int) -> list[list[str]]:
    n = max(1, int(size))
    return [items[i : i + n] for i in range(0, len(items), n)]


def _resolve_platform(row: dict) -> str:
    platform = str(row.get("platform") or "").strip().lower()
    if platform:
        return platform
    post_uid = str(row.get("post_uid") or "").strip().lower()
    if ":" not in post_uid:
        return ""
    return post_uid.split(":", 1)[0]


def _extract_image_urls_from_raw_text(raw_text: str) -> list[str]:
    urls: list[str] = []
    for raw_line in str(raw_text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line.startswith(IMAGE_LABEL_PREFIX):
            continue
        url = line[len(IMAGE_LABEL_PREFIX) :].strip()
        if url:
            urls.append(url)
    return urls


def _all_non_empty_lines_are_image_labels(raw_text: str) -> bool:
    lines = [str(line or "").strip() for line in str(raw_text or "").splitlines()]
    lines = [line for line in lines if line]
    return bool(lines) and all(line.startswith(IMAGE_LABEL_PREFIX) for line in lines)


def _looks_like_migrated_raw_text(raw_text: str) -> bool:
    text = str(raw_text or "").strip()
    if not text:
        return False
    if _all_non_empty_lines_are_image_labels(text):
        return True
    if SEGMENT_SEPARATOR in text:
        return True
    if any(marker in text for marker in LEGACY_MARKERS):
        return False
    first_line = str(text.splitlines()[0] or "").strip()
    return bool(SINGLE_SEGMENT_MIGRATED_RE.match(first_line))


def _build_migration_rows(rows: list[dict]) -> tuple[list[dict], set[str]]:
    updates: list[dict] = []
    found_uids: set[str] = set()
    for row in rows:
        post_uid = str(row.get("post_uid") or "").strip()
        platform = _resolve_platform(row)
        author = str(row.get("author") or "").strip()
        raw_text = str(row.get("raw_text") or "")
        if not post_uid:
            continue
        if platform != PLATFORM_WEIBO:
            continue
        found_uids.add(post_uid)
        if _looks_like_migrated_raw_text(raw_text):
            updates.append(
                {
                    "post_uid": post_uid,
                    "old_raw_text": raw_text,
                    "new_raw_text": raw_text,
                    "status": STATUS_SKIPPED,
                    "reason": REASON_ALREADY_MIGRATED,
                }
            )
            continue
        new_raw_text = format_weibo_thread_text(
            raw_text,
            author=author,
            image_urls=_extract_image_urls_from_raw_text(raw_text),
        )
        if not new_raw_text:
            updates.append(
                {
                    "post_uid": post_uid,
                    "old_raw_text": raw_text,
                    "new_raw_text": raw_text,
                    "status": STATUS_SKIPPED,
                    "reason": REASON_EMPTY_MIGRATED_TEXT,
                }
            )
            continue
        updates.append(
            {
                "post_uid": post_uid,
                "old_raw_text": raw_text,
                "new_raw_text": new_raw_text,
                "status": STATUS_PENDING,
                "reason": "",
            }
        )
    return updates, found_uids


def _is_legacy_weibo_scan_target(row: dict) -> bool:
    post_uid = str(row.get("post_uid") or "").strip()
    if not post_uid:
        return False
    if _resolve_platform(row) != PLATFORM_WEIBO:
        return False
    raw_text = str(row.get("raw_text") or "")
    if not raw_text.strip():
        return False
    return not _looks_like_migrated_raw_text(raw_text)


def _ensure_migration_table(conn: TursoConnection) -> None:
    conn.execute(CREATE_WEIBO_RAW_TEXT_MIGRATION_TABLE)
    conn.execute(CREATE_WEIBO_RAW_TEXT_MIGRATION_STATUS_INDEX)


def _migration_table_exists(conn: TursoConnection) -> bool:
    row = (
        conn.execute(
            """
SELECT 1
FROM sqlite_schema
WHERE type = 'table' AND name = :table_name
LIMIT 1
""",
            {"table_name": WEIBO_RAW_TEXT_MIGRATION_TABLE},
        )
        .mappings()
        .fetchone()
    )
    return bool(row)


def _upsert_migration_rows(
    conn: TursoConnection, *, rows: list[dict], updated_at: str
) -> int:
    saved = 0
    for item in rows:
        payload = dict(item)
        payload["updated_at"] = updated_at
        conn.execute(UPSERT_WEIBO_RAW_TEXT_MIGRATION_ROW, payload)
        saved += 1
    return saved


def _select_pending_migration_rows(
    conn: TursoConnection, batch_size: int
) -> list[dict]:
    rows = conn.execute(
        SELECT_PENDING_WEIBO_RAW_TEXT_MIGRATION_BATCH,
        {"limit": max(1, int(batch_size))},
    ).mappings()
    return [dict(row) for row in rows.fetchall()]


def _select_pending_migration_rows_by_post_uids(
    conn: TursoConnection, post_uids: list[str]
) -> list[dict]:
    if not post_uids:
        return []
    placeholders = make_in_placeholders(prefix="uid", count=len(post_uids))
    query = select_pending_weibo_raw_text_migration_rows_by_post_uids(placeholders)
    params = make_in_params(prefix="uid", values=post_uids)
    rows = conn.execute(query, params).mappings().fetchall()
    return [dict(row) for row in rows]


def _apply_pending_migration_rows(
    conn: TursoConnection, *, rows: list[dict], updated_at: str
) -> dict[str, int]:
    stats = {"applied": 0, "conflict": 0}
    for item in rows:
        res = conn.execute(UPDATE_POST_RAW_TEXT_IF_UNCHANGED, item)
        changed = int(res.rowcount or 0) > 0
        status = STATUS_APPLIED if changed else STATUS_CONFLICT
        reason = "" if changed else REASON_RAW_TEXT_CHANGED
        conn.execute(
            UPDATE_WEIBO_RAW_TEXT_MIGRATION_STATUS,
            {
                "post_uid": str(item.get("post_uid") or "").strip(),
                "status": status,
                "reason": reason,
                "updated_at": updated_at,
            },
        )
        if changed:
            stats["applied"] += 1
        else:
            stats["conflict"] += 1
    return stats


def _count_status(rows: list[dict], *, status: str) -> int:
    return sum(1 for item in rows if str(item.get("status") or "") == status)


def _count_targets(
    conn: TursoConnection, *, batch_size: int, stop_post_uid: str
) -> int:
    if not stop_post_uid:
        return 0

    total = 0
    last_post_uid = ""
    while True:
        rows = _select_batch(
            conn,
            batch_size=batch_size,
            last_post_uid=last_post_uid,
            stop_post_uid=stop_post_uid,
        )
        if not rows:
            return total
        total += sum(1 for row in rows if _is_legacy_weibo_scan_target(row))
        last_post_uid = str(rows[-1].get("post_uid") or last_post_uid)


def _collect_scan_updates(
    rows: list[dict], *, remaining: int | None
) -> tuple[list[dict], str]:
    updates: list[dict] = []
    last_post_uid = ""

    for row in rows:
        last_post_uid = str(row.get("post_uid") or last_post_uid)
        if not _is_legacy_weibo_scan_target(row):
            continue
        row_updates, _found = _build_migration_rows([row])
        if not row_updates:
            continue
        updates.extend(row_updates)
        if remaining is not None and len(updates) >= remaining:
            break

    return updates, last_post_uid


def _prepare_by_post_uids(
    engine,
    *,
    post_uids: list[str],
    batch_size: int,
    dry_run: bool,
    sleep_sec: float,
    verbose: bool,
) -> None:
    processed = 0
    prepared = 0
    skipped = 0
    found_uids: set[str] = set()

    with turso_connect_autocommit(engine) as conn:
        total_posts = int(conn.execute(SELECT_TOTAL_POSTS).scalar() or 0)

    print(
        f"[migrate] prepare start total_posts={total_posts} target={len(post_uids)} by_post_uids=True"
    )

    if not dry_run:
        with turso_connect_autocommit(engine) as conn:
            with turso_savepoint(conn):
                _ensure_migration_table(conn)

    for chunk in _chunks(post_uids, batch_size):
        with turso_connect_autocommit(engine) as conn:
            rows = _select_rows_by_post_uids(conn, chunk)

        updates, found_this_batch = _build_migration_rows(rows)
        found_uids.update(found_this_batch)
        processed += len(rows)
        prepared_this_batch = _count_status(updates, status=STATUS_PENDING)
        skipped_this_batch = _count_status(updates, status=STATUS_SKIPPED)
        prepared += prepared_this_batch
        skipped += skipped_this_batch

        if dry_run:
            print(
                "[dry-run] prepare batch "
                f"rows={len(rows)} pending={prepared_this_batch} skipped={skipped_this_batch} "
                f"total_processed={processed}"
            )
            continue

        with turso_connect_autocommit(engine) as conn:
            with turso_savepoint(conn):
                saved_this_batch = _upsert_migration_rows(
                    conn, rows=updates, updated_at=_now_text()
                )
        print(
            "[migrate] prepare batch "
            f"saved={saved_this_batch} pending={prepared_this_batch} skipped={skipped_this_batch} "
            f"total_processed={processed}"
        )

        if sleep_sec > 0:
            time.sleep(float(sleep_sec))

    missing = [uid for uid in post_uids if uid not in found_uids]
    if missing and verbose:
        head = ", ".join(missing[:10])
        tail = "" if len(missing) <= 10 else f" ... (+{len(missing) - 10})"
        print(f"[migrate] missing_post_uids={len(missing)} {head}{tail}")

    print(
        f"[migrate] prepare done pending={prepared} skipped={skipped} processed={processed} target={len(post_uids)}"
    )


def _prepare_scan(
    engine,
    *,
    batch_size: int,
    limit: int,
    sleep_sec: float,
    dry_run: bool,
    verbose: bool,
) -> None:
    processed = 0
    prepared = 0
    skipped = 0
    last_post_uid = ""

    with turso_connect_autocommit(engine) as conn:
        total_posts = int(conn.execute(SELECT_TOTAL_POSTS).scalar() or 0)
        stop_post_uid = _max_target_post_uid(conn)
        target_total = _count_targets(
            conn,
            batch_size=max(1, int(batch_size)),
            stop_post_uid=stop_post_uid,
        )

    if not stop_post_uid or target_total <= 0:
        print(
            f"[migrate] nothing_to_prepare total_posts={total_posts} target={target_total}"
        )
        return

    print(
        f"[migrate] prepare start total_posts={total_posts} source_target={target_total}"
    )

    if not dry_run:
        with turso_connect_autocommit(engine) as conn:
            with turso_savepoint(conn):
                _ensure_migration_table(conn)

    while True:
        remaining = None
        if limit > 0:
            remaining = max(0, int(limit) - processed)
            if remaining <= 0:
                break

        with turso_connect_autocommit(engine) as conn:
            rows = _select_batch(
                conn,
                batch_size=max(1, int(batch_size)),
                last_post_uid=last_post_uid,
                stop_post_uid=stop_post_uid,
            )

        if not rows:
            break

        updates, last_post_uid = _collect_scan_updates(rows, remaining=remaining)
        processed += len(updates)
        prepared_this_batch = _count_status(updates, status=STATUS_PENDING)
        skipped_this_batch = _count_status(updates, status=STATUS_SKIPPED)
        prepared += prepared_this_batch
        skipped += skipped_this_batch

        if dry_run:
            print(
                "[dry-run] prepare batch "
                f"rows={len(rows)} pending={prepared_this_batch} skipped={skipped_this_batch} "
                f"total_processed={processed}"
            )
        else:
            with turso_connect_autocommit(engine) as conn:
                with turso_savepoint(conn):
                    saved_this_batch = _upsert_migration_rows(
                        conn, rows=updates, updated_at=_now_text()
                    )
            print(
                "[migrate] prepare batch "
                f"saved={saved_this_batch} pending={prepared_this_batch} skipped={skipped_this_batch} "
                f"total_processed={processed}"
            )

        if sleep_sec > 0:
            time.sleep(float(sleep_sec))

    if verbose and dry_run:
        print(
            f"[migrate] prepare summary pending={prepared} skipped={skipped} processed={processed}"
        )
    print(
        f"[migrate] prepare done pending={prepared} skipped={skipped} processed={processed}"
    )


def _apply_by_post_uids(
    engine,
    *,
    post_uids: list[str],
    batch_size: int,
    dry_run: bool,
    sleep_sec: float,
    verbose: bool,
) -> None:
    processed = 0
    applied = 0
    conflict = 0
    found_uids: set[str] = set()

    print(f"[migrate] apply start target={len(post_uids)} by_post_uids=True")

    with turso_connect_autocommit(engine) as conn:
        if dry_run:
            if not _migration_table_exists(conn):
                print("[migrate] apply dry-run done pending_rows=0")
                return
        else:
            with turso_savepoint(conn):
                _ensure_migration_table(conn)

    for chunk in _chunks(post_uids, batch_size):
        with turso_connect_autocommit(engine) as conn:
            rows = _select_pending_migration_rows_by_post_uids(conn, chunk)

        found_uids.update(
            str(item.get("post_uid") or "").strip()
            for item in rows
            if item.get("post_uid")
        )
        processed += len(rows)

        if dry_run:
            print(f"[dry-run] apply batch rows={len(rows)} total_processed={processed}")
            continue

        with turso_connect_autocommit(engine) as conn:
            with turso_savepoint(conn):
                stats = _apply_pending_migration_rows(
                    conn,
                    rows=rows,
                    updated_at=_now_text(),
                )
        applied += stats["applied"]
        conflict += stats["conflict"]
        print(
            "[migrate] apply batch "
            f"applied={stats['applied']} conflict={stats['conflict']} total_processed={processed}"
        )

        if sleep_sec > 0:
            time.sleep(float(sleep_sec))

    missing = [uid for uid in post_uids if uid not in found_uids]
    if missing and verbose:
        head = ", ".join(missing[:10])
        tail = "" if len(missing) <= 10 else f" ... (+{len(missing) - 10})"
        print(f"[migrate] missing_pending_post_uids={len(missing)} {head}{tail}")

    if dry_run:
        print(f"[migrate] apply dry-run done pending_rows={processed}")
        return

    print(
        f"[migrate] apply done applied={applied} conflict={conflict} processed={processed}"
    )


def _apply_scan(
    engine,
    *,
    batch_size: int,
    limit: int,
    dry_run: bool,
    sleep_sec: float,
) -> None:
    processed = 0
    applied = 0
    conflict = 0

    print("[migrate] apply start")

    with turso_connect_autocommit(engine) as conn:
        if dry_run:
            if not _migration_table_exists(conn):
                print("[migrate] apply dry-run done pending_rows=0")
                return
        else:
            with turso_savepoint(conn):
                _ensure_migration_table(conn)

    while True:
        remaining = None
        if limit > 0:
            remaining = max(0, int(limit) - processed)
            if remaining <= 0:
                break

        with turso_connect_autocommit(engine) as conn:
            effective_batch_size = int(batch_size)
            if remaining is not None:
                effective_batch_size = min(effective_batch_size, remaining)
            rows = _select_pending_migration_rows(conn, batch_size=effective_batch_size)

        if not rows:
            break

        processed += len(rows)

        if dry_run:
            print(f"[dry-run] apply batch rows={len(rows)} total_processed={processed}")
        else:
            with turso_connect_autocommit(engine) as conn:
                with turso_savepoint(conn):
                    stats = _apply_pending_migration_rows(
                        conn,
                        rows=rows,
                        updated_at=_now_text(),
                    )
            applied += stats["applied"]
            conflict += stats["conflict"]
            print(
                "[migrate] apply batch "
                f"applied={stats['applied']} conflict={stats['conflict']} total_processed={processed}"
            )

        if sleep_sec > 0:
            time.sleep(float(sleep_sec))

    if dry_run:
        print(f"[migrate] apply dry-run done pending_rows={processed}")
        return

    print(
        f"[migrate] apply done applied={applied} conflict={conflict} processed={processed}"
    )


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    engine = get_turso_engine_from_env()

    post_uids = _parse_post_uids(getattr(args, "post_uids", ""))
    if bool(args.apply_only):
        if post_uids:
            _apply_by_post_uids(
                engine,
                post_uids=post_uids,
                batch_size=max(1, int(args.batch_size)),
                dry_run=bool(args.dry_run),
                sleep_sec=float(args.sleep_sec or 0.0),
                verbose=bool(args.verbose),
            )
            return

        _apply_scan(
            engine,
            batch_size=max(1, int(args.batch_size)),
            limit=int(args.limit or 0),
            dry_run=bool(args.dry_run),
            sleep_sec=float(args.sleep_sec or 0.0),
        )
        return

    if post_uids:
        _prepare_by_post_uids(
            engine,
            post_uids=post_uids,
            batch_size=max(1, int(args.batch_size)),
            dry_run=bool(args.dry_run),
            sleep_sec=float(args.sleep_sec or 0.0),
            verbose=bool(args.verbose),
        )
        return

    _prepare_scan(
        engine,
        batch_size=max(1, int(args.batch_size)),
        limit=int(args.limit or 0),
        sleep_sec=float(args.sleep_sec or 0.0),
        dry_run=bool(args.dry_run),
        verbose=bool(args.verbose),
    )


if __name__ == "__main__":
    main()
