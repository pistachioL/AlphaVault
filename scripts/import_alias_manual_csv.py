from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.logging_config import (  # noqa: E402
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from alphavault.research_workbench import (  # noqa: E402
    ALIAS_TASK_STATUS_BLOCKED,
    ALIAS_TASK_STATUS_PENDING,
    ALIAS_TASK_STATUS_RESOLVED,
    get_alias_resolve_tasks_map,
    get_confirmed_stock_alias_targets,
    get_official_names_by_stock_keys,
    get_research_workbench_engine_from_env,
    record_stock_alias_relation,
    set_alias_resolve_task_status,
)
from alphavault.research_workbench.alias_manual_csv import (  # noqa: E402
    ALIAS_MANUAL_CSV_SOURCE,
    AliasManualCsvImportRow,
    load_alias_manual_csv_rows,
    normalize_alias_manual_import_stock_key,
)


DEFAULT_ALIAS_MANUAL_CSV_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "alias_manual_pending.csv"
)
logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导入未确认简称 CSV 回填结果")
    parser.add_argument(
        "--csv-path",
        type=str,
        default=str(DEFAULT_ALIAS_MANUAL_CSV_PATH),
        help="回填 CSV 路径",
    )
    add_log_level_argument(parser)
    return parser.parse_args(argv)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _dedupe_import_rows(
    rows: list[AliasManualCsvImportRow],
) -> tuple[list[AliasManualCsvImportRow], set[str]]:
    ordered_rows: list[AliasManualCsvImportRow] = []
    seen_values_by_alias_key: dict[str, str] = {}
    conflict_alias_keys: set[str] = set()
    for row in rows:
        alias_key = _clean_text(row.get("alias_key"))
        stock_code = _clean_text(row.get("stock_code"))
        previous_stock_code = seen_values_by_alias_key.get(alias_key)
        if previous_stock_code is None:
            seen_values_by_alias_key[alias_key] = stock_code
            ordered_rows.append(row)
            continue
        if previous_stock_code != stock_code:
            conflict_alias_keys.add(alias_key)
    return ordered_rows, conflict_alias_keys


def import_alias_manual_csv(csv_path: str | Path) -> dict[str, int]:
    rows = load_alias_manual_csv_rows(csv_path)
    ordered_rows, duplicate_conflicts = _dedupe_import_rows(rows)
    alias_keys = [_clean_text(row.get("alias_key")) for row in ordered_rows]
    engine = get_research_workbench_engine_from_env()
    tasks_map = get_alias_resolve_tasks_map(engine, alias_keys)
    confirmed_targets = get_confirmed_stock_alias_targets(engine, alias_keys)
    target_keys = [
        normalize_alias_manual_import_stock_key(row.get("stock_code"))
        for row in ordered_rows
        if _clean_text(row.get("stock_code"))
    ]
    official_names_by_stock_key = get_official_names_by_stock_keys(engine, target_keys)

    summary = {
        "total_count": len(rows),
        "confirmed_count": 0,
        "ignored_count": 0,
        "skipped_count": 0,
        "conflict_count": 0,
        "error_count": 0,
    }
    for row in ordered_rows:
        alias_key = _clean_text(row.get("alias_key"))
        raw_stock_code = _clean_text(row.get("stock_code"))
        task_info = tasks_map.get(alias_key)
        current_status = _clean_text(task_info.get("status")) if task_info else ""
        current_target_key = _clean_text(confirmed_targets.get(alias_key))

        if alias_key in duplicate_conflicts:
            summary["conflict_count"] += 1
            logger.warning(
                "alias_manual_csv conflict duplicate alias_key=%s", alias_key
            )
            continue

        if raw_stock_code:
            target_key = normalize_alias_manual_import_stock_key(raw_stock_code)
            if not target_key:
                summary["error_count"] += 1
                logger.error(
                    "alias_manual_csv invalid stock_code alias_key=%s stock_code=%s",
                    alias_key,
                    raw_stock_code,
                )
                continue
            if target_key not in official_names_by_stock_key:
                summary["error_count"] += 1
                logger.error(
                    "alias_manual_csv missing security_master alias_key=%s stock_code=%s",
                    alias_key,
                    raw_stock_code,
                )
                continue
            if current_target_key == target_key:
                summary["skipped_count"] += 1
                logger.info(
                    "alias_manual_csv skip already_confirmed alias_key=%s stock_code=%s",
                    alias_key,
                    raw_stock_code,
                )
                continue
            if current_target_key:
                summary["conflict_count"] += 1
                logger.warning(
                    "alias_manual_csv conflict existing_target alias_key=%s current_target=%s new_stock_code=%s",
                    alias_key,
                    current_target_key,
                    raw_stock_code,
                )
                continue
            if current_status and current_status != ALIAS_TASK_STATUS_PENDING:
                summary["conflict_count"] += 1
                logger.warning(
                    "alias_manual_csv conflict task_status alias_key=%s current_status=%s new_stock_code=%s",
                    alias_key,
                    current_status,
                    raw_stock_code,
                )
                continue
            if not task_info:
                summary["error_count"] += 1
                logger.error(
                    "alias_manual_csv missing alias task alias_key=%s", alias_key
                )
                continue
            record_stock_alias_relation(
                engine,
                stock_key=target_key,
                alias_key=alias_key,
                source=ALIAS_MANUAL_CSV_SOURCE,
            )
            set_alias_resolve_task_status(
                engine,
                alias_key=alias_key,
                status=ALIAS_TASK_STATUS_RESOLVED,
            )
            summary["confirmed_count"] += 1
            logger.info(
                "alias_manual_csv confirmed alias_key=%s stock_code=%s",
                alias_key,
                raw_stock_code,
            )
            continue

        if current_target_key:
            summary["conflict_count"] += 1
            logger.warning(
                "alias_manual_csv conflict ignore_but_confirmed alias_key=%s current_target=%s",
                alias_key,
                current_target_key,
            )
            continue
        if current_status == ALIAS_TASK_STATUS_BLOCKED:
            summary["skipped_count"] += 1
            logger.info("alias_manual_csv skip already_ignored alias_key=%s", alias_key)
            continue
        if current_status and current_status != ALIAS_TASK_STATUS_PENDING:
            summary["conflict_count"] += 1
            logger.warning(
                "alias_manual_csv conflict ignore_task_status alias_key=%s current_status=%s",
                alias_key,
                current_status,
            )
            continue
        if not task_info:
            summary["error_count"] += 1
            logger.error("alias_manual_csv missing alias task alias_key=%s", alias_key)
            continue
        set_alias_resolve_task_status(
            engine,
            alias_key=alias_key,
            status=ALIAS_TASK_STATUS_BLOCKED,
        )
        summary["ignored_count"] += 1
        logger.info("alias_manual_csv ignored alias_key=%s", alias_key)

    return summary


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_present()
    args = parse_args(argv)
    configure_logging(level=getattr(args, "log_level", ""))
    summary = import_alias_manual_csv(args.csv_path)
    logger.info(
        "alias_manual_csv import total=%s confirmed=%s ignored=%s skipped=%s conflicts=%s errors=%s",
        summary["total_count"],
        summary["confirmed_count"],
        summary["ignored_count"],
        summary["skipped_count"],
        summary["conflict_count"],
        summary["error_count"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
