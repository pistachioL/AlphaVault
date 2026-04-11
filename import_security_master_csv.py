from __future__ import annotations

import argparse
import csv
from pathlib import Path

from alphavault.research_workbench import (
    get_research_workbench_engine_from_env,
    rebuild_stock_dict_shadow_best_effort,
)
from alphavault.research_workbench.security_master_repo import (
    bulk_upsert_security_master_stocks,
)
from alphavault.research_workbench.schema import use_conn
from alphavault.logging_config import (
    add_log_level_argument,
    configure_logging,
    get_logger,
)


REQUIRED_COLUMNS = ("stock_key", "market", "code", "official_name")
DEFAULT_SECURITY_MASTER_CSV_PATH = (
    Path(__file__).resolve().parent / "data" / "security_master.csv"
)
IMPORT_SECURITY_MASTER_BATCH_SIZE = 100
logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import security_master from CSV")
    parser.add_argument(
        "--csv-path",
        type=str,
        default=str(DEFAULT_SECURITY_MASTER_CSV_PATH),
        help="security_master CSV path",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=IMPORT_SECURITY_MASTER_BATCH_SIZE,
        help="security_master import batch size",
    )
    add_log_level_argument(parser)
    return parser.parse_args()


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def load_security_master_rows(csv_path: str | Path) -> list[dict[str, str]]:
    path = Path(csv_path).expanduser().resolve()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = tuple(reader.fieldnames or ())
        missing_columns = [name for name in REQUIRED_COLUMNS if name not in fieldnames]
        if missing_columns:
            missing_text = ",".join(missing_columns)
            raise RuntimeError(f"missing_csv_columns:{missing_text}")
        rows: list[dict[str, str]] = []
        for row_index, raw_row in enumerate(reader, start=2):
            row = {name: _clean_text(raw_row.get(name)) for name in REQUIRED_COLUMNS}
            if not any(row.values()):
                continue
            for name in REQUIRED_COLUMNS:
                if row[name]:
                    continue
                raise RuntimeError(f"invalid_csv_row:{row_index}:{name}")
            rows.append(row)
    return rows


def _iter_row_batches(
    rows: list[dict[str, str]],
    batch_size: int,
):
    resolved_batch_size = max(1, int(batch_size or 0))
    for start in range(0, len(rows), resolved_batch_size):
        yield rows[start : start + resolved_batch_size]


def import_csv_into_security_master(
    engine_or_conn,
    csv_path: str | Path,
    *,
    batch_size: int | None = None,
) -> int:
    rows = load_security_master_rows(csv_path)
    row_count = len(rows)
    imported = 0
    resolved_batch_size = max(1, int(batch_size or IMPORT_SECURITY_MASTER_BATCH_SIZE))
    with use_conn(engine_or_conn) as conn:
        for batch_rows in _iter_row_batches(rows, resolved_batch_size):
            imported += bulk_upsert_security_master_stocks(conn, batch_rows)
            logger.info("imported security_master rows: %s/%s", imported, row_count)
        logger.info("rebuilding security_master shadow dict...")
        rebuild_stock_dict_shadow_best_effort(conn)
        logger.info("rebuilt security_master shadow dict")
    return row_count


def main() -> int:
    args = parse_args()
    configure_logging(level=getattr(args, "log_level", ""))
    engine = get_research_workbench_engine_from_env()
    row_count = import_csv_into_security_master(
        engine,
        args.csv_path,
        batch_size=args.batch_size,
    )
    logger.info("imported security_master rows: %s", row_count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
