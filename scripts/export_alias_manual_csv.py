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
from alphavault.research_workbench.alias_manual_csv import (  # noqa: E402
    build_alias_manual_csv_row,
    write_alias_manual_csv,
)
from alphavault_reflex.services.alias_manual_rows import (  # noqa: E402
    load_alias_manual_pending_rows,
)


DEFAULT_ALIAS_MANUAL_CSV_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "alias_manual_pending.csv"
)
logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导出全部未确认简称到 CSV")
    parser.add_argument(
        "--output-path",
        type=str,
        default=str(DEFAULT_ALIAS_MANUAL_CSV_PATH),
        help="导出 CSV 路径",
    )
    add_log_level_argument(parser)
    return parser.parse_args(argv)


def export_alias_manual_csv(output_path: str | Path) -> tuple[int, Path]:
    pending_rows, load_error = load_alias_manual_pending_rows(limit=None)
    if load_error:
        raise RuntimeError(load_error)
    csv_rows = [build_alias_manual_csv_row(dict(row)) for row in pending_rows]
    path = write_alias_manual_csv(csv_rows, output_path)
    return len(csv_rows), path


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_present()
    args = parse_args(argv)
    configure_logging(level=getattr(args, "log_level", ""))
    row_count, output_path = export_alias_manual_csv(args.output_path)
    logger.info("exported alias_manual rows: %s", row_count)
    logger.info("alias_manual csv path: %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
