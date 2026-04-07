from __future__ import annotations

import argparse
import csv
from pathlib import Path

from alphavault.research_workbench import (
    get_research_workbench_engine_from_env,
    rebuild_stock_dict_shadow_best_effort,
    upsert_security_master_stock,
)


REQUIRED_COLUMNS = ("stock_key", "market", "code", "official_name")
DEFAULT_SECURITY_MASTER_CSV_PATH = (
    Path(__file__).resolve().parent / "data" / "security_master.csv"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import security_master from CSV")
    parser.add_argument(
        "--csv-path",
        type=str,
        default=str(DEFAULT_SECURITY_MASTER_CSV_PATH),
        help="security_master CSV path",
    )
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


def import_csv_into_security_master(engine_or_conn, csv_path: str | Path) -> int:
    rows = load_security_master_rows(csv_path)
    for row in rows:
        upsert_security_master_stock(
            engine_or_conn,
            stock_key=row["stock_key"],
            market=row["market"],
            code=row["code"],
            official_name=row["official_name"],
        )
    rebuild_stock_dict_shadow_best_effort(engine_or_conn)
    return len(rows)


def main() -> int:
    args = parse_args()
    engine = get_research_workbench_engine_from_env()
    row_count = import_csv_into_security_master(engine, args.csv_path)
    print(f"imported security_master rows: {row_count}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
