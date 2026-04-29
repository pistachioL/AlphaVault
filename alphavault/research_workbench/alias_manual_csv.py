from __future__ import annotations

import csv
from pathlib import Path
from typing import TypedDict

from alphavault.domains.stock.key_match import is_stock_code_value, normalize_stock_code


ALIAS_MANUAL_CSV_ENCODING = "utf-8-sig"
ALIAS_MANUAL_CSV_SOURCE = "manual_csv"
ALIAS_MANUAL_CSV_COLUMNS = (
    "alias_key",
    "sample_post_uid",
    "sample_post_url",
    "sample_evidence",
    "sample_raw_text_excerpt",
    "history_hits_count",
    "history_hits_text",
    "attempt_count",
    "updated_at",
    "ai_status",
    "ai_stock_code",
    "ai_official_name",
    "ai_confidence",
    "ai_reason",
    "ai_uncertain",
    "ai_validation_status",
    "stock_code",
)
ALIAS_MANUAL_CSV_REQUIRED_COLUMNS = ("alias_key", "stock_code")


class AliasManualCsvImportRow(TypedDict):
    alias_key: str
    stock_code: str


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def build_alias_manual_csv_row(row: dict[str, object]) -> dict[str, str]:
    out: dict[str, str] = {}
    for column in ALIAS_MANUAL_CSV_COLUMNS:
        out[column] = "" if column == "stock_code" else _clean_text(row.get(column))
    return out


def write_alias_manual_csv(
    rows: list[dict[str, str]],
    output_path: str | Path,
) -> Path:
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=ALIAS_MANUAL_CSV_ENCODING, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ALIAS_MANUAL_CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def load_alias_manual_csv_rows(
    csv_path: str | Path,
) -> list[AliasManualCsvImportRow]:
    path = Path(csv_path).expanduser().resolve()
    with path.open("r", encoding=ALIAS_MANUAL_CSV_ENCODING, newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = tuple(reader.fieldnames or ())
        missing_columns = [
            name for name in ALIAS_MANUAL_CSV_REQUIRED_COLUMNS if name not in fieldnames
        ]
        if missing_columns:
            missing_text = ",".join(missing_columns)
            raise RuntimeError(f"missing_csv_columns:{missing_text}")
        rows: list[AliasManualCsvImportRow] = []
        for row_index, raw_row in enumerate(reader, start=2):
            values = [_clean_text(value) for value in (raw_row or {}).values()]
            if not any(values):
                continue
            alias_key = _clean_text((raw_row or {}).get("alias_key"))
            stock_code = _clean_text((raw_row or {}).get("stock_code"))
            if not alias_key:
                raise RuntimeError(f"invalid_csv_row:{row_index}:alias_key")
            rows.append(
                AliasManualCsvImportRow(
                    alias_key=alias_key,
                    stock_code=stock_code,
                )
            )
    return rows


def normalize_alias_manual_import_stock_key(value: object) -> str:
    code = normalize_stock_code(_clean_text(value))
    if not is_stock_code_value(code):
        return ""
    return f"stock:{code}"


__all__ = [
    "ALIAS_MANUAL_CSV_COLUMNS",
    "ALIAS_MANUAL_CSV_ENCODING",
    "ALIAS_MANUAL_CSV_REQUIRED_COLUMNS",
    "ALIAS_MANUAL_CSV_SOURCE",
    "AliasManualCsvImportRow",
    "build_alias_manual_csv_row",
    "load_alias_manual_csv_rows",
    "normalize_alias_manual_import_stock_key",
    "write_alias_manual_csv",
]
