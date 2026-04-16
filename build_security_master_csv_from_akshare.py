from __future__ import annotations

import argparse
import csv
import importlib
from io import BytesIO
from pathlib import Path
from typing import Protocol

import requests

from alphavault.domains.stock.key_match import normalize_stock_code
from alphavault.domains.stock.keys import normalize_stock_key
from alphavault.domains.stock.names import normalize_stock_official_name
from alphavault.logging_config import (
    add_log_level_argument,
    configure_logging,
    get_logger,
)

REQUIRED_COLUMNS = ("stock_key", "market", "code", "official_name")
DEFAULT_SECURITY_MASTER_CSV_PATH = (
    Path(__file__).resolve().parent / "data" / "security_master.csv"
)
SH_MAIN_INDICATOR = "主板A股"
SH_STAR_INDICATOR = "科创板"
SZ_A_INDICATOR = "A股列表"
HKEX_SECURITIES_URL = (
    "https://www.hkex.com.hk/chi/services/trading/securities/securitieslists/"
    "ListOfSecurities_c.xlsx"
)
HKEX_SOURCE_NAME = "hk_main"
HKEX_SECURITIES_HEADER_ROW = 2
HKEX_CODE_COLUMN = "股份代號"
HKEX_NAME_COLUMN = "股份名稱"
HKEX_CATEGORY_COLUMN = "分類"
HKEX_SUBCATEGORY_COLUMN = "次分類"
HKEX_EQUITY_CATEGORY = "股本"
HKEX_EQUITY_SUBCATEGORY_KEYWORD = "股本證券"
HK_OPENCC_CONFIG = "hk2s.json"
DEFAULT_MARKETS = ("sh", "sz", "hk")
SUPPORTED_MARKETS = set(DEFAULT_MARKETS)
logger = get_logger(__name__)


class TextConverter(Protocol):
    def convert(self, text: str) -> str: ...


class TabularRecords(Protocol):
    columns: object

    def to_dict(self, orient: str) -> list[dict[str, object]]: ...


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build security_master CSV from AKShare and HKEX"
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=str(DEFAULT_SECURITY_MASTER_CSV_PATH),
        help="security_master CSV output path",
    )
    parser.add_argument(
        "--markets",
        type=str,
        default="sh,sz,hk",
        help="comma-separated markets: sh,sz,hk",
    )
    add_log_level_argument(parser)
    return parser.parse_args()


def _load_akshare_module():
    try:
        return importlib.import_module("akshare")
    except ModuleNotFoundError as err:
        raise RuntimeError("missing_dependency:akshare") from err


def build_hk_opencc_converter() -> TextConverter:
    try:
        opencc_module = importlib.import_module("opencc")
    except ModuleNotFoundError as err:
        raise RuntimeError("missing_dependency:opencc") from err
    return opencc_module.OpenCC(HK_OPENCC_CONFIG)


def _load_openpyxl_module():
    try:
        return importlib.import_module("openpyxl")
    except ModuleNotFoundError as err:
        raise RuntimeError("missing_dependency:openpyxl") from err


def parse_markets(value: str) -> tuple[str, ...]:
    items = [str(item or "").strip().lower() for item in str(value or "").split(",")]
    markets = tuple(item for item in items if item)
    if not markets:
        raise RuntimeError("missing_markets")
    invalid_markets = [item for item in markets if item not in SUPPORTED_MARKETS]
    if invalid_markets:
        raise RuntimeError(f"invalid_markets:{','.join(invalid_markets)}")
    unique_markets: list[str] = []
    for item in markets:
        if item in unique_markets:
            continue
        unique_markets.append(item)
    return tuple(unique_markets)


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return False


def _clean_text(value: object) -> str:
    if _is_missing(value):
        return ""
    return str(value or "").strip()


def _clean_code(value: object) -> str:
    text = _clean_text(value)
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return normalize_stock_code(text)


def _clean_hkex_code(value: object) -> str:
    normalized_code = _clean_code(value)
    if normalized_code.isdigit():
        return normalized_code.zfill(5)
    return normalized_code


def _frame_columns(
    frame: TabularRecords | list[dict[str, object]],
) -> tuple[str, ...]:
    raw_columns = getattr(frame, "columns", None)
    if raw_columns is not None:
        return tuple(
            str(column or "").strip()
            for column in raw_columns
            if str(column or "").strip()
        )
    records = _frame_records(frame)
    if not records:
        return ()
    return tuple(str(key or "").strip() for key in records[0] if str(key or "").strip())


def _frame_records(
    frame: TabularRecords | list[dict[str, object]],
    *,
    wanted_columns: tuple[str, ...] | None = None,
) -> list[dict[str, object]]:
    if isinstance(frame, list):
        raw_records = [dict(row) for row in frame if isinstance(row, dict)]
    else:
        raw_records = frame.to_dict("records")
    if not wanted_columns:
        return raw_records
    return [
        {column: row.get(column) for column in wanted_columns} for row in raw_records
    ]


def _require_frame_columns(
    frame: TabularRecords | list[dict[str, object]],
    *,
    source_name: str,
    required_columns: tuple[str, ...],
    error_prefix: str = "unexpected_akshare_columns",
) -> None:
    frame_columns = set(_frame_columns(frame))
    for name in required_columns:
        if name in frame_columns:
            continue
        raise RuntimeError(f"{error_prefix}:{source_name}:{name}")


def _fetch_frame(
    akshare_module, *, fetch_name: str, source_name: str, **kwargs
) -> TabularRecords | list[dict[str, object]]:
    fetch_fn = getattr(akshare_module, fetch_name)
    try:
        return fetch_fn(**kwargs)
    except Exception as err:
        raise RuntimeError(
            f"akshare_fetch_failed:{source_name}:{type(err).__name__}"
        ) from err


def _load_hkex_records_from_sheet(
    sheet, *, header_row_index: int
) -> list[dict[str, object]]:
    header: list[str] = []
    records: list[dict[str, object]] = []
    for row_index, values in enumerate(sheet.iter_rows(values_only=True)):
        cells = list(values)
        if row_index < header_row_index:
            continue
        if row_index == header_row_index:
            header = [str(cell or "").strip() for cell in cells]
            continue
        if not header:
            continue
        record: dict[str, object] = {}
        for index, name in enumerate(header):
            if not name:
                continue
            record[name] = cells[index] if index < len(cells) else None
        if record:
            records.append(record)
    return records


def load_hkex_securities_frame() -> list[dict[str, object]]:
    openpyxl_module = _load_openpyxl_module()
    try:
        response = requests.get(HKEX_SECURITIES_URL, timeout=30)
        response.raise_for_status()
        workbook = openpyxl_module.load_workbook(
            BytesIO(response.content),
            read_only=True,
            data_only=True,
        )
        sheet = workbook.active
        return _load_hkex_records_from_sheet(
            sheet,
            header_row_index=HKEX_SECURITIES_HEADER_ROW,
        )
    except Exception as err:
        raise RuntimeError(
            f"hkex_fetch_failed:{HKEX_SOURCE_NAME}:{type(err).__name__}"
        ) from err


def _build_row(
    *,
    market: str,
    code: object,
    official_name: object,
    code_cleaner=_clean_code,
) -> dict[str, str] | None:
    resolved_code = code_cleaner(code)
    resolved_name = normalize_stock_official_name(official_name)
    if not resolved_code or not resolved_name:
        return None
    stock_key = normalize_stock_key(f"stock:{resolved_code}.{market}")
    return {
        "stock_key": stock_key,
        "market": market,
        "code": resolved_code,
        "official_name": resolved_name,
    }


def _append_source_rows(
    rows_by_stock_key: dict[str, dict[str, str]],
    *,
    frame: TabularRecords | list[dict[str, object]],
    source_name: str,
    market: str,
    code_column: str,
    name_column: str,
) -> None:
    _require_frame_columns(
        frame,
        source_name=source_name,
        required_columns=(code_column, name_column),
    )
    for record in _frame_records(frame, wanted_columns=(code_column, name_column)):
        row = _build_row(
            market=market,
            code=record.get(code_column),
            official_name=record.get(name_column),
        )
        if row is None:
            continue
        rows_by_stock_key.setdefault(row["stock_key"], row)


def _is_hk_equity_record(record: dict[str, object]) -> bool:
    category = _clean_text(record.get(HKEX_CATEGORY_COLUMN))
    if category != HKEX_EQUITY_CATEGORY:
        return False
    subcategory = _clean_text(record.get(HKEX_SUBCATEGORY_COLUMN))
    return HKEX_EQUITY_SUBCATEGORY_KEYWORD in subcategory


def append_hkex_equity_rows(
    rows_by_stock_key: dict[str, dict[str, str]],
    frame: TabularRecords | list[dict[str, object]],
    hk_converter: TextConverter,
) -> None:
    _require_frame_columns(
        frame,
        source_name=HKEX_SOURCE_NAME,
        required_columns=(
            HKEX_CODE_COLUMN,
            HKEX_NAME_COLUMN,
            HKEX_CATEGORY_COLUMN,
            HKEX_SUBCATEGORY_COLUMN,
        ),
        error_prefix="unexpected_hkex_columns",
    )
    for record in _frame_records(
        frame,
        wanted_columns=(
            HKEX_CODE_COLUMN,
            HKEX_NAME_COLUMN,
            HKEX_CATEGORY_COLUMN,
            HKEX_SUBCATEGORY_COLUMN,
        ),
    ):
        if not _is_hk_equity_record(record):
            continue
        official_name = hk_converter.convert(_clean_text(record.get(HKEX_NAME_COLUMN)))
        row = _build_row(
            market="HK",
            code=record.get(HKEX_CODE_COLUMN),
            official_name=official_name,
            code_cleaner=_clean_hkex_code,
        )
        if row is None:
            continue
        rows_by_stock_key.setdefault(row["stock_key"], row)


def build_security_master_rows(
    akshare_module=None,
    *,
    markets: tuple[str, ...] = DEFAULT_MARKETS,
    hkex_securities_frame: TabularRecords | list[dict[str, object]] | None = None,
    hk_converter: TextConverter | None = None,
) -> list[dict[str, str]]:
    rows_by_stock_key: dict[str, dict[str, str]] = {}
    if ("sh" in markets or "sz" in markets) and akshare_module is None:
        raise RuntimeError("missing_dependency:akshare")
    if "sh" in markets:
        _append_source_rows(
            rows_by_stock_key,
            frame=_fetch_frame(
                akshare_module,
                fetch_name="stock_info_sh_name_code",
                source_name="sh_main",
                symbol=SH_MAIN_INDICATOR,
            ),
            source_name="sh_main",
            market="SH",
            code_column="证券代码",
            name_column="证券简称",
        )
        _append_source_rows(
            rows_by_stock_key,
            frame=_fetch_frame(
                akshare_module,
                fetch_name="stock_info_sh_name_code",
                source_name="sh_star",
                symbol=SH_STAR_INDICATOR,
            ),
            source_name="sh_star",
            market="SH",
            code_column="证券代码",
            name_column="证券简称",
        )
    if "sz" in markets:
        _append_source_rows(
            rows_by_stock_key,
            frame=_fetch_frame(
                akshare_module,
                fetch_name="stock_info_sz_name_code",
                source_name="sz_a",
                symbol=SZ_A_INDICATOR,
            ),
            source_name="sz_a",
            market="SZ",
            code_column="A股代码",
            name_column="A股简称",
        )
    if "hk" in markets:
        append_hkex_equity_rows(
            rows_by_stock_key,
            hkex_securities_frame
            if hkex_securities_frame is not None
            else load_hkex_securities_frame(),
            hk_converter if hk_converter is not None else build_hk_opencc_converter(),
        )
    return [rows_by_stock_key[key] for key in sorted(rows_by_stock_key)]


def write_security_master_csv(
    rows: list[dict[str, str]], output_path: str | Path
) -> None:
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REQUIRED_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def build_security_master_csv(
    output_path: str | Path,
    akshare_module=None,
    *,
    markets: tuple[str, ...] = DEFAULT_MARKETS,
    hkex_securities_frame: TabularRecords | list[dict[str, object]] | None = None,
    hk_converter: TextConverter | None = None,
) -> int:
    akshare_client = akshare_module
    if akshare_client is None and ("sh" in markets or "sz" in markets):
        akshare_client = _load_akshare_module()
    rows = build_security_master_rows(
        akshare_client,
        markets=markets,
        hkex_securities_frame=hkex_securities_frame,
        hk_converter=hk_converter,
    )
    write_security_master_csv(rows, output_path)
    return len(rows)


def main() -> int:
    args = parse_args()
    configure_logging(level=getattr(args, "log_level", ""))
    try:
        markets = parse_markets(args.markets)
        row_count = build_security_master_csv(args.output_path, markets=markets)
    except RuntimeError as err:
        raise SystemExit(str(err)) from None
    logger.info("built security_master rows: %s", row_count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
