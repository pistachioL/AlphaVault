from __future__ import annotations

import argparse
import csv
import importlib
from pathlib import Path
from typing import Protocol

import pandas as pd

from alphavault.domains.stock.key_match import normalize_stock_code
from alphavault.domains.stock.keys import normalize_stock_key

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


class TextConverter(Protocol):
    def convert(self, text: str) -> str: ...


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
    try:
        return bool(pd.isna(value))
    except TypeError:
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


def _normalize_fullwidth_alnum_and_space(text: str) -> str:
    normalized_chars: list[str] = []
    for char in text:
        codepoint = ord(char)
        if codepoint == 0x3000:
            normalized_chars.append(" ")
            continue
        if 0xFF10 <= codepoint <= 0xFF19:
            normalized_chars.append(chr(codepoint - 0xFEE0))
            continue
        if 0xFF21 <= codepoint <= 0xFF3A:
            normalized_chars.append(chr(codepoint - 0xFEE0))
            continue
        if 0xFF41 <= codepoint <= 0xFF5A:
            normalized_chars.append(chr(codepoint - 0xFEE0))
            continue
        normalized_chars.append(char)
    return "".join(normalized_chars)


def _normalize_official_name(value: object) -> str:
    return "".join(_normalize_fullwidth_alnum_and_space(_clean_text(value)).split())


def _require_frame_columns(
    frame: pd.DataFrame,
    *,
    source_name: str,
    required_columns: tuple[str, ...],
    error_prefix: str = "unexpected_akshare_columns",
) -> None:
    for name in required_columns:
        if name in frame.columns:
            continue
        raise RuntimeError(f"{error_prefix}:{source_name}:{name}")


def _fetch_frame(
    akshare_module, *, fetch_name: str, source_name: str, **kwargs
) -> pd.DataFrame:
    fetch_fn = getattr(akshare_module, fetch_name)
    try:
        return fetch_fn(**kwargs)
    except Exception as err:
        raise RuntimeError(
            f"akshare_fetch_failed:{source_name}:{type(err).__name__}"
        ) from err


def load_hkex_securities_frame() -> pd.DataFrame:
    try:
        return pd.read_excel(
            HKEX_SECURITIES_URL,
            header=HKEX_SECURITIES_HEADER_ROW,
            dtype=str,
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
    resolved_name = _normalize_official_name(official_name)
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
    frame: pd.DataFrame,
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
    for record in frame[[code_column, name_column]].to_dict("records"):
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
    frame: pd.DataFrame,
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
    for record in frame[
        [
            HKEX_CODE_COLUMN,
            HKEX_NAME_COLUMN,
            HKEX_CATEGORY_COLUMN,
            HKEX_SUBCATEGORY_COLUMN,
        ]
    ].to_dict("records"):
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
    hkex_securities_frame: pd.DataFrame | None = None,
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
    hkex_securities_frame: pd.DataFrame | None = None,
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
    try:
        markets = parse_markets(args.markets)
        row_count = build_security_master_csv(args.output_path, markets=markets)
    except RuntimeError as err:
        raise SystemExit(str(err)) from None
    print(f"built security_master rows: {row_count}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
