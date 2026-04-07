from __future__ import annotations

import importlib
from pathlib import Path


def test_load_security_master_rows_reads_expected_csv_columns(tmp_path: Path) -> None:
    script = importlib.import_module("import_security_master_csv")
    csv_path = tmp_path / "security_master.csv"
    csv_path.write_text(
        "stock_key,market,code,official_name\nstock:600519.SH,SH,600519,贵州茅台\n",
        encoding="utf-8",
    )

    rows = script.load_security_master_rows(csv_path)

    assert rows == [
        {
            "stock_key": "stock:600519.SH",
            "market": "SH",
            "code": "600519",
            "official_name": "贵州茅台",
        }
    ]


def test_import_csv_into_security_master_upserts_rows_and_rebuilds_shadow(
    tmp_path: Path,
    monkeypatch,
) -> None:
    script = importlib.import_module("import_security_master_csv")
    csv_path = tmp_path / "security_master.csv"
    csv_path.write_text(
        "stock_key,market,code,official_name\n"
        "stock:600519.SH,SH,600519,贵州茅台\n"
        "stock:1810.HK,HK,1810,小米集团-W\n",
        encoding="utf-8",
    )

    upserted: list[tuple[str, str, str, str]] = []
    rebuilt: list[object] = []

    def _fake_upsert_security_master_stock(
        engine_or_conn,
        *,
        stock_key: str,
        market: str,
        code: str,
        official_name: str,
    ) -> None:
        del engine_or_conn
        upserted.append((stock_key, market, code, official_name))

    def _fake_rebuild_stock_dict_shadow_best_effort(engine_or_conn) -> bool:
        rebuilt.append(engine_or_conn)
        return True

    monkeypatch.setattr(
        script,
        "upsert_security_master_stock",
        _fake_upsert_security_master_stock,
    )
    monkeypatch.setattr(
        script,
        "rebuild_stock_dict_shadow_best_effort",
        _fake_rebuild_stock_dict_shadow_best_effort,
    )

    engine = object()

    row_count = script.import_csv_into_security_master(engine, csv_path)

    assert row_count == 2
    assert upserted == [
        ("stock:600519.SH", "SH", "600519", "贵州茅台"),
        ("stock:1810.HK", "HK", "1810", "小米集团-W"),
    ]
    assert rebuilt == [engine]


def test_load_security_master_rows_fails_when_required_field_is_empty(
    tmp_path: Path,
) -> None:
    script = importlib.import_module("import_security_master_csv")
    csv_path = tmp_path / "security_master.csv"
    csv_path.write_text(
        "stock_key,market,code,official_name\nstock:600519.SH,SH,600519,\n",
        encoding="utf-8",
    )

    try:
        script.load_security_master_rows(csv_path)
    except RuntimeError as err:
        assert str(err) == "invalid_csv_row:2:official_name"
    else:
        raise AssertionError("expected invalid csv row error")
