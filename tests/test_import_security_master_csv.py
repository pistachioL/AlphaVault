from __future__ import annotations

from contextlib import contextmanager
import importlib
import logging
from pathlib import Path
import sys


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


def test_import_csv_into_security_master_bulk_upserts_rows_with_progress_and_rebuilds_shadow(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    script = importlib.import_module("import_security_master_csv")
    csv_path = tmp_path / "security_master.csv"
    csv_path.write_text(
        "stock_key,market,code,official_name\n"
        "stock:600519.SH,SH,600519,贵州茅台\n"
        "stock:1810.HK,HK,1810,小米集团-W\n"
        "stock:601899.SH,SH,601899,紫金矿业\n",
        encoding="utf-8",
    )

    bulk_upserted: list[tuple[object, list[dict[str, str]]]] = []
    rebuilt: list[object] = []
    fake_conn = object()

    @contextmanager
    def _fake_use_conn(engine_or_conn):
        assert engine_or_conn is engine
        yield fake_conn

    def _fake_bulk_upsert_security_master_stocks(
        engine_or_conn,
        rows: list[dict[str, str]],
    ) -> int:
        bulk_upserted.append((engine_or_conn, [dict(row) for row in rows]))
        return len(rows)

    def _fake_rebuild_stock_dict_shadow_best_effort(engine_or_conn) -> bool:
        rebuilt.append(engine_or_conn)
        return True

    engine = object()

    monkeypatch.setattr(script, "IMPORT_SECURITY_MASTER_BATCH_SIZE", 2, raising=False)
    monkeypatch.setattr(script, "use_conn", _fake_use_conn, raising=False)
    monkeypatch.setattr(
        script,
        "bulk_upsert_security_master_stocks",
        _fake_bulk_upsert_security_master_stocks,
        raising=False,
    )
    monkeypatch.setattr(
        script,
        "rebuild_stock_dict_shadow_best_effort",
        _fake_rebuild_stock_dict_shadow_best_effort,
    )
    with caplog.at_level(logging.INFO):
        row_count = script.import_csv_into_security_master(
            engine, csv_path, batch_size=2
        )

    assert row_count == 3
    assert bulk_upserted == [
        (
            fake_conn,
            [
                {
                    "stock_key": "stock:600519.SH",
                    "market": "SH",
                    "code": "600519",
                    "official_name": "贵州茅台",
                },
                {
                    "stock_key": "stock:1810.HK",
                    "market": "HK",
                    "code": "1810",
                    "official_name": "小米集团-W",
                },
            ],
        ),
        (
            fake_conn,
            [
                {
                    "stock_key": "stock:601899.SH",
                    "market": "SH",
                    "code": "601899",
                    "official_name": "紫金矿业",
                }
            ],
        ),
    ]
    assert caplog.messages == [
        "imported security_master rows: 2/3",
        "imported security_master rows: 3/3",
        "rebuilding security_master shadow dict...",
        "rebuilt security_master shadow dict",
    ]
    assert rebuilt == [fake_conn]


def test_parse_args_accepts_batch_size(monkeypatch) -> None:
    script = importlib.import_module("import_security_master_csv")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "import_security_master_csv.py",
            "--csv-path",
            "/tmp/security_master.csv",
            "--batch-size",
            "20",
        ],
    )

    args = script.parse_args()

    assert args.csv_path == "/tmp/security_master.csv"
    assert args.batch_size == 20


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
