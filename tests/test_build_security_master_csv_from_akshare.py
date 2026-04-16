from __future__ import annotations

import importlib
from pathlib import Path


class _FakeFrame:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = [dict(row) for row in rows]
        self.columns = list(self._rows[0]) if self._rows else []

    def to_dict(self, orient: str) -> list[dict[str, object]]:
        assert orient == "records"
        return [dict(row) for row in self._rows]


class _FakeAkshare:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def stock_info_sh_name_code(self, *, symbol: str) -> _FakeFrame:
        self.calls.append(("stock_info_sh_name_code", symbol))
        if symbol == "主板A股":
            return _FakeFrame(
                [
                    {"证券代码": "600519", "证券简称": "贵州茅台"},
                    {"证券代码": "600519", "证券简称": "贵州茅台-重复"},
                    {"证券代码": "", "证券简称": "空代码"},
                ]
            )
        if symbol == "科创板":
            return _FakeFrame([{"证券代码": "688111", "证券简称": "金山办公"}])
        raise AssertionError(f"unexpected symbol: {symbol}")

    def stock_info_sz_name_code(self, *, symbol: str) -> _FakeFrame:
        self.calls.append(("stock_info_sz_name_code", symbol))
        if symbol != "A股列表":
            raise AssertionError(f"unexpected symbol: {symbol}")
        return _FakeFrame(
            [
                {"A股代码": "000001", "A股简称": "平安银行"},
                {"A股代码": "300750", "A股简称": "宁德时代"},
                {"A股代码": "000002", "A股简称": ""},
            ]
        )


class _FakeOpenCC:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def convert(self, text: str) -> str:
        self.calls.append(text)
        mapping = {
            "騰訊控股": "腾讯控股",
            "小米集團－Ｗ": "小米集团－Ｗ",
            "騰 訊 控 股": "腾 讯 控 股",
            "匯豐控股": "汇丰控股",
            "博時HashKey比特幣ETF": "博时HashKey比特币ETF",
        }
        return mapping.get(text, text)


def _hkex_equity_frame() -> _FakeFrame:
    return _FakeFrame(
        [
            {
                "股份代號": "00700",
                "股份名稱": "騰訊控股",
                "分類": "股本",
                "次分類": "股本證券(主板)",
            },
            {
                "股份代號": "01810",
                "股份名稱": "小米集團－Ｗ",
                "分類": "股本",
                "次分類": "股本證券(主板)",
            },
            {
                "股份代號": "08495",
                "股份名稱": "1957 & CO. (HOSPITALITY)",
                "分類": "股本",
                "次分類": "股本證券(GEM)",
            },
            {
                "股份代號": "00860",
                "股份名稱": "ＡＰＯＬＬＯ出行",
                "分類": "股本",
                "次分類": "股本證券(主板)",
            },
            {
                "股份代號": "03009",
                "股份名稱": "博時HashKey比特幣ETF",
                "分類": "交易所買賣產品",
                "次分類": "交易所買賣基金",
            },
        ]
    )


def test_build_security_master_rows_reads_expected_sources() -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")
    fake_akshare = _FakeAkshare()
    fake_opencc = _FakeOpenCC()

    rows = script.build_security_master_rows(
        fake_akshare,
        hkex_securities_frame=_hkex_equity_frame(),
        hk_converter=fake_opencc,
    )

    assert fake_akshare.calls == [
        ("stock_info_sh_name_code", "主板A股"),
        ("stock_info_sh_name_code", "科创板"),
        ("stock_info_sz_name_code", "A股列表"),
    ]
    assert fake_opencc.calls == [
        "騰訊控股",
        "小米集團－Ｗ",
        "1957 & CO. (HOSPITALITY)",
        "ＡＰＯＬＬＯ出行",
    ]
    assert rows == [
        {
            "stock_key": "stock:000001.SZ",
            "market": "SZ",
            "code": "000001",
            "official_name": "平安银行",
        },
        {
            "stock_key": "stock:00700.HK",
            "market": "HK",
            "code": "00700",
            "official_name": "腾讯控股",
        },
        {
            "stock_key": "stock:00860.HK",
            "market": "HK",
            "code": "00860",
            "official_name": "APOLLO出行",
        },
        {
            "stock_key": "stock:01810.HK",
            "market": "HK",
            "code": "01810",
            "official_name": "小米集团-W",
        },
        {
            "stock_key": "stock:08495.HK",
            "market": "HK",
            "code": "08495",
            "official_name": "1957&CO.(HOSPITALITY)",
        },
        {
            "stock_key": "stock:300750.SZ",
            "market": "SZ",
            "code": "300750",
            "official_name": "宁德时代",
        },
        {
            "stock_key": "stock:600519.SH",
            "market": "SH",
            "code": "600519",
            "official_name": "贵州茅台",
        },
        {
            "stock_key": "stock:688111.SH",
            "market": "SH",
            "code": "688111",
            "official_name": "金山办公",
        },
    ]


def test_build_security_master_rows_fails_when_akshare_columns_change() -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")

    class _BrokenAkshare:
        def stock_info_sh_name_code(self, *, symbol: str) -> _FakeFrame:
            if symbol == "主板A股":
                return _FakeFrame([{"代码": "600519", "证券简称": "贵州茅台"}])
            if symbol == "科创板":
                return _FakeFrame([{"证券代码": "688111", "证券简称": "金山办公"}])
            raise AssertionError(f"unexpected symbol: {symbol}")

        def stock_info_sz_name_code(self, *, symbol: str) -> _FakeFrame:
            return _FakeFrame([{"A股代码": "000001", "A股简称": "平安银行"}])

    try:
        script.build_security_master_rows(
            _BrokenAkshare(),
            hkex_securities_frame=_hkex_equity_frame(),
            hk_converter=_FakeOpenCC(),
        )
    except RuntimeError as err:
        assert str(err) == "unexpected_akshare_columns:sh_main:证券代码"
    else:
        raise AssertionError("expected akshare columns error")


def test_build_security_master_csv_writes_expected_csv(tmp_path: Path) -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")
    output_path = tmp_path / "security_master.csv"

    row_count = script.build_security_master_csv(
        output_path,
        _FakeAkshare(),
        hkex_securities_frame=_hkex_equity_frame(),
        hk_converter=_FakeOpenCC(),
    )

    assert row_count == 8
    assert output_path.read_text(encoding="utf-8") == (
        "stock_key,market,code,official_name\n"
        "stock:000001.SZ,SZ,000001,平安银行\n"
        "stock:00700.HK,HK,00700,腾讯控股\n"
        "stock:00860.HK,HK,00860,APOLLO出行\n"
        "stock:01810.HK,HK,01810,小米集团-W\n"
        "stock:08495.HK,HK,08495,1957&CO.(HOSPITALITY)\n"
        "stock:300750.SZ,SZ,300750,宁德时代\n"
        "stock:600519.SH,SH,600519,贵州茅台\n"
        "stock:688111.SH,SH,688111,金山办公\n"
    )


def test_build_security_master_rows_skips_hk_when_markets_exclude_it() -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")
    fake_akshare = _FakeAkshare()

    rows = script.build_security_master_rows(
        fake_akshare,
        markets=("sh", "sz"),
        hkex_securities_frame=_hkex_equity_frame(),
        hk_converter=_FakeOpenCC(),
    )

    assert fake_akshare.calls == [
        ("stock_info_sh_name_code", "主板A股"),
        ("stock_info_sh_name_code", "科创板"),
        ("stock_info_sz_name_code", "A股列表"),
    ]
    assert rows == [
        {
            "stock_key": "stock:000001.SZ",
            "market": "SZ",
            "code": "000001",
            "official_name": "平安银行",
        },
        {
            "stock_key": "stock:300750.SZ",
            "market": "SZ",
            "code": "300750",
            "official_name": "宁德时代",
        },
        {
            "stock_key": "stock:600519.SH",
            "market": "SH",
            "code": "600519",
            "official_name": "贵州茅台",
        },
        {
            "stock_key": "stock:688111.SH",
            "market": "SH",
            "code": "688111",
            "official_name": "金山办公",
        },
    ]


def test_build_security_master_rows_removes_whitespace_inside_official_name() -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")
    fake_opencc = _FakeOpenCC()

    class _NameSpaceAkshare:
        def stock_info_sh_name_code(self, *, symbol: str) -> _FakeFrame:
            if symbol == "主板A股":
                return _FakeFrame([{"证券代码": "600519", "证券简称": "贵州茅台"}])
            if symbol == "科创板":
                return _FakeFrame([{"证券代码": "688111", "证券简称": "金山办公"}])
            raise AssertionError(f"unexpected symbol: {symbol}")

        def stock_info_sz_name_code(self, *, symbol: str) -> _FakeFrame:
            if symbol != "A股列表":
                raise AssertionError(f"unexpected symbol: {symbol}")
            return _FakeFrame([{"A股代码": "000012", "A股简称": "南  玻Ａ"}])

    rows = script.build_security_master_rows(
        _NameSpaceAkshare(),
        markets=("sz", "hk"),
        hkex_securities_frame=_FakeFrame(
            [
                {
                    "股份代號": "00700",
                    "股份名稱": "騰 訊 控 股",
                    "分類": "股本",
                    "次分類": "股本證券(主板)",
                }
            ]
        ),
        hk_converter=fake_opencc,
    )

    assert rows == [
        {
            "stock_key": "stock:000012.SZ",
            "market": "SZ",
            "code": "000012",
            "official_name": "南玻A",
        },
        {
            "stock_key": "stock:00700.HK",
            "market": "HK",
            "code": "00700",
            "official_name": "腾讯控股",
        },
    ]


def test_parse_markets_fails_when_input_has_unknown_market() -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")

    try:
        script.parse_markets("sh,sz,us")
    except RuntimeError as err:
        assert str(err) == "invalid_markets:us"
    else:
        raise AssertionError("expected invalid markets error")


def test_append_hkex_equity_rows_fails_when_hkex_columns_change() -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")

    rows_by_stock_key: dict[str, dict[str, str]] = {}

    try:
        script.append_hkex_equity_rows(
            rows_by_stock_key,
            _FakeFrame(
                [
                    {
                        "证券代码": "00700",
                        "股份名稱": "騰訊控股",
                        "分類": "股本",
                        "次分類": "股本證券(主板)",
                    }
                ]
            ),
            _FakeOpenCC(),
        )
    except RuntimeError as err:
        assert str(err) == "unexpected_hkex_columns:hk_main:股份代號"
    else:
        raise AssertionError("expected hkex columns error")


def test_append_hkex_equity_rows_filters_non_equity_rows() -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")
    rows_by_stock_key: dict[str, dict[str, str]] = {}

    script.append_hkex_equity_rows(
        rows_by_stock_key,
        _hkex_equity_frame(),
        _FakeOpenCC(),
    )

    assert "stock:03009.HK" not in rows_by_stock_key
    assert set(rows_by_stock_key) == {
        "stock:00700.HK",
        "stock:01810.HK",
        "stock:08495.HK",
        "stock:00860.HK",
    }


def test_load_hkex_securities_frame_fails_with_clear_error_when_fetch_breaks(
    monkeypatch,
) -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")

    def _fake_get(*args, **kwargs):
        del args, kwargs
        raise ConnectionError("boom")

    monkeypatch.setattr(script.requests, "get", _fake_get)

    try:
        script.load_hkex_securities_frame()
    except RuntimeError as err:
        assert str(err) == "hkex_fetch_failed:hk_main:ConnectionError"
    else:
        raise AssertionError("expected hkex fetch error")


def test_build_hk_opencc_converter_fails_with_clear_error_when_dependency_missing(
    monkeypatch,
) -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")

    def _fake_import_module(name: str):
        if name == "opencc":
            raise ModuleNotFoundError("missing")
        return importlib.import_module(name)

    monkeypatch.setattr(script.importlib, "import_module", _fake_import_module)

    try:
        script.build_hk_opencc_converter()
    except RuntimeError as err:
        assert str(err) == "missing_dependency:opencc"
    else:
        raise AssertionError("expected opencc dependency error")


def test_main_exits_with_clean_message_when_build_fails(monkeypatch) -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")

    class _Args:
        output_path = "/tmp/security_master.csv"
        markets = "sh,sz,hk"

    def _fake_parse_args():
        return _Args()

    def _fake_build_security_master_csv(
        output_path: str,
        akshare_module=None,
        *,
        markets=(),
        hkex_securities_frame=None,
        hk_converter=None,
    ) -> int:
        del output_path, akshare_module, markets, hkex_securities_frame, hk_converter
        raise RuntimeError("hkex_fetch_failed:hk_main:ConnectionError")

    monkeypatch.setattr(script, "parse_args", _fake_parse_args)
    monkeypatch.setattr(
        script,
        "build_security_master_csv",
        _fake_build_security_master_csv,
    )

    try:
        script.main()
    except SystemExit as err:
        assert str(err) == "hkex_fetch_failed:hk_main:ConnectionError"
    else:
        raise AssertionError("expected system exit")


def test_main_exits_with_clean_message_when_markets_invalid(monkeypatch) -> None:
    script = importlib.import_module("build_security_master_csv_from_akshare")

    class _Args:
        output_path = "/tmp/security_master.csv"
        markets = "sh,us"

    def _fake_parse_args():
        return _Args()

    monkeypatch.setattr(script, "parse_args", _fake_parse_args)

    try:
        script.main()
    except SystemExit as err:
        assert str(err) == "invalid_markets:us"
    else:
        raise AssertionError("expected system exit")
