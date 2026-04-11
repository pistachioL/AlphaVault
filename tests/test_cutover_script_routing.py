from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest


def test_manual_run_ai_rejects_unknown_post_uid_prefix(monkeypatch) -> None:
    script = importlib.import_module("manual_run_ai")

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        script,
        "parse_args",
        lambda: SimpleNamespace(post_uids="bad:1", log_level="info"),
    )
    monkeypatch.setattr(
        script,
        "_build_config",
        lambda _args: SimpleNamespace(ai_rpm=0.0, prompt_version="test"),
    )

    class _FakeLimiter:
        def __init__(self, _rpm: float) -> None:
            self.rpm = _rpm

    monkeypatch.setattr(script, "RateLimiter", _FakeLimiter)

    with pytest.raises(RuntimeError, match="unknown_source_platform:bad:1"):
        script.main()


def test_reset_ai_results_rejects_unknown_post_uid_prefix(monkeypatch) -> None:
    script = importlib.import_module("reset_ai_results")

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        script,
        "parse_args",
        lambda: SimpleNamespace(
            all=False,
            post_uids="bad:1",
            yes=False,
            chunk_size=200,
            dry_run=True,
            log_level="info",
        ),
    )
    monkeypatch.setattr(
        script,
        "_iter_source_engines",
        lambda: [("weibo", object()), ("xueqiu", object())],
    )

    with pytest.raises(RuntimeError, match="unknown_source_platform:bad:1"):
        script.main()


def test_scan_and_reset_invalid_ai_tags_rejects_unknown_post_uid_prefix(
    monkeypatch,
) -> None:
    script = importlib.import_module("scan_and_reset_invalid_ai_tags")

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        script,
        "parse_args",
        lambda: SimpleNamespace(
            prompt_version="",
            limit=0,
            chunk_size=200,
            dry_run=False,
            yes=True,
            log_level="info",
        ),
    )
    monkeypatch.setattr(
        script,
        "_iter_source_engines",
        lambda: [("weibo", object()), ("xueqiu", object())],
    )

    scan_calls = {"count": 0}

    def _fake_scan_invalid_post_uids(
        _engine,
        *,
        prompt_version: str,
        limit: int,
    ) -> tuple[list[str], dict[str, str], int]:
        del prompt_version, limit
        scan_calls["count"] += 1
        if scan_calls["count"] == 1:
            return ["bad:1"], {}, 1
        return [], {}, 0

    monkeypatch.setattr(script, "_scan_invalid_post_uids", _fake_scan_invalid_post_uids)

    with pytest.raises(RuntimeError, match="unknown_source_platform:bad:1"):
        script.main()
