from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest


def _load_script_module():
    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "requeue_all_ai_from_db.py"
    )
    spec = importlib.util.spec_from_file_location("requeue_all_ai_from_db", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed_to_load_script_module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_script_can_run_directly_with_help() -> None:
    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "requeue_all_ai_from_db.py"
    )

    result = subprocess.run(
        [sys.executable, str(script_path), "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--base-url" in result.stdout


def _args(**overrides):
    values = {
        "base_url": "http://127.0.0.1:8080/",
        "key": "expected-key",
        "platform": "weibo",
        "limit": 2,
        "sleep_seconds": 3.0,
        "max_rounds": 5,
        "mode_order": "failed,legacy_unprocessed",
        "timeout_seconds": 10.0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _payload(
    *,
    mode: str,
    dry_run: bool,
    scanned_total: int,
    enqueued_total: int = 0,
    queue_backlog: int = 0,
    error: str = "",
):
    return {
        "ok": True,
        "mode": mode,
        "dry_run": dry_run,
        "scanned_total": scanned_total,
        "enqueued_total": enqueued_total,
        "sources": [
            {
                "source": "weibo",
                "platform": "weibo",
                "mode": mode,
                "scanned": scanned_total,
                "enqueued": enqueued_total,
                "queue_backlog": queue_backlog,
                "error": error,
            }
        ],
    }


def test_main_runs_both_modes_until_done(monkeypatch) -> None:
    script = _load_script_module()
    call_order: list[tuple[str, bool]] = []
    sleep_calls: list[float] = []
    responses = {
        ("failed", False): [
            _payload(mode="failed", dry_run=False, scanned_total=2, enqueued_total=2),
            _payload(mode="failed", dry_run=False, scanned_total=1, enqueued_total=1),
        ],
        ("failed", True): [
            _payload(mode="failed", dry_run=True, scanned_total=1, queue_backlog=1),
            _payload(mode="failed", dry_run=True, scanned_total=0, queue_backlog=0),
        ],
        ("legacy_unprocessed", False): [
            _payload(
                mode="legacy_unprocessed",
                dry_run=False,
                scanned_total=2,
                enqueued_total=2,
            )
        ],
        ("legacy_unprocessed", True): [
            _payload(
                mode="legacy_unprocessed",
                dry_run=True,
                scanned_total=0,
                queue_backlog=0,
            )
        ],
    }

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(script, "parse_args", lambda: _args())

    def _fake_request_requeue(**kwargs):
        mode = str(kwargs["mode"])
        dry_run = bool(kwargs["dry_run"])
        call_order.append((mode, dry_run))
        return responses[(mode, dry_run)].pop(0)

    monkeypatch.setattr(script, "_request_requeue", _fake_request_requeue)
    monkeypatch.setattr(
        script.time, "sleep", lambda seconds: sleep_calls.append(seconds)
    )

    assert script.main() == 0
    assert call_order == [
        ("failed", False),
        ("failed", True),
        ("failed", False),
        ("failed", True),
        ("legacy_unprocessed", False),
        ("legacy_unprocessed", True),
    ]
    assert sleep_calls == [3.0]


def test_main_uses_env_key_when_arg_missing(monkeypatch) -> None:
    script = _load_script_module()
    seen_keys: list[str] = []

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        script,
        "parse_args",
        lambda: _args(key="", mode_order="failed", max_rounds=1, sleep_seconds=0.0),
    )
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "env-key")

    def _fake_request_requeue(**kwargs):
        seen_keys.append(str(kwargs["key"]))
        return _payload(
            mode=str(kwargs["mode"]),
            dry_run=bool(kwargs["dry_run"]),
            scanned_total=0,
            queue_backlog=0,
        )

    monkeypatch.setattr(script, "_request_requeue", _fake_request_requeue)

    assert script.main() == 0
    assert seen_keys == ["env-key", "env-key"]


def test_main_raises_when_max_rounds_reached(monkeypatch) -> None:
    script = _load_script_module()

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        script,
        "parse_args",
        lambda: _args(mode_order="failed", max_rounds=1, sleep_seconds=0.0),
    )
    monkeypatch.setattr(
        script,
        "_request_requeue",
        lambda **kwargs: _payload(
            mode=str(kwargs["mode"]),
            dry_run=bool(kwargs["dry_run"]),
            scanned_total=1,
            queue_backlog=1 if bool(kwargs["dry_run"]) else 0,
            enqueued_total=1 if not bool(kwargs["dry_run"]) else 0,
        ),
    )

    with pytest.raises(RuntimeError, match="max_rounds_reached mode=failed"):
        script.main()


def test_main_stops_when_scanned_total_is_zero_even_if_backlog_remains(
    monkeypatch,
) -> None:
    script = _load_script_module()
    call_order: list[tuple[str, bool]] = []

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        script,
        "parse_args",
        lambda: _args(mode_order="failed", max_rounds=3, sleep_seconds=0.0),
    )

    def _fake_request_requeue(**kwargs):
        mode = str(kwargs["mode"])
        dry_run = bool(kwargs["dry_run"])
        call_order.append((mode, dry_run))
        if dry_run:
            return _payload(
                mode=mode,
                dry_run=True,
                scanned_total=0,
                queue_backlog=9,
            )
        return _payload(
            mode=mode,
            dry_run=False,
            scanned_total=2,
            enqueued_total=2,
            queue_backlog=9,
        )

    monkeypatch.setattr(script, "_request_requeue", _fake_request_requeue)

    assert script.main() == 0
    assert call_order == [("failed", False), ("failed", True)]


def test_main_raises_when_sources_are_empty(monkeypatch) -> None:
    script = _load_script_module()

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        script,
        "parse_args",
        lambda: _args(mode_order="failed", max_rounds=1, sleep_seconds=0.0),
    )
    monkeypatch.setattr(
        script,
        "_request_requeue",
        lambda **kwargs: {
            "ok": True,
            "mode": str(kwargs["mode"]),
            "dry_run": bool(kwargs["dry_run"]),
            "scanned_total": 0,
            "enqueued_total": 0,
            "sources": [],
        },
    )

    with pytest.raises(RuntimeError, match="empty_sources mode=failed"):
        script.main()


def test_request_requeue_raises_on_http_error(monkeypatch) -> None:
    script = _load_script_module()

    class _FakeResponse:
        status_code = 500
        text = "boom"

        def json(self):  # type: ignore[no-untyped-def]
            return {"ok": False, "error": "boom"}

    monkeypatch.setattr(
        script.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(),
    )

    with pytest.raises(RuntimeError, match="http_error status=500"):
        script._request_requeue(
            base_url="http://127.0.0.1:8080/",
            key="expected-key",
            platform="weibo",
            limit=2,
            dry_run=False,
            mode="failed",
            timeout_seconds=10.0,
        )
