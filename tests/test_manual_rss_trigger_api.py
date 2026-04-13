from __future__ import annotations

import importlib
import importlib.util
from types import SimpleNamespace

from starlette.testclient import TestClient

from alphavault_reflex import alphavault_reflex as reflex_app


def _client() -> TestClient:
    return TestClient(reflex_app.app._api)


def test_manual_rss_trigger_returns_500_when_key_env_missing(monkeypatch) -> None:
    monkeypatch.delenv("WORKER_ADMIN_TRIGGER_KEY", raising=False)

    response = _client().get("/api/rss/trigger", params={"key": "anything"})

    assert response.status_code == 500
    assert response.json().get("error") == "missing_manual_trigger_key"


def test_manual_rss_trigger_returns_401_when_key_invalid(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "expected-key")

    response = _client().get("/api/rss/trigger", params={"key": "wrong-key"})

    assert response.status_code == 401
    assert response.json().get("error") == "unauthorized"


def test_manual_rss_trigger_returns_result_when_key_valid(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "expected-key")

    def _fake_run_manual_rss_ingest_once() -> dict[str, object]:
        return {
            "accepted_total": 2,
            "enqueue_error": False,
            "sources": [
                {
                    "source": "weibo",
                    "platform": "weibo",
                    "rss_url_count": 1,
                    "accepted": 2,
                    "enqueue_error": False,
                    "error": "",
                }
            ],
        }

    monkeypatch.setattr(
        reflex_app,
        "run_manual_rss_ingest_once",
        _fake_run_manual_rss_ingest_once,
    )

    response = _client().get("/api/rss/trigger", params={"key": "expected-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload.get("ok") is True
    assert payload.get("accepted_total") == 2
    assert payload.get("enqueue_error") is False


def test_manual_db_requeue_returns_400_when_mode_invalid(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "expected-key")

    response = _client().get(
        "/api/admin/requeue-from-db",
        params={"key": "expected-key", "mode": "wrong"},
    )

    assert response.status_code == 400
    assert response.json().get("error") == "invalid_mode"


def test_manual_db_requeue_returns_result_when_key_valid(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "expected-key")

    def _fake_run_manual_db_requeue_once(
        *, mode: str, platform: str | None, limit: int, dry_run: bool
    ) -> dict[str, object]:
        assert mode == "failed"
        assert platform == "weibo"
        assert limit == 10
        assert dry_run is True
        return {
            "mode": mode,
            "platform": platform,
            "limit": limit,
            "dry_run": dry_run,
            "scanned_total": 4,
            "enqueued_total": 0,
            "sources": [],
        }

    monkeypatch.setattr(
        reflex_app,
        "run_manual_db_requeue_once",
        _fake_run_manual_db_requeue_once,
    )

    response = _client().get(
        "/api/admin/requeue-from-db",
        params={
            "key": "expected-key",
            "mode": "failed",
            "platform": "weibo",
            "limit": "10",
            "dry_run": "1",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload.get("ok") is True
    assert payload.get("mode") == "failed"
    assert payload.get("platform") == "weibo"
    assert payload.get("scanned_total") == 4
    assert payload.get("enqueued_total") == 0


def test_manual_db_requeue_returns_500_when_runner_raises(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "expected-key")

    def _fake_raise(**_kwargs) -> dict[str, object]:
        raise RuntimeError("boom")

    monkeypatch.setattr(reflex_app, "run_manual_db_requeue_once", _fake_raise)

    response = _client().get(
        "/api/admin/requeue-from-db",
        params={"key": "expected-key", "mode": "failed"},
    )

    assert response.status_code == 500
    assert response.json().get("error") == "manual_db_requeue_failed"


def test_manual_process_metrics_returns_500_when_key_env_missing(monkeypatch) -> None:
    monkeypatch.delenv("WORKER_ADMIN_TRIGGER_KEY", raising=False)

    response = _client().get("/api/admin/processes", params={"key": "anything"})

    assert response.status_code == 500
    assert response.json().get("error") == "missing_manual_trigger_key"


def test_manual_process_metrics_returns_401_when_key_invalid(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "expected-key")

    response = _client().get("/api/admin/processes", params={"key": "wrong-key"})

    assert response.status_code == 401
    assert response.json().get("error") == "unauthorized"


def test_manual_process_metrics_returns_result_when_key_valid(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "expected-key")

    def _fake_load_process_metrics() -> list[dict[str, object]]:
        return [
            {
                "pid": 22,
                "rss_mb": 85.3,
                "memory_text": "85.3 MB",
                "cpu_percent": 1.2,
                "cmdline": "python3 -u weibo_rss_worker.py --log-level info",
            },
            {
                "pid": 11,
                "rss_mb": 64.1,
                "memory_text": "64.1 MB",
                "cpu_percent": 0.0,
                "cmdline": "python3 -m gunicorn alphavault_reflex.alphavault_reflex:app()",
            },
        ]

    monkeypatch.setattr(
        reflex_app,
        "load_process_metrics",
        _fake_load_process_metrics,
        raising=False,
    )

    response = _client().get("/api/admin/processes", params={"key": "expected-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload.get("ok") is True
    assert payload.get("processes") == _fake_load_process_metrics()


def test_manual_process_metrics_returns_500_when_reader_raises(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "expected-key")

    def _fake_raise() -> list[dict[str, object]]:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        reflex_app,
        "load_process_metrics",
        _fake_raise,
        raising=False,
    )

    response = _client().get("/api/admin/processes", params={"key": "expected-key"})

    assert response.status_code == 500
    assert response.json().get("error") == "manual_process_metrics_failed"


def test_manual_process_metrics_logs_exception_when_reader_raises(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ADMIN_TRIGGER_KEY", "expected-key")
    seen_messages: list[str] = []

    def _fake_raise() -> list[dict[str, object]]:
        raise RuntimeError("boom")

    def _fake_exception(message: str) -> None:
        seen_messages.append(message)

    monkeypatch.setattr(
        reflex_app,
        "load_process_metrics",
        _fake_raise,
        raising=False,
    )
    monkeypatch.setattr(
        reflex_app,
        "logger",
        SimpleNamespace(exception=_fake_exception),
        raising=False,
    )

    response = _client().get("/api/admin/processes", params={"key": "expected-key"})

    assert response.status_code == 500
    assert seen_messages == ["manual_process_metrics_failed"]


def test_load_process_metrics_parses_ps_output(monkeypatch) -> None:
    assert (
        importlib.util.find_spec("alphavault_reflex.services.process_metrics")
        is not None
    )

    process_metrics = importlib.import_module(
        "alphavault_reflex.services.process_metrics"
    )

    def _fake_run(*_args, **_kwargs) -> SimpleNamespace:
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "  11 65536 0.0 python3 -m gunicorn alphavault_reflex.alphavault_reflex:app()\n"
                "  22 98304 1.2 python3 -u weibo_rss_worker.py --log-level info\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(process_metrics.subprocess, "run", _fake_run)

    result = process_metrics.load_process_metrics()

    assert result == [
        {
            "pid": 22,
            "rss_mb": 96.0,
            "memory_text": "96.0 MB",
            "cpu_percent": 1.2,
            "cmdline": "python3 -u weibo_rss_worker.py --log-level info",
        },
        {
            "pid": 11,
            "rss_mb": 64.0,
            "memory_text": "64.0 MB",
            "cpu_percent": 0.0,
            "cmdline": "python3 -m gunicorn alphavault_reflex.alphavault_reflex:app()",
        },
    ]
