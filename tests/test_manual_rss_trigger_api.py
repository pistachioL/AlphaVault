from __future__ import annotations

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
