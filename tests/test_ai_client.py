from __future__ import annotations

import types

from alphavault.ai import _client as ai_client_module


def test_call_ai_with_litellm_runs_request_gate_for_each_retry_attempt(
    monkeypatch,
) -> None:
    gate_calls: list[str] = []
    attempt_calls: list[str] = []

    def _request_gate() -> None:
        gate_calls.append("gate")

    def _fake_responses(**_kwargs):  # type: ignore[no-untyped-def]
        attempt_calls.append("attempt")
        if len(attempt_calls) < 3:
            raise RuntimeError("temporary_fail")
        return object()

    monkeypatch.setattr(
        ai_client_module,
        "_import_litellm",
        lambda: types.SimpleNamespace(responses=_fake_responses),
    )
    monkeypatch.setattr(ai_client_module, "_extract_ai_text", lambda _resp: '{"ok": 1}')
    monkeypatch.setattr(
        ai_client_module, "_append_trace", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(ai_client_module.time, "sleep", lambda _seconds: None)

    parsed = ai_client_module._call_ai_with_litellm(
        prompt="prompt",
        api_mode="responses",
        ai_stream=False,
        model_name="openai/gpt-5.2",
        base_url="",
        api_key="test-key",
        timeout_seconds=30.0,
        retry_count=2,
        temperature=0.1,
        reasoning_effort="low",
        trace_out=None,
        trace_label="test",
        request_gate=_request_gate,
    )

    assert parsed == {"ok": 1}
    assert attempt_calls == ["attempt", "attempt", "attempt"]
    assert gate_calls == ["gate", "gate", "gate"]
