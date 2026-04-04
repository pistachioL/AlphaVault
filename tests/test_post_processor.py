from __future__ import annotations

from typing import cast

from alphavault.ai.topic_prompt_v4 import TOPIC_PROMPT_VERSION
from alphavault.db.turso_db import TursoEngine
from alphavault.rss.utils import RateLimiter
from alphavault.worker import post_processor
from alphavault.worker.runtime_models import LLMConfig


def _build_config(*, prompt_version: str) -> LLMConfig:
    return LLMConfig(
        api_key="test-key",
        model="test-model",
        prompt_version=prompt_version,
        relevant_threshold=0.5,
        base_url="",
        api_mode="responses",
        ai_stream=False,
        ai_retries=0,
        ai_temperature=0.1,
        ai_reasoning_effort="low",
        ai_rpm=0.0,
        ai_timeout_seconds=30.0,
        trace_out=None,
        verbose=False,
    )


def test_process_one_post_uid_forces_topic_prompt_v4(monkeypatch) -> None:
    seen: dict[str, object] = {}
    config = _build_config(prompt_version="legacy-v3")

    monkeypatch.setattr(
        post_processor,
        "mark_ai_error",
        lambda *_args, **_kwargs: None,
    )

    def _fake_v4(**kwargs):  # type: ignore[no-untyped-def]
        seen["post_uid"] = kwargs["post_uid"]
        seen["config"] = kwargs["config"]
        return True

    monkeypatch.setattr(
        post_processor,
        "process_one_post_uid_topic_prompt_v4",
        _fake_v4,
    )

    ok = post_processor.process_one_post_uid(
        engine=cast(TursoEngine, object()),
        post_uid="weibo:1",
        config=config,
        limiter=RateLimiter(0),
    )

    assert ok is True
    assert seen["post_uid"] == "weibo:1"
    assert isinstance(seen["config"], LLMConfig)
    assert seen["config"].prompt_version == TOPIC_PROMPT_VERSION
    assert config.prompt_version == "legacy-v3"
