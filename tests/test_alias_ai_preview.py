from __future__ import annotations

from types import SimpleNamespace

from alphavault.infra.ai.alias_resolve_predictor import enrich_alias_tasks_with_ai


def test_enrich_alias_tasks_with_ai_skips_when_disabled() -> None:
    rows = enrich_alias_tasks_with_ai(
        [
            {
                "alias_key": "stock:茅台",
                "sample_post_uid": "weibo:1",
                "sample_evidence": "白酒龙头继续走强",
                "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
            }
        ],
        ai_enabled=False,
    )

    assert rows == [
        {
            "alias_key": "stock:茅台",
            "sample_post_uid": "weibo:1",
            "sample_evidence": "白酒龙头继续走强",
            "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
            "ai_status": "skipped",
            "ai_stock_code": "",
            "ai_official_name": "",
            "ai_confidence": "",
            "ai_reason": "白酒龙头继续走强",
            "ai_uncertain": "",
        }
    ]


def test_enrich_alias_tasks_with_ai_applies_model_predictions(
    monkeypatch,
) -> None:
    from alphavault.infra.ai import alias_resolve_predictor as predictor_module

    monkeypatch.setattr(
        predictor_module,
        "ai_is_configured",
        lambda: (True, ""),
    )
    monkeypatch.setattr(
        predictor_module,
        "ai_runtime_config_from_env",
        lambda **_kwargs: SimpleNamespace(
            api_mode="responses",
            model="test-model",
            base_url="",
            api_key="k",
            timeout_seconds=10.0,
            retries=0,
            temperature=0.0,
            reasoning_effort="low",
        ),
    )
    monkeypatch.setattr(
        predictor_module,
        "_call_ai_with_litellm",
        lambda **_kwargs: {
            "predictions": [
                {
                    "alias_key": "stock:茅台",
                    "stock_code": "600519.sh",
                    "official_name": "贵州茅台",
                    "confidence": 0.92,
                    "reason": "样例提到高端白酒和提价，最像贵州茅台。",
                    "is_uncertain": False,
                }
            ]
        },
    )

    rows = enrich_alias_tasks_with_ai(
        [
            {
                "alias_key": "stock:茅台",
                "sample_post_uid": "weibo:1",
                "sample_evidence": "白酒龙头继续走强",
                "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
            }
        ],
        ai_enabled=True,
    )

    assert rows == [
        {
            "alias_key": "stock:茅台",
            "sample_post_uid": "weibo:1",
            "sample_evidence": "白酒龙头继续走强",
            "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
            "ai_status": "ranked",
            "ai_stock_code": "600519.SH",
            "ai_official_name": "贵州茅台",
            "ai_confidence": "0.92",
            "ai_reason": "样例提到高端白酒和提价，最像贵州茅台。",
            "ai_uncertain": "",
        }
    ]
