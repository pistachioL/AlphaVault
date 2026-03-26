from __future__ import annotations

import json

from alphavault_reflex.services.stock_backfill import (
    merge_post_assertions,
    run_targeted_stock_backfill,
)


def test_run_targeted_stock_backfill_returns_stock_specific_trade_assertions(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.services.stock_backfill.ai_is_configured",
        lambda: (True, ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.stock_backfill._call_ai_with_litellm",
        lambda **kwargs: {
            "status": "relevant",
            "invest_score": 0.9,
            "assertions": [
                {
                    "action": "trade.buy",
                    "action_strength": 2,
                    "summary": "他这里是想先小仓买一点",
                    "evidence": "今天先买一点紫金矿业",
                    "confidence": 0.88,
                }
            ],
        },
    )

    rows = run_targeted_stock_backfill(
        {
            "post_uid": "p1",
            "platform_post_id": "123",
            "author": "alice",
            "created_at": "2026-03-26 10:00:00",
            "url": "https://example.com/p1",
            "raw_text": "今天先买一点紫金矿业，别急着满仓。",
            "display_md": "今天先买一点紫金矿业，别急着满仓。",
        },
        stock_key="stock:601899.SH",
        display_name="紫金矿业",
    )

    assert rows[0]["topic_key"] == "stock:601899.SH"
    assert rows[0]["action"] == "trade.buy"
    assert json.loads(rows[0]["stock_codes_json"]) == ["601899.SH"]
    assert json.loads(rows[0]["stock_names_json"]) == ["紫金矿业"]


def test_merge_post_assertions_keeps_existing_and_skips_duplicates() -> None:
    existing = [
        {
            "topic_key": "stock:601899.SH",
            "action": "trade.watch",
            "action_strength": 1,
            "summary": "先看着",
            "evidence": "先看看",
            "confidence": 0.4,
            "stock_codes_json": '["601899.SH"]',
            "stock_names_json": '["紫金矿业"]',
            "industries_json": "[]",
            "commodities_json": "[]",
            "indices_json": "[]",
        }
    ]
    new_rows = [
        {
            "topic_key": "stock:601899.SH",
            "action": "trade.watch",
            "action_strength": 1,
            "summary": "先看着",
            "evidence": "先看看",
            "confidence": 0.4,
            "stock_codes_json": '["601899.SH"]',
            "stock_names_json": '["紫金矿业"]',
            "industries_json": "[]",
            "commodities_json": "[]",
            "indices_json": "[]",
        },
        {
            "topic_key": "stock:601899.SH",
            "action": "trade.buy",
            "action_strength": 2,
            "summary": "准备先上车",
            "evidence": "先买一点",
            "confidence": 0.8,
            "stock_codes_json": '["601899.SH"]',
            "stock_names_json": '["紫金矿业"]',
            "industries_json": "[]",
            "commodities_json": "[]",
            "indices_json": "[]",
        },
    ]

    merged = merge_post_assertions(existing, new_rows)

    assert [row["summary"] for row in merged] == ["先看着", "准备先上车"]
