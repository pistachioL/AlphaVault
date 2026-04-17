from __future__ import annotations

from ast import literal_eval

from alphavault_reflex.pages.organizer import _candidate_card
from alphavault_reflex.pages.organizer import _stock_alias_group_header
from alphavault_reflex.pages.organizer import organizer_page


def _render_texts(component) -> list[str]:  # type: ignore[no-untyped-def]
    rendered = component.render()
    texts: list[str] = []

    def _walk(node: object) -> None:
        if isinstance(node, dict):
            cond_state = node.get("cond_state")
            true_value = node.get("true_value")
            false_value = node.get("false_value")
            if isinstance(cond_state, str):
                if cond_state == "true":
                    _walk(true_value)
                    return
                if cond_state == "false":
                    _walk(false_value)
                    return
                _walk(true_value)
                _walk(false_value)
                return
            contents = node.get("contents")
            if isinstance(contents, str):
                try:
                    texts.append(str(literal_eval(contents)))
                except (SyntaxError, ValueError):
                    texts.append(contents)
            for value in node.values():
                _walk(value)
            return
        if isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(rendered)
    return texts


def test_organizer_page_uses_loading_state_to_hide_fake_empty() -> None:
    rendered = str(organizer_page().render())

    assert "show_loading_rx_state_" in rendered
    assert "show_pending_empty_rx_state_" in rendered


def test_stock_alias_candidate_card_shows_merge_target_once() -> None:
    texts = _render_texts(
        _candidate_card(
            {
                "relation_type": "stock_alias",
                "candidate_key": "stock:紫金矿业",
                "candidate_id": "candidate-1",
                "selected": False,
                "left_key": "stock:601899.SH",
                "right_key": "stock:紫金矿业",
                "suggestion_reason": "近30天同票提及 46 次",
                "evidence_summary": "近30天同票提及 46 次",
                "ai_status": "ranked",
                "ai_reason": "AI 看上下文像同一只票的简称。",
                "ai_confidence": "0.88",
                "sample_post_uid": "",
                "sample_evidence": "",
                "sample_raw_text_excerpt": "",
                "ai_display_title": "AI 状态",
                "ai_display_label": "已排序",
            }
        )
    )

    assert "stock:紫金矿业" in texts
    assert "归并到：stock:601899.SH" in texts
    assert texts.count("近30天同票提及 46 次") == 1
    assert "AI 状态：已排序" in texts
    assert "AI 看上下文像同一只票的简称。" in texts
    assert "AI 置信度：0.88" in texts


def test_stock_alias_candidate_card_renders_pending_hint() -> None:
    rendered = str(
        _candidate_card(
            {
                "relation_type": "stock_alias",
                "candidate_key": "stock:紫金矿业",
                "candidate_id": "candidate-1",
                "selected": False,
                "left_key": "stock:601899.SH",
                "right_key": "stock:紫金矿业",
                "suggestion_reason": "近30天同票提及 46 次",
                "evidence_summary": "近30天同票提及 46 次",
                "ai_status": "",
                "ai_reason": "",
                "ai_confidence": "",
                "sample_post_uid": "",
                "sample_evidence": "",
                "sample_raw_text_excerpt": "",
                "ai_display_title": "",
                "ai_display_label": "",
            }
        ).render()
    )

    assert "candidate_action_pending_id" in rendered
    assert "RadixThemesSpinner" in rendered
    assert "\\\\u5904\\\\u7406\\\\u4e2d\\\\u2026" in rendered


def test_organizer_page_renders_stock_alias_batch_toolbar() -> None:
    texts = _render_texts(organizer_page())
    rendered = str(organizer_page().render())

    assert "AI重跑当前页" in texts
    assert "再看10条" in texts
    assert "全选本页" in texts
    assert "清空选择" in texts
    assert "批量确认" in texts
    assert "批量忽略" in texts
    assert "批量不再推荐" in texts
    assert "selected_stock_alias_candidate_count" in rendered


def test_stock_alias_group_list_renders_group_headers() -> None:
    merge_texts = _render_texts(
        _stock_alias_group_header(
            {
                "row_kind": "group_header",
                "group_label": "建议合并（1）",
            }
        )
    )
    reject_texts = _render_texts(
        _stock_alias_group_header(
            {
                "row_kind": "group_header",
                "group_label": "不建议合并（1）",
            }
        )
    )

    assert "建议合并（1）" in merge_texts
    assert "不建议合并（1）" in reject_texts


def test_non_stock_alias_candidate_card_does_not_show_merge_target() -> None:
    texts = _render_texts(
        _candidate_card(
            {
                "relation_type": "stock_sector",
                "candidate_key": "cluster:面板",
                "candidate_id": "candidate-2",
                "selected": False,
                "left_key": "stock:000725.SZ",
                "right_key": "cluster:面板",
                "suggestion_reason": "近30天板块共现 12 次",
                "evidence_summary": "样例帖子提到面板涨价",
                "ai_status": "",
                "ai_reason": "",
                "ai_confidence": "",
                "sample_post_uid": "",
                "sample_evidence": "",
                "sample_raw_text_excerpt": "",
                "ai_display_title": "",
                "ai_display_label": "",
            }
        )
    )

    assert "cluster:面板" in texts
    assert all(not text.startswith("归并到：") for text in texts)
    assert "近30天板块共现 12 次" in texts
    assert "样例帖子提到面板涨价" in texts
