from __future__ import annotations

import reflex as rx

from alphavault_reflex.organizer_state import (
    OrganizerState,
    SECTION_ALIAS_MANUAL,
    SECTION_SECTOR_SECTOR,
    SECTION_STOCK_ALIAS,
    SECTION_STOCK_SECTOR,
)


def _candidate_card(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["candidate_key"], class_name="av-research-side-title"),
        rx.text(row["suggestion_reason"], class_name="av-research-muted"),
        rx.text(row["evidence_summary"], class_name="av-research-muted"),
        rx.hstack(
            rx.button(
                "确认",
                on_click=lambda: OrganizerState.accept_candidate(row["candidate_id"]),
                class_name="av-btn av-btn-small",
            ),
            rx.button(
                "忽略",
                on_click=lambda: OrganizerState.ignore_candidate(row["candidate_id"]),
                variant="soft",
            ),
            rx.button(
                "不再推荐",
                on_click=lambda: OrganizerState.block_candidate(row["candidate_id"]),
                variant="soft",
                color_scheme="gray",
            ),
            spacing="2",
            margin_top="10px",
        ),
        class_name="av-research-side-item",
    )


def _manual_alias_card(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["alias_key"], class_name="av-research-side-title"),
        rx.text(
            f"已尝试 {row['attempt_count']} 次",
            class_name="av-research-muted",
        ),
        rx.cond(
            row["sample_post_uid"] != "",
            rx.text(
                f"样例帖子：{row['sample_post_uid']}",
                class_name="av-research-muted",
            ),
            rx.el.div(),
        ),
        rx.cond(
            row["sample_evidence"] != "",
            rx.text(row["sample_evidence"], class_name="av-research-muted"),
            rx.el.div(),
        ),
        rx.cond(
            row["sample_raw_text_excerpt"] != "",
            rx.text(
                row["sample_raw_text_excerpt"],
                class_name="av-research-muted",
            ),
            rx.el.div(),
        ),
        rx.cond(
            row["ai_status"] != "",
            rx.el.div(
                rx.text(
                    f"AI 状态：{row['ai_status']}",
                    class_name="av-research-muted",
                ),
                rx.cond(
                    row["ai_stock_code"] != "",
                    rx.text(
                        f"AI 代码：{row['ai_stock_code']}",
                        class_name="av-research-muted",
                    ),
                    rx.el.div(),
                ),
                rx.cond(
                    row["ai_official_name"] != "",
                    rx.text(
                        f"AI 全名：{row['ai_official_name']}",
                        class_name="av-research-muted",
                    ),
                    rx.el.div(),
                ),
                rx.cond(
                    row["ai_confidence"] != "",
                    rx.text(
                        f"AI 置信度：{row['ai_confidence']}",
                        class_name="av-research-muted",
                    ),
                    rx.el.div(),
                ),
                rx.cond(
                    row["ai_reason"] != "",
                    rx.text(
                        row["ai_reason"],
                        class_name="av-research-muted",
                    ),
                    rx.el.div(),
                ),
            ),
            rx.el.div(),
        ),
        rx.hstack(
            rx.button(
                "手动处理",
                on_click=lambda: OrganizerState.open_alias_manual_dialog(
                    row["alias_key"],
                    row["ai_stock_code"],
                ),
                class_name="av-btn av-btn-small",
            ),
            spacing="2",
            margin_top="10px",
        ),
        class_name="av-research-side-item",
    )


def _alias_manual_dialog() -> rx.Component:
    return rx.dialog.root(
        rx.dialog.content(
            rx.dialog.title("手动处理别名"),
            rx.cond(
                OrganizerState.alias_manual_alias_key != "",
                rx.text(
                    OrganizerState.alias_manual_alias_key,
                    class_name="av-research-muted",
                ),
                rx.el.div(),
            ),
            rx.input(
                value=OrganizerState.alias_manual_target_input,
                on_change=OrganizerState.set_alias_manual_target_input,
                placeholder="输入股票代码，比如 601899.SH",
                width="320px",
            ),
            rx.cond(
                OrganizerState.alias_manual_error != "",
                rx.text(
                    OrganizerState.alias_manual_error,
                    class_name="av-error-text",
                ),
                rx.el.div(),
            ),
            rx.hstack(
                rx.button(
                    "确认归并",
                    on_click=OrganizerState.confirm_alias_manual_merge,
                    class_name="av-btn av-btn-small",
                ),
                rx.button(
                    "拉黑",
                    on_click=OrganizerState.block_alias_manual,
                    variant="soft",
                    color_scheme="gray",
                ),
                rx.dialog.close(
                    rx.button(
                        "关",
                        variant="soft",
                        on_click=OrganizerState.close_alias_manual_dialog,
                    )
                ),
                spacing="2",
                margin_top="12px",
            ),
            style={
                "max_width": "min(640px, 92vw)",
                "max_height": "85vh",
                "overflow": "auto",
            },
        ),
        open=OrganizerState.alias_manual_dialog_open,
        on_open_change=OrganizerState.set_alias_manual_dialog_open,
    )


def organizer_page() -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.heading("整理中心", size="6"),
            rx.text("先看未确认简称，再看个股别名候选，最后看板块关系。"),
            class_name="av-research-head",
        ),
        rx.hstack(
            rx.button(
                "个股归并",
                on_click=lambda: OrganizerState.set_active_section(SECTION_STOCK_ALIAS),
                variant="soft",
            ),
            rx.button(
                "未确认简称",
                on_click=lambda: OrganizerState.set_active_section(
                    SECTION_ALIAS_MANUAL
                ),
                variant="soft",
            ),
            rx.button(
                "个股→板块",
                on_click=lambda: OrganizerState.set_active_section(
                    SECTION_STOCK_SECTOR
                ),
                variant="soft",
            ),
            rx.button(
                "板块→板块",
                on_click=lambda: OrganizerState.set_active_section(
                    SECTION_SECTOR_SECTOR
                ),
                variant="soft",
            ),
            spacing="3",
        ),
        rx.cond(
            OrganizerState.active_section == SECTION_ALIAS_MANUAL,
            rx.hstack(
                rx.button(
                    "AI预判前10条",
                    on_click=OrganizerState.preview_alias_ai_batch,
                    class_name="av-btn av-btn-small",
                ),
                rx.button(
                    "再看30条",
                    on_click=OrganizerState.load_more_alias_tasks,
                    variant="soft",
                ),
                spacing="3",
            ),
            rx.el.div(),
        ),
        rx.cond(
            OrganizerState.load_error != "",
            rx.el.div(
                rx.text("错误：", class_name="av-error-title"),
                rx.text(OrganizerState.load_error, class_name="av-error-text"),
                class_name="av-error",
            ),
            rx.el.div(),
        ),
        rx.cond(
            OrganizerState.has_pending_rows,
            rx.cond(
                OrganizerState.active_section == SECTION_ALIAS_MANUAL,
                rx.el.div(
                    rx.foreach(OrganizerState.pending_rows, _manual_alias_card),
                    class_name="av-research-side-list",
                ),
                rx.el.div(
                    rx.foreach(OrganizerState.pending_rows, _candidate_card),
                    class_name="av-research-side-list",
                ),
            ),
            rx.text("当前分区没有待确认项。", class_name="av-research-muted"),
        ),
        _alias_manual_dialog(),
        class_name="av-research-page",
    )
