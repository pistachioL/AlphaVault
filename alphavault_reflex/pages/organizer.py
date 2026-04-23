from __future__ import annotations

import reflex as rx
from reflex.vars.base import Var

from alphavault_reflex.organizer_state import (
    OrganizerState,
    PendingRow,
    SECTION_ALIAS_MANUAL,
    SECTION_SECTOR_SECTOR,
    SECTION_STOCK_ALIAS,
    SECTION_STOCK_SECTOR,
)

LOADING_TEXT = "加载中…"


def _checkbox_checked(value: object) -> bool | rx.Var[bool]:
    if isinstance(value, Var):
        return value.bool()
    return bool(value)


def _section_loading() -> rx.Component:
    return rx.el.div(
        rx.spinner(size="3"),
        rx.text(LOADING_TEXT, class_name="av-research-muted"),
        style={
            "display": "flex",
            "alignItems": "center",
            "gap": "10px",
            "marginTop": "12px",
        },
    )


def _candidate_card(row: rx.Var[PendingRow]) -> rx.Component:
    row_action_pending = (
        OrganizerState.candidate_action_pending_id == row["candidate_id"]
    )
    return rx.el.div(
        rx.hstack(
            rx.cond(
                row["relation_type"] == SECTION_STOCK_ALIAS,
                rx.checkbox(
                    checked=_checkbox_checked(row["selected"]),
                    on_change=lambda checked: OrganizerState.toggle_stock_alias_candidate(
                        row["candidate_id"],
                        checked,
                    ),
                    disabled=OrganizerState.show_loading
                    | OrganizerState.has_candidate_action_pending,
                ),
                rx.el.div(),
            ),
            rx.text(row["candidate_key"], class_name="av-research-side-title"),
            align="center",
            spacing="2",
        ),
        rx.cond(
            row["relation_type"] == SECTION_STOCK_ALIAS,
            rx.cond(
                row["left_key"] != "",
                rx.text(
                    f"归并到：{row['left_key']}",
                    class_name="av-research-muted",
                ),
                rx.el.div(),
            ),
            rx.el.div(),
        ),
        rx.cond(
            row["suggestion_reason"] != "",
            rx.text(row["suggestion_reason"], class_name="av-research-muted"),
            rx.el.div(),
        ),
        rx.cond(
            (row["evidence_summary"] != "")
            & (row["evidence_summary"] != row["suggestion_reason"]),
            rx.text(row["evidence_summary"], class_name="av-research-muted"),
            rx.el.div(),
        ),
        rx.cond(
            (row["relation_type"] == SECTION_STOCK_ALIAS)
            & (row["sample_post_uid"] != ""),
            rx.text(
                f"样例帖子：{row['sample_post_uid']}",
                class_name="av-research-muted",
            ),
            rx.el.div(),
        ),
        rx.cond(
            (row["relation_type"] == SECTION_STOCK_ALIAS)
            & (row["sample_evidence"] != ""),
            rx.text(row["sample_evidence"], class_name="av-research-muted"),
            rx.el.div(),
        ),
        rx.cond(
            (row["relation_type"] == SECTION_STOCK_ALIAS)
            & (row["sample_raw_text_excerpt"] != ""),
            rx.text(
                row["sample_raw_text_excerpt"],
                class_name="av-research-muted",
            ),
            rx.el.div(),
        ),
        rx.cond(
            (row["relation_type"] == SECTION_STOCK_ALIAS) & (row["ai_status"] != ""),
            rx.el.div(
                rx.text(
                    f"{row['ai_display_title']}：{row['ai_display_label']}",
                    class_name="av-research-muted",
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
                    rx.text(row["ai_reason"], class_name="av-research-muted"),
                    rx.el.div(),
                ),
            ),
            rx.el.div(),
        ),
        rx.cond(
            (row["relation_type"] == SECTION_STOCK_ALIAS) & row_action_pending,
            rx.el.div(
                rx.spinner(size="1"),
                rx.text("处理中…", class_name="av-research-muted"),
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "8px",
                    "marginTop": "8px",
                },
            ),
            rx.el.div(),
        ),
        rx.hstack(
            rx.button(
                "确认",
                on_click=lambda: OrganizerState.accept_candidate(row["candidate_id"]),
                class_name="av-btn av-btn-small",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending,
            ),
            rx.button(
                "忽略",
                on_click=lambda: OrganizerState.ignore_candidate(row["candidate_id"]),
                variant="soft",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending,
            ),
            rx.button(
                "不再推荐",
                on_click=lambda: OrganizerState.block_candidate(row["candidate_id"]),
                variant="soft",
                color_scheme="gray",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending,
            ),
            spacing="2",
            margin_top="10px",
        ),
        class_name="av-research-side-item",
    )


def _summary_card(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["label"], class_name="av-research-muted"),
        rx.text(
            row["value"],
            class_name="av-research-side-title",
            style={"fontSize": "24px", "lineHeight": "1.1"},
        ),
        class_name="av-research-side-item",
        style={
            "padding": "12px 14px",
            "display": "flex",
            "flexDirection": "column",
            "gap": "6px",
            "minWidth": "0",
        },
    )


def _summary_toolbar(rows: rx.Var[list[dict[str, str]]]) -> rx.Component:
    return rx.vstack(
        rx.vstack(
            rx.text("全库状态", class_name="av-research-muted"),
            rx.el.div(
                rx.foreach(rows, _summary_card),
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(120px, 1fr))",
                    "gap": "10px",
                    "width": "100%",
                },
            ),
            spacing="2",
            width="100%",
            align="stretch",
        ),
        width="100%",
        align="stretch",
    )


def _stock_alias_batch_toolbar() -> rx.Component:
    return rx.vstack(
        _summary_toolbar(OrganizerState.stock_alias_status_summary_rows),
        rx.hstack(
            rx.text(
                f"已选 {OrganizerState.selected_stock_alias_candidate_count} 条",
                class_name="av-research-muted",
            ),
            rx.button(
                "AI重跑当前页",
                on_click=OrganizerState.rerun_stock_alias_ai_current_page,
                class_name="av-btn av-btn-small",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending,
            ),
            rx.button(
                "再看10条",
                on_click=OrganizerState.load_more_stock_alias_candidates,
                variant="soft",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending,
            ),
            rx.button(
                "全选本页",
                on_click=OrganizerState.select_all_stock_alias_candidates,
                variant="soft",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending,
            ),
            rx.button(
                "清空选择",
                on_click=OrganizerState.clear_selected_stock_alias_candidates,
                variant="soft",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending,
            ),
            rx.button(
                "批量确认",
                on_click=OrganizerState.batch_accept_selected_candidates,
                class_name="av-btn av-btn-small",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending
                | (~OrganizerState.has_selected_stock_alias_candidates),
            ),
            rx.button(
                "批量忽略",
                on_click=OrganizerState.batch_ignore_selected_candidates,
                variant="soft",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending
                | (~OrganizerState.has_selected_stock_alias_candidates),
            ),
            rx.button(
                "批量不再推荐",
                on_click=OrganizerState.batch_block_selected_candidates,
                variant="soft",
                color_scheme="gray",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_candidate_action_pending
                | (~OrganizerState.has_selected_stock_alias_candidates),
            ),
            spacing="3",
            wrap="wrap",
            width="100%",
        ),
        rx.cond(
            OrganizerState.stock_alias_auto_merge_message != "",
            rx.text(
                OrganizerState.stock_alias_auto_merge_message,
                class_name="av-research-muted",
            ),
            rx.el.div(),
        ),
        align="start",
        spacing="2",
        margin_top="12px",
    )


def _alias_manual_batch_toolbar() -> rx.Component:
    return rx.vstack(
        _summary_toolbar(OrganizerState.alias_manual_status_summary_rows),
        rx.hstack(
            rx.text(
                f"已选 {OrganizerState.selected_alias_manual_count} 条",
                class_name="av-research-muted",
            ),
            rx.button(
                "AI重跑当前页",
                on_click=OrganizerState.rerun_alias_manual_ai_current_page,
                class_name="av-btn av-btn-small",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_alias_manual_action_pending,
            ),
            rx.button(
                "再看10条",
                on_click=OrganizerState.load_more_alias_tasks,
                variant="soft",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_alias_manual_action_pending,
            ),
            rx.button(
                "全选本页",
                on_click=OrganizerState.select_all_alias_manual_tasks,
                variant="soft",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_alias_manual_action_pending,
            ),
            rx.button(
                "清空选择",
                on_click=OrganizerState.clear_selected_alias_manual_tasks,
                variant="soft",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_alias_manual_action_pending,
            ),
            rx.button(
                "批量确认",
                on_click=OrganizerState.batch_confirm_selected_alias_manual_tasks,
                class_name="av-btn av-btn-small",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_alias_manual_action_pending
                | (~OrganizerState.has_selected_alias_manual_tasks),
            ),
            rx.button(
                "批量忽略",
                on_click=OrganizerState.batch_ignore_selected_alias_manual_tasks,
                variant="soft",
                disabled=OrganizerState.show_loading
                | OrganizerState.has_alias_manual_action_pending
                | (~OrganizerState.has_selected_alias_manual_tasks),
            ),
            spacing="3",
            wrap="wrap",
            width="100%",
        ),
        align="start",
        spacing="2",
        margin_top="12px",
    )


def _stock_alias_group_header(row: rx.Var[PendingRow]) -> rx.Component:
    return rx.el.div(
        rx.text(row["group_label"], class_name="av-research-side-title"),
        style={
            "marginTop": "16px",
        },
    )


def _stock_alias_display_row(row: rx.Var[PendingRow]) -> rx.Component:
    return rx.cond(
        row["row_kind"] == "group_header",
        _stock_alias_group_header(row),
        _candidate_card(row),
    )


def _stock_alias_grouped_display_list(
    rows: rx.Var[list[PendingRow]] | list[PendingRow],
) -> rx.Component:
    return rx.el.div(
        rx.foreach(rows, _stock_alias_display_row),
        class_name="av-research-side-list",
    )


def _manual_alias_card(row: rx.Var[PendingRow]) -> rx.Component:
    row_action_pending = (
        OrganizerState.alias_manual_action_pending_key == row["alias_key"]
    )
    return rx.el.div(
        rx.hstack(
            rx.checkbox(
                checked=_checkbox_checked(row["selected"]),
                on_change=lambda checked: OrganizerState.toggle_alias_manual_task(
                    row["alias_key"],
                    checked,
                ),
                disabled=OrganizerState.show_loading
                | OrganizerState.has_alias_manual_action_pending,
            ),
            rx.text(row["alias_key"], class_name="av-research-side-title"),
            align="center",
            spacing="2",
        ),
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
        rx.el.div(
            rx.text(
                f"AI 状态：{row['ai_status_display']}",
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
                row["ai_validation_label"] != "",
                rx.text(
                    f"校验：{row['ai_validation_label']}",
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
        rx.cond(
            row_action_pending,
            rx.el.div(
                rx.spinner(size="1"),
                rx.text("处理中…", class_name="av-research-muted"),
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "8px",
                    "marginTop": "8px",
                },
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
                disabled=OrganizerState.show_loading
                | OrganizerState.has_alias_manual_action_pending,
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
                disabled=OrganizerState.show_loading
                | OrganizerState.has_alias_manual_action_pending,
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
                    disabled=OrganizerState.show_loading
                    | OrganizerState.has_alias_manual_action_pending,
                ),
                rx.button(
                    "拉黑",
                    on_click=OrganizerState.block_alias_manual,
                    variant="soft",
                    color_scheme="gray",
                    disabled=OrganizerState.show_loading
                    | OrganizerState.has_alias_manual_action_pending,
                ),
                rx.dialog.close(
                    rx.button(
                        "关",
                        variant="soft",
                        on_click=OrganizerState.close_alias_manual_dialog,
                        disabled=OrganizerState.show_loading
                        | OrganizerState.has_alias_manual_action_pending,
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
                disabled=OrganizerState.show_loading,
            ),
            rx.button(
                "未确认简称",
                on_click=lambda: OrganizerState.set_active_section(
                    SECTION_ALIAS_MANUAL
                ),
                variant="soft",
                disabled=OrganizerState.show_loading,
            ),
            rx.button(
                "个股→板块",
                on_click=lambda: OrganizerState.set_active_section(
                    SECTION_STOCK_SECTOR
                ),
                variant="soft",
                disabled=OrganizerState.show_loading,
            ),
            rx.button(
                "板块→板块",
                on_click=lambda: OrganizerState.set_active_section(
                    SECTION_SECTOR_SECTOR
                ),
                variant="soft",
                disabled=OrganizerState.show_loading,
            ),
            spacing="3",
        ),
        rx.cond(
            OrganizerState.active_section == SECTION_STOCK_ALIAS,
            _stock_alias_batch_toolbar(),
            rx.el.div(),
        ),
        rx.cond(
            OrganizerState.active_section == SECTION_ALIAS_MANUAL,
            _alias_manual_batch_toolbar(),
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
            OrganizerState.show_loading,
            _section_loading(),
            rx.cond(
                OrganizerState.has_pending_rows,
                rx.cond(
                    OrganizerState.active_section == SECTION_ALIAS_MANUAL,
                    rx.el.div(
                        rx.foreach(OrganizerState.pending_rows, _manual_alias_card),
                        class_name="av-research-side-list",
                    ),
                    rx.cond(
                        OrganizerState.active_section == SECTION_STOCK_ALIAS,
                        _stock_alias_grouped_display_list(
                            OrganizerState.stock_alias_grouped_display_rows
                        ),
                        rx.el.div(
                            rx.foreach(OrganizerState.pending_rows, _candidate_card),
                            class_name="av-research-side-list",
                        ),
                    ),
                ),
                rx.cond(
                    OrganizerState.show_pending_empty,
                    rx.text("当前分区没有待确认项。", class_name="av-research-muted"),
                    rx.el.div(),
                ),
            ),
        ),
        _alias_manual_dialog(),
        class_name="av-research-page",
    )
