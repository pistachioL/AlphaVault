from __future__ import annotations

import reflex as rx


def tree_line_row(line: rx.Var[dict[str, str]]) -> rx.Component:
    content = rx.el.div(
        rx.el.span(line["content"]),
        rx.cond(
            line["id_suffix"] != "",
            rx.el.span(line["id_suffix"], class_name="av-tree-line-id"),
            rx.el.span(),
        ),
        class_name="av-tree-line-content",
    )
    return rx.cond(
        line["prefix"] != "",
        rx.el.div(
            rx.el.span(line["prefix"], class_name=line["prefix_class"]),
            content,
            class_name=line["row_class"],
        ),
        rx.el.div(
            content,
            class_name=line["row_class"],
        ),
    )


__all__ = ["tree_line_row"]
