from __future__ import annotations

from alphavault_reflex.services.thread_tree_lines import TREE_COLLAPSE_HINT_PREFIX
from alphavault_reflex.services.thread_tree_lines import build_tree_render_lines


def test_build_tree_render_lines_splits_id_suffix() -> None:
    lines = build_tree_render_lines("A [原帖 ID: 123]")
    assert len(lines) == 1
    assert lines[0]["content"] == "A"
    assert lines[0]["id_suffix"] == "[原帖 ID: 123]"


def test_build_tree_render_lines_marks_continuation_lines() -> None:
    lines = build_tree_render_lines("└── a\nb")
    assert len(lines) == 2
    assert lines[1]["prefix"] == " " * len("└── ")
    assert "av-tree-line-continuation" in lines[1]["row_class"]


def test_build_tree_render_lines_does_not_indent_collapsed_hint() -> None:
    lines = build_tree_render_lines(f"└── a\n{TREE_COLLAPSE_HINT_PREFIX}3 行")
    assert len(lines) == 2
    assert lines[1]["prefix"] == ""
    assert "av-tree-line-no-prefix" in lines[1]["row_class"]
