from __future__ import annotations

from alphavault_reflex.services import homework_board
from alphavault_reflex.services import research_data
from alphavault_reflex.services import sector_hot_read
from alphavault_reflex.services import stock_hot_read
from alphavault_reflex import homework_state
from alphavault_reflex import organizer_state
from alphavault import research_sector_view
from alphavault import research_signal_view
from alphavault import follow_pages
from alphavault.domains.thread_tree import service as thread_tree_service
from alphavault.domains.thread_tree import build as thread_tree_build
from alphavault.domains.thread_tree import render as thread_tree_render
from alphavault.domains.stock import object_index
from alphavault.domains.stock import key_match
from alphavault.app.relation import candidate_builders
from alphavault.domains.relation import relation_candidates
from alphavault.worker import stock_hot_payload_builder
from alphavault.worker import sector_hot_payload_builder


def test_homework_board_has_no_top_level_pandas_import() -> None:
    assert not hasattr(homework_board, "pd")


def test_research_data_has_no_top_level_pandas_import() -> None:
    assert not hasattr(research_data, "pd")


def test_sector_hot_read_has_no_top_level_pandas_import() -> None:
    assert not hasattr(sector_hot_read, "pd")


def test_stock_hot_read_has_no_top_level_pandas_import() -> None:
    assert not hasattr(stock_hot_read, "pd")


def test_homework_state_has_no_top_level_pandas_import() -> None:
    assert not hasattr(homework_state, "pd")


def test_organizer_state_has_no_top_level_pandas_import() -> None:
    assert not hasattr(organizer_state, "pd")


def test_research_sector_view_has_no_top_level_pandas_import() -> None:
    assert not hasattr(research_sector_view, "pd")


def test_thread_tree_service_has_no_top_level_pandas_import() -> None:
    assert not hasattr(thread_tree_service, "pd")


def test_research_signal_view_has_no_top_level_pandas_import() -> None:
    assert not hasattr(research_signal_view, "pd")


def test_follow_pages_has_no_top_level_pandas_import() -> None:
    assert not hasattr(follow_pages, "pd")


def test_thread_tree_build_has_no_top_level_pandas_import() -> None:
    assert not hasattr(thread_tree_build, "pd")


def test_thread_tree_render_has_no_top_level_pandas_import() -> None:
    assert not hasattr(thread_tree_render, "pd")


def test_stock_object_index_has_no_top_level_pandas_import() -> None:
    assert not hasattr(object_index, "pd")


def test_stock_key_match_has_no_top_level_pandas_import() -> None:
    assert not hasattr(key_match, "pd")


def test_candidate_builders_has_no_top_level_pandas_import() -> None:
    assert not hasattr(candidate_builders, "pd")


def test_relation_candidates_has_no_top_level_pandas_import() -> None:
    assert not hasattr(relation_candidates, "pd")


def test_stock_hot_payload_builder_has_no_top_level_pandas_import() -> None:
    assert not hasattr(stock_hot_payload_builder, "pd")


def test_sector_hot_payload_builder_has_no_top_level_pandas_import() -> None:
    assert not hasattr(sector_hot_payload_builder, "pd")
