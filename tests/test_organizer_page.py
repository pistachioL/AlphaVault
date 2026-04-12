from __future__ import annotations

from alphavault_reflex.pages.organizer import organizer_page


def test_organizer_page_uses_loading_state_to_hide_fake_empty() -> None:
    rendered = str(organizer_page().render())

    assert "show_loading_rx_state_" in rendered
    assert "show_pending_empty_rx_state_" in rendered
