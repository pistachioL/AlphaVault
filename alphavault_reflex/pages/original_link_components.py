from __future__ import annotations

import reflex as rx

from alphavault_reflex.services.original_link import ORIGINAL_LINK_APP_ICON
from alphavault_reflex.services.original_link import ORIGINAL_LINK_APP_ICON_CLASS_NAME
from alphavault_reflex.services.original_link import ORIGINAL_LINK_CLASS_NAME
from alphavault_reflex.services.original_link import ORIGINAL_LINK_POST_UID_ATTR
from alphavault_reflex.services.original_link import ORIGINAL_LINK_URL_ATTR


def _merge_class_names(*class_names: str) -> str:
    return " ".join(name.strip() for name in class_names if name.strip())


def original_post_link(
    label: str | rx.Var[str],
    url: str | rx.Var[str],
    post_uid: str | rx.Var[str],
    *,
    class_name: str = "",
) -> rx.Component:
    return rx.link(
        rx.el.span(
            ORIGINAL_LINK_APP_ICON,
            class_name=ORIGINAL_LINK_APP_ICON_CLASS_NAME,
            style={"display": "none", "marginRight": "0.3em"},
            aria_hidden="true",
        ),
        label,
        href=url,
        is_external=True,
        class_name=_merge_class_names(ORIGINAL_LINK_CLASS_NAME, class_name),
        custom_attrs={
            ORIGINAL_LINK_URL_ATTR: url,
            ORIGINAL_LINK_POST_UID_ATTR: post_uid,
        },
    )


__all__ = ["original_post_link"]
