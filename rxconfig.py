import reflex as rx
from reflex.plugins import SitemapPlugin

config = rx.Config(
    app_name="alphavault_reflex",
    show_built_with_reflex=False,
    disable_plugins=[SitemapPlugin],
)
