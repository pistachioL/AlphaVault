from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from alphavault_reflex.services import tree_loader


def test_load_single_post_for_tree_from_env_keeps_xueqiu_uid(monkeypatch) -> None:
    requested_uid = "xueqiu:status:381213336\t"
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(tree_loader, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        tree_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [
            SimpleNamespace(name="weibo", url="weibo-url", token="weibo-token"),
            SimpleNamespace(name="xueqiu", url="xueqiu-url", token="xueqiu-token"),
        ],
    )

    def _fake_cached(
        db_url: str, auth_token: str, source_name: str, post_uid: str
    ) -> pd.DataFrame:
        calls.append((source_name, post_uid))
        return pd.DataFrame([{"post_uid": requested_uid}])

    posts, err = tree_loader.load_single_post_for_tree_from_env(
        requested_uid,
        load_cached_fn=_fake_cached,
    )

    assert err == ""
    assert not posts.empty
    assert calls == [("xueqiu", requested_uid)]
