from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from alphavault_reflex.services import turso_read


def test_load_single_post_for_tree_from_env_keeps_xueqiu_uid(monkeypatch) -> None:
    requested_uid = "xueqiu:status:381213336\t"
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(turso_read, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        turso_read,
        "load_configured_turso_sources_from_env",
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

    monkeypatch.setattr(turso_read, "_load_single_post_for_tree_cached", _fake_cached)

    posts, err = turso_read.load_single_post_for_tree_from_env(requested_uid)

    assert err == ""
    assert not posts.empty
    assert calls == [("xueqiu", requested_uid)]
