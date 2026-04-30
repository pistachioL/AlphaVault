from __future__ import annotations

from collections.abc import Iterable

from alphavault.db.postgres_db import PostgresEngine, ensure_postgres_engine
from alphavault.db.postgres_env import (
    infer_platform_from_post_uid,
    require_postgres_source_from_env,
)
from alphavault.db.source_queue import load_cloud_post
from alphavault.env import load_dotenv_if_present


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def load_sample_post_raw_text_map(post_uids: Iterable[str]) -> dict[str, str]:
    unique_post_uids = [
        post_uid
        for post_uid in dict.fromkeys(_clean_text(item) for item in post_uids)
        if post_uid
    ]
    if not unique_post_uids:
        return {}

    load_dotenv_if_present()
    engines_by_platform: dict[str, PostgresEngine] = {}
    out: dict[str, str] = {}
    for post_uid in unique_post_uids:
        platform = _clean_text(infer_platform_from_post_uid(post_uid))
        if not platform:
            continue
        engine = engines_by_platform.get(platform)
        if engine is None:
            source = require_postgres_source_from_env(platform)
            engine = ensure_postgres_engine(source.dsn, schema_name=source.schema)
            engines_by_platform[platform] = engine
        try:
            post = load_cloud_post(engine, post_uid)
        except Exception:
            continue
        raw_text = _clean_text(post.raw_text)
        if raw_text:
            out[post_uid] = raw_text
    return out


__all__ = ["load_sample_post_raw_text_map"]
