from __future__ import annotations

from functools import lru_cache


from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import (
    infer_platform_from_post_uid,
)
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.env import load_dotenv_if_present
from alphavault_reflex.services.source_loader import (
    DEFAULT_FATAL_EXCEPTIONS,
    MISSING_TURSO_SOURCES_ERROR,
    load_configured_source_schemas_from_env,
    source_table,
)


@lru_cache(maxsize=32)
def load_post_urls_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    post_uids: tuple[str, ...],
) -> dict[str, str]:
    if not post_uids:
        return {}
    del auth_token
    schema_name = str(source_name or "").strip()
    engine = ensure_postgres_engine(db_url, schema_name=schema_name)
    placeholders = ", ".join(["?"] * len(post_uids))
    sql = (
        f"SELECT post_uid, url FROM {source_table(schema_name, 'posts')} "
        f"WHERE post_uid IN ({placeholders})"
    )
    with postgres_connect_autocommit(engine) as conn:
        df = turso_read_sql_df(conn, sql, params=list(post_uids))
    if df.empty or "post_uid" not in df.columns or "url" not in df.columns:
        return {}
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        uid = str(row.get("post_uid") or "").strip()
        url = str(row.get("url") or "").strip()
        if uid and url:
            out[uid] = url
    return out


def load_post_urls_from_env(
    post_uids: list[str],
    *,
    load_cached_fn=load_post_urls_cached,
) -> tuple[dict[str, str], str]:
    cleaned = [str(uid or "").strip() for uid in (post_uids or [])]
    uids = tuple(sorted({uid for uid in cleaned if uid}))
    if not uids:
        return {}, ""

    load_dotenv_if_present()
    sources = load_configured_source_schemas_from_env()
    if not sources:
        return {}, MISSING_TURSO_SOURCES_ERROR

    sources_by_name = {s.name: s for s in sources}
    groups: dict[str, list[str]] = {}
    unknown: list[str] = []
    for uid in uids:
        platform = infer_platform_from_post_uid(uid)
        if platform and platform in sources_by_name:
            groups.setdefault(platform, []).append(uid)
        else:
            unknown.append(uid)

    out: dict[str, str] = {}
    try:
        for platform, group_uids in groups.items():
            source = sources_by_name[platform]
            out.update(
                load_cached_fn(
                    source.url,
                    source.token,
                    platform,
                    tuple(group_uids),
                )
            )
        if unknown:
            for source in sources:
                out.update(
                    load_cached_fn(
                        source.url,
                        source.token,
                        str(getattr(source, "name", "") or "").strip(),
                        tuple(unknown),
                    )
                )
    except BaseException as err:
        if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
            raise
        return {}, f"turso_connect_error:{type(err).__name__}"

    return out, ""


__all__ = ["load_post_urls_cached", "load_post_urls_from_env"]
