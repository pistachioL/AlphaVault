from __future__ import annotations

from alphavault.db.sql.common import pragma_table_info
from alphavault.db.sql.turso_db import (
    CREATE_ASSERTION_MENTIONS_TABLE,
    CREATE_ASSERTION_ENTITIES_TABLE,
    CLOUD_SCHEMA_INDEX_STATEMENTS,
    CREATE_ASSERTIONS_TABLE,
    CREATE_POSTS_TABLE,
    copy_topic_cluster_topics,
    create_topic_cluster_post_overrides_table,
    create_topic_cluster_topics_table,
    create_topic_cluster_topics_v2_table,
    create_topic_clusters_table,
    drop_table,
    drop_table_if_exists,
    rename_table,
    select_count_as_n,
    topic_cluster_index_statements,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
)

TOPIC_CLUSTERS_TABLE = "topic_clusters"
TOPIC_CLUSTER_TOPICS_TABLE = "topic_cluster_topics"
TOPIC_CLUSTER_POST_OVERRIDES_TABLE = "topic_cluster_post_overrides"
TOPIC_CLUSTER_TOPICS_V2_TABLE = f"{TOPIC_CLUSTER_TOPICS_TABLE}_v2"


def _topic_cluster_topics_pk_cols(conn: TursoConnection) -> list[str]:
    try:
        rows = (
            conn.execute(pragma_table_info(TOPIC_CLUSTER_TOPICS_TABLE)).mappings().all()
        )
    except Exception:
        return []
    if not rows:
        return []
    ordered = sorted(rows, key=lambda r: int(r.get("pk") or 0))
    cols = [
        str(r.get("name") or "").strip() for r in ordered if int(r.get("pk") or 0) > 0
    ]
    return [c for c in cols if c]


def _migrate_topic_cluster_topics_to_v2(conn: TursoConnection) -> None:
    conn.execute(drop_table_if_exists(TOPIC_CLUSTER_TOPICS_V2_TABLE))
    conn.execute(create_topic_cluster_topics_v2_table(TOPIC_CLUSTER_TOPICS_V2_TABLE))
    conn.execute(
        copy_topic_cluster_topics(
            src_table=TOPIC_CLUSTER_TOPICS_TABLE,
            dst_table=TOPIC_CLUSTER_TOPICS_V2_TABLE,
        )
    )

    old_n = int(
        (
            conn.execute(select_count_as_n(TOPIC_CLUSTER_TOPICS_TABLE))
            .mappings()
            .first()
            or {}
        ).get("n")
        or 0
    )
    new_n = int(
        (
            conn.execute(select_count_as_n(TOPIC_CLUSTER_TOPICS_V2_TABLE))
            .mappings()
            .first()
            or {}
        ).get("n")
        or 0
    )
    if new_n < old_n:
        raise RuntimeError(
            f"topic_cluster_topics migrate failed: copied_rows={new_n} < old_rows={old_n}"
        )

    conn.execute(drop_table(TOPIC_CLUSTER_TOPICS_TABLE))
    conn.execute(
        rename_table(TOPIC_CLUSTER_TOPICS_V2_TABLE, TOPIC_CLUSTER_TOPICS_TABLE)
    )


def init_topic_cluster_schema(engine: TursoEngine) -> None:
    with turso_connect_autocommit(engine) as conn:
        conn.execute(create_topic_clusters_table(TOPIC_CLUSTERS_TABLE))
        conn.execute(create_topic_cluster_topics_table(TOPIC_CLUSTER_TOPICS_TABLE))
        conn.execute(
            create_topic_cluster_post_overrides_table(
                TOPIC_CLUSTER_POST_OVERRIDES_TABLE
            )
        )

        pk_cols = _topic_cluster_topics_pk_cols(conn)
        if pk_cols == ["topic_key"]:
            _migrate_topic_cluster_topics_to_v2(conn)

        for stmt in topic_cluster_index_statements(
            TOPIC_CLUSTER_TOPICS_TABLE, TOPIC_CLUSTER_POST_OVERRIDES_TABLE
        ):
            conn.execute(stmt)


def init_cloud_schema(engine: TursoEngine) -> None:
    with turso_connect_autocommit(engine) as conn:
        conn.execute(CREATE_POSTS_TABLE)
        conn.execute(CREATE_ASSERTIONS_TABLE)
        conn.execute(CREATE_ASSERTION_MENTIONS_TABLE)
        conn.execute(CREATE_ASSERTION_ENTITIES_TABLE)
        for stmt in CLOUD_SCHEMA_INDEX_STATEMENTS:
            conn.execute(stmt)

    init_topic_cluster_schema(engine)
