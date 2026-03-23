from __future__ import annotations

WANTED_ASSERTION_COLUMNS = [
    "post_uid",
    "idx",
    "topic_key",
    "action",
    "action_strength",
    "summary",
    "evidence",
    "confidence",
    "stock_codes_json",
    "stock_names_json",
    "industries_json",
    "commodities_json",
    "indices_json",
    "author",
    "created_at",
]


def build_processed_posts_query(display_expr: str) -> str:
    return f"""
SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
       {display_expr},
       final_status AS status, invest_score, processed_at, model, prompt_version
FROM posts
WHERE processed_at IS NOT NULL
"""


def build_assertions_query(selected_columns: list[str]) -> str:
    if selected_columns:
        return f"SELECT {', '.join(selected_columns)} FROM assertions"
    return "SELECT * FROM assertions"
