from __future__ import annotations


def make_relation_id(
    *,
    relation_type: str,
    left_key: str,
    right_key: str,
    relation_label: str,
) -> str:
    return "|".join(
        [
            str(relation_type or "").strip(),
            str(relation_label or "").strip(),
            str(left_key or "").strip(),
            str(right_key or "").strip(),
        ]
    )


def make_candidate_id(
    *,
    relation_type: str,
    left_key: str,
    right_key: str,
    relation_label: str,
) -> str:
    return make_relation_id(
        relation_type=relation_type,
        left_key=left_key,
        right_key=right_key,
        relation_label=relation_label,
    )


__all__ = [
    "make_candidate_id",
    "make_relation_id",
]
