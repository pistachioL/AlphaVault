from __future__ import annotations

from alphavault.db.analysis_feedback import (
    ENTRYPOINT_HOMEWORK_TREE,
    ENTRYPOINT_STOCK_RESEARCH,
    submit_post_analysis_feedback as _submit_post_analysis_feedback,
)

ANALYSIS_FEEDBACK_DIALOG_TITLE = "标错并重跑"
ANALYSIS_FEEDBACK_TAG_LABEL = "错在哪"
ANALYSIS_FEEDBACK_NOTE_LABEL = "补一句哪里错了"
ANALYSIS_FEEDBACK_TAG_PLACEHOLDER = "选一个标签"
ANALYSIS_FEEDBACK_NOTE_PLACEHOLDER = "比如：原文是先看看，不是直接买入。"
ANALYSIS_FEEDBACK_SUBMIT_TEXT = "提交"
ANALYSIS_FEEDBACK_CANCEL_TEXT = "取消"
ANALYSIS_FEEDBACK_SUCCESS_TEXT = "已记下，后台会在下一轮自动重跑。"
ANALYSIS_FEEDBACK_FAILED_TEXT = "提交失败，请稍后再试。"
ANALYSIS_FEEDBACK_QUEUE_FAILED_TEXT = "已记下纠错，但加入待重跑失败，请稍后再试。"
ANALYSIS_FEEDBACK_MISSING_POST_TEXT = "这条帖子没有可重跑的编号。"
ANALYSIS_FEEDBACK_MISSING_TAG_TEXT = "先选一下错在哪里。"
ANALYSIS_FEEDBACK_TAG_OPTIONS = [
    "标的错了",
    "动作错了",
    "摘要错了",
    "漏了重点",
    "其他",
]

_SUCCESS_QUEUE_STATUSES = frozenset(("pushed", "duplicate"))


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def submit_post_analysis_feedback(
    *,
    post_uid: str,
    feedback_tag: str,
    feedback_note: str,
    entrypoint: str,
) -> dict[str, str]:
    resolved_post_uid = _clean_text(post_uid)
    resolved_tag = _clean_text(feedback_tag)
    resolved_note = _clean_text(feedback_note)
    resolved_entrypoint = _clean_text(entrypoint)
    if not resolved_post_uid:
        return {"ok": "0", "message": ANALYSIS_FEEDBACK_MISSING_POST_TEXT}
    if not resolved_tag:
        return {"ok": "0", "message": ANALYSIS_FEEDBACK_MISSING_TAG_TEXT}
    try:
        result = _submit_post_analysis_feedback(
            post_uid=resolved_post_uid,
            feedback_tag=resolved_tag,
            feedback_note=resolved_note,
            entrypoint=resolved_entrypoint,
        )
    except Exception as err:
        error_text = _clean_text(err)
        return {
            "ok": "0",
            "message": error_text or ANALYSIS_FEEDBACK_FAILED_TEXT,
        }

    queue_status = _clean_text(result.get("queue_status"))
    is_ok = queue_status in _SUCCESS_QUEUE_STATUSES
    return {
        "ok": "1" if is_ok else "0",
        "message": (
            ANALYSIS_FEEDBACK_SUCCESS_TEXT
            if is_ok
            else ANALYSIS_FEEDBACK_QUEUE_FAILED_TEXT
        ),
        "feedback_id": _clean_text(result.get("feedback_id")),
        "feedback_status": _clean_text(result.get("feedback_status")),
        "queue_status": queue_status,
    }


__all__ = [
    "ANALYSIS_FEEDBACK_CANCEL_TEXT",
    "ANALYSIS_FEEDBACK_DIALOG_TITLE",
    "ANALYSIS_FEEDBACK_FAILED_TEXT",
    "ANALYSIS_FEEDBACK_MISSING_POST_TEXT",
    "ANALYSIS_FEEDBACK_MISSING_TAG_TEXT",
    "ANALYSIS_FEEDBACK_NOTE_LABEL",
    "ANALYSIS_FEEDBACK_NOTE_PLACEHOLDER",
    "ANALYSIS_FEEDBACK_QUEUE_FAILED_TEXT",
    "ANALYSIS_FEEDBACK_SUBMIT_TEXT",
    "ANALYSIS_FEEDBACK_SUCCESS_TEXT",
    "ANALYSIS_FEEDBACK_TAG_LABEL",
    "ANALYSIS_FEEDBACK_TAG_OPTIONS",
    "ANALYSIS_FEEDBACK_TAG_PLACEHOLDER",
    "ENTRYPOINT_HOMEWORK_TREE",
    "ENTRYPOINT_STOCK_RESEARCH",
    "submit_post_analysis_feedback",
]
