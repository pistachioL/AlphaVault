from __future__ import annotations


def _import_litellm():
    try:
        import litellm  # type: ignore
    except Exception as exc:
        raise RuntimeError("litellm_not_installed") from exc

    if hasattr(litellm, "suppress_debug_info"):
        # Prevent LiteLLM from printing "Give Feedback / Get Help" debug info on exceptions.
        litellm.suppress_debug_info = True

    return litellm


def _resolve_litellm_model_name(model_name: str, base_url: str) -> str:
    resolved_model_name = str(model_name or "").strip()
    if not resolved_model_name:
        return ""
    if "/" in resolved_model_name:
        return resolved_model_name
    if str(base_url or "").strip():
        return f"openai/{resolved_model_name}"
    return resolved_model_name

