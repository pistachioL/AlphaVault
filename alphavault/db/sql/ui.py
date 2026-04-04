def build_assertions_query(selected_columns: list[str]) -> str:
    if selected_columns:
        return f"SELECT {', '.join(selected_columns)} FROM assertions"
    return "SELECT * FROM assertions"


__all__ = ["build_assertions_query"]
