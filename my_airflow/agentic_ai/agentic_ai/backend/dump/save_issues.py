import pandas as pd
from database import save_df_to_postgres

def save_issues(state) -> dict:
    issues = state.get("issues", [])
    table_id = state.get("table_id", "unknown_table")

    if not issues:
        return pd.DataFrame(columns=["table_id", "row_index", "column", "issue", "value"])

    df = pd.DataFrame(issues)
    df.insert(0, "table_id", table_id)


    return state
