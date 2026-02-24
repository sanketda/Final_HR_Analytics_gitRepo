import re
import pandas as pd
from typing import Dict, List, Any
import json
from save_issues import save_issues

def check_custom_patterns(
    df: pd.DataFrame,
    custom_patterns: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    For each (column, regex) in custom_patterns, scan df[column] and
    flag any non-null value that does NOT full-match the regex.
    """
    issues: List[Dict[str, Any]] = []
    for col, pattern in custom_patterns.items():
        if col not in df.columns:
            continue
        regex = re.compile(pattern)
        for idx, val in df[col].items():
            if pd.isnull(val):
                continue
            val_str = str(val)
            # full-match required; if it fails, record an issue
            if not regex.fullmatch(val_str):
                issues.append({
                    "row_index": int(idx),
                    "column": col,
                    "issue": "pattern_mismatch",
                    "value": val
                })
    return issues

def get_validation_rules(state: dict) -> dict:
    with open("E:/Academics/TY/Shyena Tech Yarns/backend/Data Quality/columns_metadata.json", "r") as file:
        metadata = json.load(file) #take this columns json from postgres table using table_id

    expected_schema = {col["name"]: col["type_name"] for col in metadata}
    expected_columns = [col["name"] for col in metadata]
    nullable_map = {col["name"]: col["nullable"] for col in metadata}

    state["expected_schema"] = expected_schema
    state["expected_columns"] = expected_columns
    state["nullable_map"] = nullable_map
    return state

def generate_quality_metrics(df: pd.DataFrame, issues: List[Dict[str, Any]]) -> dict:
    metrics = {}
    total_rows = len(df)

    # Organize issues by column
    issues_by_col = {}
    for issue in issues:
        issues_by_col.setdefault(issue["column"], set()).add(issue["row_index"])

    for col in df.columns:
        non_null = df[col].notna().sum()
        unique = df[col].nunique(dropna=True)
        invalid_rows = len(issues_by_col.get(col, []))

        metrics[col] = {
            "completeness_%": round((non_null / total_rows) * 100, 2) if total_rows else 0,
            "uniqueness_%": round((unique / total_rows) * 100, 2) if total_rows else 0,
            "validity_%": round(((total_rows - invalid_rows) / total_rows) * 100, 2) if total_rows else 0
        }
    return metrics


def check_corruption(df: pd.DataFrame) -> List[Dict[str, Any]]:
    issues = []
    for col in df.columns:
        for idx, val in df[col].items():
            if pd.isnull(val):
                continue
            val_str = str(val)

            # Non-ASCII characters
            if re.search(r'[^\x20-\x7E]', val_str):
                issues.append({
                    "row_index": int(idx),
                    "column": col,
                    "issue": "corrupted_non_ascii",
                    "value": val
                })


            special_ratio = sum(c in "!@#$%^&*()_+=[]{};:\"'<>,.?/~`" for c in val_str) / max(len(val_str), 1)
            if special_ratio > 0.4:
                issues.append({
                    "row_index": int(idx),
                    "column": col,
                    "issue": "corrupted_special_chars",
                    "value": val
                })
    return issues


def validate_uc_type(value, expected_uc_type: str) -> bool:
    if pd.isnull(value):
        return True 

    expected_uc_type = expected_uc_type.lower()

    try:
        if expected_uc_type in {"int", "integer", "tinyint", "smallint", "bigint"}:
            int(value)  # try converting
            return True
        elif expected_uc_type in {"float", "real", "double", "decimal"}:
            float(value)  # try converting
            return True
        elif expected_uc_type == "boolean":
            return str(value).lower() in {"true", "false", "0", "1"}
        elif expected_uc_type in {"date", "timestamp", "datetime"}:
            pd.to_datetime(value)  # try parsing
            return True
        elif expected_uc_type in {"string", "char", "varchar"}:
            return True  # everything is representable as string
        elif expected_uc_type == "binary":
            return isinstance(value, (bytes, bytearray))
        elif expected_uc_type.startswith("array"):
            return isinstance(value, list)
        elif expected_uc_type.startswith("map"):
            return isinstance(value, dict)
        elif expected_uc_type.startswith("struct"):
            return isinstance(value, dict)
        else:
            return True  # fallback
    except Exception:
        return False



def check_dtype_mismatch(df: pd.DataFrame, expected_schema: Dict[str, str]) -> List[Dict[str, Any]]:
    issues = []

    for col, expected_dtype in expected_schema.items():
        if col not in df.columns:
            continue

        for idx, val in df[col].items():
            if not validate_uc_type(val, expected_dtype):
                issues.append({
                    "row_index": int(idx),
                    "column": col,
                    "issue": "dtype_mismatch",
                    "value": val
                })

    return issues






def check_missing_columns(df: pd.DataFrame, expected_columns: List[str]) -> List[Dict[str, Any]]:
    issues = []
    
    for col in expected_columns:
        if col not in df.columns:

            issues.append({
                "row_index": None,       
                "column": col,            
                "issue": "missing_column",
                "value": None             
            })
    
    return issues



def check_unexpected_columns(df: pd.DataFrame, expected_columns: List[str]) -> List[Dict[str, Any]]:
    issues = []
    
    for col in df.columns:
        if col not in expected_columns:

            issues.append({
                "row_index": None,       
                "column": col,            
                "issue": "missing_column",
                "value": None             
            })
    
    return issues


def check_corruption(df: pd.DataFrame) -> List[Dict[str, Any]]:
    issues = []
    for col in df.columns:
        for idx, val in df[col].items():
            if pd.isnull(val):
                continue
            val_str = str(val)

            # Non-ASCII characters
            if re.search(r'[^\x20-\x7E]', val_str):
                issues.append({
                    "row_index": int(idx),
                    "column": col,
                    "issue": "corrupted_non_ascii",
                    "value": val
                })

            special_ratio = sum(c in "!@#$%^&*()_+=[]{};:\"'<>,.?/~`" for c in val_str) / max(len(val_str), 1)
            if special_ratio > 0.4:
                issues.append({
                    "row_index": int(idx),
                    "column": col,
                    "issue": "corrupted_special_chars",
                    "value": val
                })
    return issues



def check_nulls(df: pd.DataFrame, nullable_map: Dict[str, bool]) -> List[Dict[str, Any]]:
    issues = []
    null_like_values = {"null", "NULL", "None", "NONE", "nan", "NaN", "NAN"} 

    for col in df.columns:
        if not nullable_map.get(col, True): 
            for idx, val in df[col].items():

                if pd.isnull(val):
                    issues.append({
                        "row_index": int(idx),
                        "column": col,
                        "issue": "null_violation",
                        "value": None
                    })
                # Case 2: string representations of null
                elif isinstance(val, str) and val.strip() in null_like_values:
                    issues.append({
                        "row_index": int(idx),
                        "column": col,
                        "issue": "null_violation",
                        "value": val
                    })
    return issues



def generate_quality_metrics(df: pd.DataFrame, issues: List[Dict[str, Any]]) -> dict:
    metrics = {}
    total_rows = len(df)

    # Organize issues by column
    issues_by_col = {}
    for issue in issues:
        issues_by_col.setdefault(issue["column"], set()).add(issue["row_index"])

    for col in df.columns:
        non_null = df[col].notna().sum()
        unique = df[col].nunique(dropna=True)
        invalid_rows = len(issues_by_col.get(col, []))

        metrics[col] = {
            "completeness_%": round((non_null / total_rows) * 100, 2) if total_rows else 0,
            "uniqueness_%": round((unique / total_rows) * 100, 2) if total_rows else 0,
            "validity_%": round(((total_rows - invalid_rows) / total_rows) * 100, 2) if total_rows else 0
        }
    return metrics


def generate_quality_report(state: dict, report_path: str = "quality_report.json"):
    df = state.get("df", pd.DataFrame())
    issues = state.get("issues", [])

    report = {
        "total_rows": len(df),
        "total_issues": len(issues),
        "issues_by_column": {},
        "quality_metrics": generate_quality_metrics(df, issues)
    }

    for issue in issues:
        col = issue["column"]
        issue_type = issue["issue"]
        report["issues_by_column"].setdefault(col, {})
        report["issues_by_column"][col][issue_type] = report["issues_by_column"][col].get(issue_type, 0) + 1

    with open(report_path, "w") as f:
        json.dump(report, f, indent=4)

    print(f"📊 Quality report saved to {report_path}")
    return report


def validation_agent(state: dict) -> dict:
    df = state.get("df")
    if df is None:
        state["issues"] = []
        return state

    state = get_validation_rules(state)
    expected_schema = state["expected_schema"]
    expected_columns = state["expected_columns"]
    nullable_map     = state["nullable_map"]

    custom_patterns  = state.get("custom_patterns", {})

    missing_columns    = check_missing_columns(df, expected_columns)
    unexpected_columns = check_unexpected_columns(df, expected_columns)
    dtype_issues       = check_dtype_mismatch(df, expected_schema)
    null_issues        = check_nulls(df, nullable_map)
    corruption_issues  = check_corruption(df)
    custom_issues      = check_custom_patterns(df, custom_patterns)

    state["issues"] = (
        missing_columns
        + unexpected_columns
        + dtype_issues
        + null_issues
        + corruption_issues
        + custom_issues
    )

    with open("issues.json", "w") as f:
        json.dump(state["issues"], f, indent=4)
    print("Issues saved to issues.json")

    state["quality_report"] = generate_quality_report(state)
    return state


if __name__ == "__main__":
    state = {
    }
    state["custom_patterns"] = {
    "date_time": r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]) (?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$"
}
    state["df"] = pd.read_csv("orders_100_balanced.csv")
    state["table_id"] = "338c21da-10c1-459f-85bf-9251c30dd9b8"

    
    state = validation_agent(state)

    print(state["issues"])
    
    # save_issues(state)


    
