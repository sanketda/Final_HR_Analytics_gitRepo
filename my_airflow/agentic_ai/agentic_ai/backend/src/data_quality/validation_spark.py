# validation_spark.py

import os
import re
import json
from typing import Dict, List, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from database import get_full_name, get_metadata, update_last_dq_checked
from typing import List, Dict, Any, Tuple
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DataQualityValidation").getOrCreate()


PII_PATTERNS = {
    "email": (
        re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
        lambda v: re.sub(r'([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*(@.*)', r'\1****\2', v),
    ),
    "phone": (
        re.compile(r"(\+?\d{10,15})"),
        lambda v: re.sub(r'(\d{2})\d+(\d{2})', r'\1****\2', v),
    ),
    "credit_card": (
        re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
        lambda v: re.sub(r'\d(?=\d{4})', '*', v),
    ),
    "ssn": (
        re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),  # US SSN format
        lambda v: "***-**-" + v[-4:],
    ),
    "passport": (
        re.compile(r"\b[A-PR-WYa-pr-wy][1-9]\d{6,8}\b"),  # generic passport regex
        lambda v: v[0] + "****" + v[-2:],  # keep first + last 2
    ),
    "iban": (
        re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b"),  # IBAN format
        lambda v: v[:4] + "****" + v[-4:],
    ),
    "ipv4": (
        re.compile(r"\b\d{1,3}(\.\d{1,3}){3}\b"),
        lambda v: "***.***.***." + v.split(".")[-1],
    ),
    "ipv6": (
        re.compile(r"\b([0-9a-f]{1,4}:){7}[0-9a-f]{1,4}\b", re.I),
        lambda v: "****:****:****:****:" + v.split(":")[-1],
    ),
    "dob": (
        re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),  # YYYY-MM-DD
        lambda v: "****-**-**",
    ),
}


def mask_pii(value: str) -> str:
    if value is None:
        return value
    for pii_type, (pattern, masker) in PII_PATTERNS.items():
        if pattern.search(value):
            return masker(value)
    return value

def check_and_mask_pii(df):
    issues = []
    window = Window.orderBy(F.monotonically_increasing_id())
    df = df.withColumn("_row_index", F.row_number().over(window) - 1)

    mask_udf = F.udf(mask_pii, StringType())

    for col in df.columns:
        # Collect sample rows for reporting issues
        sample_rows = df.select("_row_index", col).where(F.col(col).isNotNull()).limit(5).collect()
        for row in sample_rows:
            masked_val = mask_pii(str(row[col]))
            if masked_val != str(row[col]):
                issues.append({
                    "column": col,
                    "issue": "pii_detected",
                    "row_index": row["_row_index"],
                    "value": row[col],
                    "masked_value": masked_val
                })

        # Apply masking to entire column
        df = df.withColumn(col, mask_udf(F.col(col).cast("string")))

    return issues, df.drop("_row_index")


def check_custom_patterns(
    df,
    custom_patterns: Dict[str, str]
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for column_name, pattern in custom_patterns.items():
        if column_name not in df.columns:
            continue
        regex = re.compile(pattern)
        # collect column values with their row-index
        for (val,), idx in df.select(column_name).rdd.zipWithIndex().collect():
            if val is None:
                continue
            val_str = str(val)
            if not regex.fullmatch(val_str):
                issues.append({
                    "row_index": idx,
                    "column": column_name,
                    "issue": "pattern_mismatch",
                    "value": val
                })
    return issues


# def get_validation_rules(state: dict) -> dict:
#     # with open("E:/Academics/TY/Shyena Tech Yarns/backend/Data Quality/columns_metadata.json", "r") as file:
#     #     metadata = json.load(file)

#     state["expected_schema"] = {col["name"]: col["type_name"] for col in metadata}
#     state["expected_columns"] = [col["name"] for col in metadata]
#     state["nullable_map"]    = {col["name"]: col["nullable"]  for col in metadata}
#     return state


def generate_quality_metrics(df, issues: List[Dict[str, Any]]) -> dict:
    metrics = {}
    total_rows = df.count()

    issues_by_col: Dict[str, set] = {}
    for issue in issues:
        issues_by_col.setdefault(issue["column"], set()).add(issue["row_index"])

    for column_name in df.columns:
        non_null = df.filter(col(column_name).isNotNull()).count()
        unique   = df.select(countDistinct(col(column_name))).collect()[0][0]
        invalid  = len(issues_by_col.get(column_name, set()))

        metrics[column_name] = {
            "completeness_%": round((non_null  / total_rows) * 100, 2) if total_rows else 0,
            "uniqueness_%":  round((unique    / total_rows) * 100, 2) if total_rows else 0,
            "validity_%":    round(((total_rows - invalid) / total_rows) * 100, 2) if total_rows else 0
        }
    return metrics


import re
from typing import List, Dict, Any

def check_corruption(df) -> List[Dict[str, Any]]:
    issues = []
    for column_name in df.columns:
        for idx, row in enumerate(df.select(column_name).toLocalIterator()):
            val = row[column_name]
            if val is None:
                continue

            val_str = str(val)

            # --- check non-ASCII characters
            if re.search(r"[^\x20-\x7E]", val_str):
                issues.append({
                    "row_index": idx,
                    "column": column_name,
                    "issue": "corrupted_non_ascii",
                    "value": val
                })

            # --- check too many special characters
            special_ratio = sum(c in "!@#$%^&*()_+=[]{};:\"'<>,.?/~`" for c in val_str) / max(len(val_str), 1)
            if special_ratio > 0.4:
                issues.append({
                    "row_index": idx,
                    "column": column_name,
                    "issue": "corrupted_special_chars",
                    "value": val
                })

    return issues



def validate_uc_type(value, expected_uc_type: str) -> bool:
    if value is None:
        return True
    expected_uc_type = expected_uc_type.lower()
    try:
        if expected_uc_type in {"int", "integer", "tinyint", "smallint", "bigint"}:
            int(value); return True
        elif expected_uc_type in {"float", "real", "double", "decimal"}:
            float(value); return True
        elif expected_uc_type == "boolean":
            return str(value).lower() in {"true", "false", "0", "1"}
        elif expected_uc_type in {"date", "timestamp", "datetime"}:
            # rely on Spark when parsing; just accept non-null here
            return True
        elif expected_uc_type in {"string", "char", "varchar"}:
            return True
        elif expected_uc_type == "binary":
            return isinstance(value, (bytes, bytearray))
        elif expected_uc_type.startswith("array"):
            return isinstance(value, list)
        elif expected_uc_type.startswith("map") or expected_uc_type.startswith("struct"):
            return isinstance(value, dict)
        else:
            return True
    except Exception:
        return False


from typing import Dict, List, Any

def check_dtype_mismatch(df, expected_schema: Dict[str, str]) -> List[Dict[str, Any]]:
    issues = []
    for column_name, exp_type in expected_schema.items():
        if column_name not in df.columns:
            continue

        for idx, row in enumerate(df.select(column_name).toLocalIterator()):
            val = row[column_name]
            if not validate_uc_type(val, exp_type):
                issues.append({
                    "row_index": idx,
                    "column": column_name,
                    "issue": "dtype_mismatch",
                    "value": val
                })

    return issues



def check_missing_columns(df, expected_columns: List[str]) -> List[Dict[str, Any]]:
    return [
        {
            "row_index": None,
            "column": col,
            "issue": "missing_column",
            "value": None
        }
        for col in expected_columns if col not in df.columns
    ]


def check_unexpected_columns(df, expected_columns: List[str]) -> List[Dict[str, Any]]:
    return [
        {
            "row_index": None,
            "column": col,
            "issue": "unexpected_column",
            "value": None
        }
        for col in df.columns if col not in expected_columns
    ]


def check_nulls(df, nullable_map: Dict[str, bool]) -> List[Dict[str, Any]]:
    issues = []
    null_like = {"null", "NULL", "None", "NONE", "nan", "NaN", "NAN"}
    for column_name in df.columns:
        if not nullable_map.get(column_name, True):
            for (val,), idx in df.select(column_name).rdd.zipWithIndex().collect():
                if val is None or (isinstance(val, str) and val.strip() in null_like):
                    issues.append({
                        "row_index": idx,
                        "column": column_name,
                        "issue": "null_violation",
                        "value": None if val is None else val
                    })
    return issues


def generate_quality_report(state: dict, report_path: str = "quality_report.json"):
    df     = state.get("df")
    issues = state.get("issues", [])

    report = {
        "total_rows":     df.count(),
        "total_issues":   len(issues),
        "issues_by_column": {},
        "quality_metrics": generate_quality_metrics(df, issues)
    }

    for issue in issues:
        coln = issue["column"]
        itype = issue["issue"]
        report["issues_by_column"].setdefault(coln, {})
        report["issues_by_column"][coln][itype] = report["issues_by_column"][coln].get(itype, 0) + 1

    with open(report_path, "w") as f:
        json.dump(report, f, indent=4)
    print(f"📊 Quality report saved to {report_path}")

    update_last_dq_checked(state["table_id"])

    return report


def loader(state: dict) -> dict:
    state["full_name"] = get_full_name(state["table_id"])
    state["columns_metadata_json"] = get_metadata(state["table_id"])

    state["df"] =  spark.read.table(state["full_name"]) 
    
    state["expected_schema"] = {col["name"]: col["type_name"] for col in state["columns_metadata_json"]}
    state["expected_columns"] = [col["name"] for col in state["columns_metadata_json"]]
    state["nullable_map"]   = {col["name"]: col["nullable"]  for col in state["columns_metadata_json"]}
    
    #  Extract PII columns from metadata
    state["pii_columns"] = [
        col["name"]
        for col in state["columns_metadata_json"]
        if col.get("pii", False)
    ]
    
    state["df"].show()
    return state



def validation_agent(state: dict) -> dict:
    all_issues = []

    for table_id in state["tables"]:
        state["table_id"] = table_id
        df = state.get("df")
        if df is None:
            print(f"⚠️ No DataFrame found for table {table_id}")
            continue

        # Run validations
        missing      = check_missing_columns(df, state["expected_columns"])
        unexpected   = check_unexpected_columns(df, state["expected_columns"])
        dtype_errs   = check_dtype_mismatch(df, state["expected_schema"])
        null_errs    = check_nulls(df, state["nullable_map"])
        corrupt_errs = check_corruption(df)

        pii_issues, df = check_and_mask_pii(df)

        # Combine
        table_issues = missing + unexpected + dtype_errs + null_errs + corrupt_errs + pii_issues
        print(f"🔍 Table {table_id} produced {len(table_issues)} issues")

        if table_issues:
            print("   Sample issues:", table_issues[:3])

        all_issues.extend(table_issues)

        state["df"] = df

    # Save once after loop
    state["issues"] = all_issues
    state["quality_report"] = generate_quality_report(state)

    if all_issues:
        output_path = os.path.abspath("issues.json")
        with open(output_path, "w") as f:
            json.dump(all_issues, f, indent=4)
        print(f"✅ Issues saved to {output_path} with {len(all_issues)} issues")
    else:
        print("⚠️ No issues found. Skipping write.")

    return state




# if __name__ == "__main__":
#     state = {}
#     state["custom_patterns"] = {
#         "date_time": r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]) (?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$"
#     }

#     state["table_id"] = "338c21da-10c1-459f-85bf-9251c30dd9b8"

#     state = validation_agent(state)
#     print(state["issues"])


if __name__ == "__main__":
    from pyspark.sql import Row

    # --- Create dummy dataset with errors + PII ---
    test_data = [
        Row(
            id=1,
            name="Alice",
            email="alice@example.com",
            phone="1234567890",
            credit_card="4111111111111111",
            ssn="123-45-6789",
            passport="X1234567",
            iban="GB82WEST12345698765432",
            ip="192.168.1.1",
            ipv6="2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            dob="1990-05-20"
        ),
        Row(
            id=2,
            name=None,  # Null to test completeness
            email="bob[at]example.com",  # invalid email
            phone="99999",  # invalid phone
            credit_card="123",  # invalid card
            ssn=None,
            passport=None,
            iban=None,
            ip=None,
            ipv6=None,
            dob=None
        ),
        Row(
            id=3,
            name="Alice",  # Duplicate name
            email="test@gmail.com",
            phone="+919876543210",
            credit_card="5500000000000004",
            ssn="987-65-4321",
            passport="K12345678",
            iban="DE89370400440532013000",
            ip="10.0.0.5",
            ipv6="fe80:0:0:0:200:f8ff:fe21:67cf",
            dob="1985-12-10"
        ),
        Row(
            id=4,
            name="Eve",
            email=None,
            phone=None,
            credit_card=None,
            ssn="111-22-3333",
            passport="M9876543",
            iban="FR1420041010050500013M02606",
            ip="172.16.254.1",
            ipv6="::1",  # loopback IPv6
            dob="2000-01-01"
        ),
    ]

    df_test = spark.createDataFrame(test_data)

    # Build test state
    state = {
        "tables": ["dummy_table"],
        "table_id": "dummy_table",
        "df": df_test,
        "expected_schema": {
            "id": "int",
            "name": "string",
            "email": "string",
            "phone": "string",
            "credit_card": "string",
            "ssn": "string",
            "passport": "string",
            "iban": "string",
            "ip": "string",
            "ipv6": "string",
            "dob": "string",
        },
        "expected_columns": [
            "id", "name", "email", "phone", "credit_card",
            "ssn", "passport", "iban", "ip", "ipv6", "dob"
        ],
        "nullable_map": {
            "id": False,
            "name": False,
            "email": True,
            "phone": True,
            "credit_card": True,
            "ssn": True,
            "passport": True,
            "iban": True,
            "ip": True,
            "ipv6": True,
            "dob": True,
        }
    }

    # Run validation agent
    state = validation_agent(state)

    print("\n=== Issues Detected ===")
    print(state["issues"])

    print("\n=== Masked DataFrame ===")
    state["df"].show(truncate=False)

    print("\n=== Quality Report ===")
    print(state["quality_report"])





