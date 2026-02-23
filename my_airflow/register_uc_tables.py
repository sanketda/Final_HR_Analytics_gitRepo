#!/usr/bin/env python3
"""
Register all Unity Catalog tables via REST API.
UC v0.2.1 requires `type_json` field in every column definition.
server.managed-table.enabled=false => uses EXTERNAL tables with storage_location.
"""
import json, subprocess, sys

UC_URL = "http://localhost:8081/api/2.1/unity-catalog"
CATALOG = "hr_analytics"
DELTA_BASE = "file:///home/mount_disk/Eclassifier-workspace/Data-engineer-airflow/hr_analytics_dev/delta_lake/hr_analytics"

def make_col(name, type_str, position, nullable=True):
    """Build a UC-compatible column definition with type_json."""
    return {
        "name": name,
        "type_text": type_str,
        "type_name": type_str.upper().split("(")[0],  # e.g. DECIMAL(4,1) -> DECIMAL
        "type_json": json.dumps({"name": name, "type": type_str, "nullable": nullable, "metadata": {}}),
        "position": position,
        "nullable": nullable
    }

def register_table(schema, table, columns):
    location = f"{DELTA_BASE}/{schema}/{table}"
    print(f"\n>>> Registering: {CATALOG}.{schema}.{table}")

    payload = {
        "name": table,
        "catalog_name": CATALOG,
        "schema_name": schema,
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "storage_location": location,
        "columns": columns
    }

    result = subprocess.run(
        ["curl", "-s", "-X", "POST", f"{UC_URL}/tables",
         "-H", "Content-Type: application/json",
         "-d", json.dumps(payload)],
        capture_output=True, text=True
    )
    resp = json.loads(result.stdout)
    if "table_id" in resp:
        print(f"    ✅ Created → table_id: {resp['table_id']}")
    elif "already exists" in resp.get("message", ""):
        print(f"    ⚠️  Already exists (skipped)")
    else:
        print(f"    ❌ Error: {resp.get('message', resp)}")

def verify():
    print("\n" + "="*50)
    print("VERIFICATION — Tables in each schema:")
    print("="*50)
    for schema in ["bronze", "silver", "gold"]:
        result = subprocess.run(
            ["curl", "-s", f"{UC_URL}/tables?catalog_name={CATALOG}&schema_name={schema}"],
            capture_output=True, text=True
        )
        data = json.loads(result.stdout)
        tables = data.get("tables", [])
        print(f"\n  --- hr_analytics.{schema} ({len(tables)} tables) ---")
        for t in tables:
            print(f"    ✅ {t['name']} | {t.get('data_source_format')} | {t.get('table_type')} | {t.get('storage_location','')}")
        if not tables:
            print("    (no tables)")

# ================================================================
# TABLE DEFINITIONS
# ================================================================

# --- BRONZE ---
register_table("bronze", "attendance_raw", [
    make_col("_airbyte_ab_id",   "string",    0),
    make_col("_airbyte_data",    "string",    1),
    make_col("ingest_timestamp", "timestamp", 2),
])

register_table("bronze", "leave_raw", [
    make_col("_airbyte_ab_id",   "string",    0),
    make_col("_airbyte_data",    "string",    1),
    make_col("ingest_timestamp", "timestamp", 2),
])

register_table("bronze", "timesheet_raw", [
    make_col("_airbyte_ab_id",   "string",    0),
    make_col("_airbyte_data",    "string",    1),
    make_col("ingest_timestamp", "timestamp", 2),
])

register_table("bronze", "employee_raw", [
    make_col("_airbyte_ab_id",   "string",    0),
    make_col("_airbyte_data",    "string",    1),
    make_col("ingest_timestamp", "timestamp", 2),
])

register_table("bronze", "biometric_raw", [
    make_col("_airbyte_ab_id",   "string",    0),
    make_col("_airbyte_data",    "string",    1),
    make_col("ingest_timestamp", "timestamp", 2),
])

# --- SILVER ---
register_table("silver", "attendance_cleaned", [
    make_col("source_attendance_id", "string",    0),
    make_col("work_date",            "date",      1),
    make_col("work_date_str",        "string",    2),
    make_col("employee_id",          "string",    3),
    make_col("email",                "string",    4),
    make_col("ercno",                "long",      5),
    make_col("shift_name",           "string",    6),
    make_col("zoho_status",          "string",    7),
    make_col("deviation_time",       "string",    8),
    make_col("first_in_ts",          "timestamp", 9),
    make_col("last_out_ts",          "timestamp", 10),
    make_col("work_minutes",         "int",       11),
    make_col("total_minutes",        "int",       12),
])

register_table("silver", "leave_cleaned", [
    make_col("employee_id",       "string",       0),
    make_col("source_leave_id",   "long",         1),
    make_col("zoho_record_id",    "string",       2),
    make_col("leave_type",        "string",       3),
    make_col("from_date",         "date",         4),
    make_col("to_date",           "date",         5),
    make_col("date_of_request",   "date",         6),
    make_col("approval_status",   "string",       7),
    make_col("leave_unit",        "string",       8),
    make_col("leave_count",       "decimal(4,1)", 9),
])

register_table("silver", "timesheet_cleaned", [
    make_col("employee_id",    "string", 0),
    make_col("work_date",      "date",   1),
    make_col("source_log_id",  "int",    2),
    make_col("job_name",       "string", 3),
    make_col("project_name",   "string", 4),
    make_col("task_name",      "string", 5),
    make_col("hours_str",      "string", 6),
    make_col("minutes_int",    "int",    7),
    make_col("billing_status", "string", 8),
])

register_table("silver", "employee_cleaned", [
    make_col("employee_id",    "string", 0),
    make_col("first_name",     "string", 1),
    make_col("last_name",      "string", 2),
    make_col("full_name",      "string", 3),
    make_col("email_id",       "string", 4),
    make_col("department",     "string", 5),
    make_col("designation",    "string", 6),
    make_col("date_of_joining","date",   7),
    make_col("employee_status","string", 8),
    make_col("record_hash",    "string", 9),
])

register_table("silver", "biometric_cleaned", [
    make_col("source_bio_id",     "string", 0),
    make_col("employee_id",       "string", 1),
    make_col("work_date",         "date",   2),
    make_col("bio_status",        "string", 3),
    make_col("work_minutes",      "int",    4),
    make_col("total_minutes",     "int",    5),
    make_col("ot_minutes",        "int",    6),
])

# --- GOLD ---
register_table("gold", "dim_employee", [
    make_col("dim_employee_key",   "int",       0, False),
    make_col("employee_id",        "string",    1),
    make_col("zoho_id",            "string",    2),
    make_col("first_name",         "string",    3),
    make_col("last_name",          "string",    4),
    make_col("full_name",          "string",    5),
    make_col("email_id",           "string",    6),
    make_col("other_email",        "string",    7),
    make_col("mobile",             "string",    8),
    make_col("department",         "string",    9),
    make_col("designation",        "string",    10),
    make_col("reporting_to",       "string",    11),
    make_col("employee_type",      "string",    12),
    make_col("employee_status",    "string",    13),
    make_col("date_of_joining",    "date",      14),
    make_col("date_of_exit",       "date",      15),
    make_col("location_name",      "string",    16),
    make_col("source_of_hire",     "string",    17),
    make_col("is_billable",        "int",       18),
    make_col("record_hash",        "string",    19),
    make_col("valid_from",         "timestamp", 20),
    make_col("valid_to",           "timestamp", 21),
    make_col("is_current",         "int",       22),
    make_col("dw_load_timestamp",  "timestamp", 23),
])

register_table("gold", "fact_attendance", [
    make_col("dim_employee_key",      "int",       0),
    make_col("dim_date_key",          "int",       1),
    make_col("source_attendance_id",  "string",    2),
    make_col("work_date_str",         "string",    3),
    make_col("deviation_time",        "string",    4),
    make_col("first_in_building",     "string",    5),
    make_col("first_in_location",     "string",    6),
    make_col("last_out_building",     "string",    7),
    make_col("last_out_location",     "string",    8),
    make_col("shift_end_time_str",    "string",    9),
    make_col("shift_name",            "string",    10),
    make_col("zoho_status",           "string",    11),
    make_col("zoho_total_hours_str",  "string",    12),
    make_col("zoho_working_hour_str", "string",    13),
    make_col("ercno",                 "long",      14),
    make_col("shifttime_str",         "string",    15),
    make_col("first_in_ts",           "timestamp", 16),
    make_col("last_out_ts",           "timestamp", 17),
    make_col("work_minutes",          "int",       18),
    make_col("total_minutes",         "int",       19),
    make_col("dw_load_timestamp",     "timestamp", 20),
])

register_table("gold", "fact_attendance_biometric", [
    make_col("dim_employee_key",   "int",       0),
    make_col("dim_date_key",       "int",       1),
    make_col("source_bio_id",      "string",    2),
    make_col("name_in_bio",        "string",    3),
    make_col("shift_in_bio",       "string",    4),
    make_col("bio_in_time_str",    "string",    5),
    make_col("bio_out_time_str",   "string",    6),
    make_col("bio_work_dur_str",   "string",    7),
    make_col("ot_str",             "string",    8),
    make_col("bio_total_dur_str",  "string",    9),
    make_col("bio_status",         "string",    10),
    make_col("remarks",            "string",    11),
    make_col("work_date",          "date",      12),
    make_col("work_minutes",       "int",       13),
    make_col("total_minutes",      "int",       14),
    make_col("ot_minutes",         "int",       15),
    make_col("dw_load_timestamp",  "timestamp", 16),
])

register_table("gold", "fact_leave", [
    make_col("dim_employee_key",   "int",          0),
    make_col("dim_date_key",       "int",          1),
    make_col("source_leave_id",    "long",         2),
    make_col("zoho_record_id",     "string",       3),
    make_col("approval_status",    "string",       4),
    make_col("date_of_request",    "date",         5),
    make_col("from_date",          "date",         6),
    make_col("to_date",            "date",         7),
    make_col("leave_type",         "string",       8),
    make_col("team_email_id",      "string",       9),
    make_col("leave_unit",         "string",       10),
    make_col("zuidin_leave",       "long",         11),
    make_col("reason",             "string",       12),
    make_col("leave_start_time",   "string",       13),
    make_col("leave_end_time",     "string",       14),
    make_col("leave_count",        "decimal(4,1)", 15),
    make_col("dw_load_timestamp",  "timestamp",    16),
])

register_table("gold", "fact_timesheet", [
    make_col("dim_employee_key",     "int",       0),
    make_col("dim_date_key",         "int",       1),
    make_col("source_log_id",        "int",       2),
    make_col("log_approval_status",  "string",    3),
    make_col("billing_status",       "string",    4),
    make_col("billed_status",        "string",    5),
    make_col("job_name",             "string",    6),
    make_col("project_name",         "string",    7),
    make_col("task_name",            "string",    8),
    make_col("timelog_id",           "string",    9),
    make_col("hours_str",            "string",    10),
    make_col("minutes_int",          "int",       11),
    make_col("from_time_str",        "string",    12),
    make_col("to_time_str",          "string",    13),
    make_col("work_date",            "date",      14),
    make_col("dw_load_timestamp",    "timestamp", 15),
])

verify()
print("\nDone! ✅")
