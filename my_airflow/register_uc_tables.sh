#!/bin/bash
# ============================================================
# Register EXTERNAL tables in Unity Catalog via REST API
# UC is accessible at http://localhost:8081
# server.managed-table.enabled=false  => must use EXTERNAL tables with storage_location
# ============================================================

UC_URL="http://localhost:8081/api/2.1/unity-catalog"
CATALOG="hr_analytics"
DELTA_BASE="file:///home/mount_disk/Eclassifier-workspace/Data-engineer-airflow/hr_analytics_dev/delta_lake/hr_analytics"

echo "========================================"
echo "Unity Catalog Table Registration Script"
echo "========================================"

register_table() {
  local schema=$1
  local table=$2
  local columns_json=$3
  local storage_location="${DELTA_BASE}/${schema}/${table}"

  echo ""
  echo ">>> Registering: $CATALOG.$schema.$table"
  echo "    Location: $storage_location"

  PAYLOAD=$(cat <<EOF
{
  "name": "$table",
  "catalog_name": "$CATALOG",
  "schema_name": "$schema",
  "table_type": "EXTERNAL",
  "data_source_format": "DELTA",
  "storage_location": "$storage_location",
  "columns": $columns_json
}
EOF
)

  RESPONSE=$(curl -s -X POST "$UC_URL/tables" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD")

  if echo "$RESPONSE" | grep -q '"table_id"'; then
    echo "    ✅ Created successfully"
  elif echo "$RESPONSE" | grep -q 'already exists'; then
    echo "    ⚠️  Already exists (skipped)"
  else
    echo "    ❌ Error: $RESPONSE"
  fi
}

# ============================================================
# BRONZE LAYER
# ============================================================
register_table "bronze" "attendance_raw" '[
  {"name": "_airbyte_ab_id", "type_text": "string", "type_name": "STRING", "position": 0, "nullable": true},
  {"name": "_airbyte_data", "type_text": "string", "type_name": "STRING", "position": 1, "nullable": true},
  {"name": "ingest_timestamp", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 2, "nullable": true}
]'

register_table "bronze" "leave_raw" '[
  {"name": "_airbyte_ab_id", "type_text": "string", "type_name": "STRING", "position": 0, "nullable": true},
  {"name": "_airbyte_data", "type_text": "string", "type_name": "STRING", "position": 1, "nullable": true},
  {"name": "ingest_timestamp", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 2, "nullable": true}
]'

register_table "bronze" "timesheet_raw" '[
  {"name": "_airbyte_ab_id", "type_text": "string", "type_name": "STRING", "position": 0, "nullable": true},
  {"name": "_airbyte_data", "type_text": "string", "type_name": "STRING", "position": 1, "nullable": true},
  {"name": "ingest_timestamp", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 2, "nullable": true}
]'

register_table "bronze" "employee_raw" '[
  {"name": "_airbyte_ab_id", "type_text": "string", "type_name": "STRING", "position": 0, "nullable": true},
  {"name": "_airbyte_data", "type_text": "string", "type_name": "STRING", "position": 1, "nullable": true},
  {"name": "ingest_timestamp", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 2, "nullable": true}
]'

# ============================================================
# SILVER LAYER
# ============================================================
register_table "silver" "attendance_cleaned" '[
  {"name": "source_attendance_id", "type_text": "string", "type_name": "STRING", "position": 0, "nullable": true},
  {"name": "work_date", "type_text": "date", "type_name": "DATE", "position": 1, "nullable": true},
  {"name": "work_date_str", "type_text": "string", "type_name": "STRING", "position": 2, "nullable": true},
  {"name": "employee_id", "type_text": "string", "type_name": "STRING", "position": 3, "nullable": true},
  {"name": "email", "type_text": "string", "type_name": "STRING", "position": 4, "nullable": true},
  {"name": "ercno", "type_text": "long", "type_name": "LONG", "position": 5, "nullable": true},
  {"name": "shift_name", "type_text": "string", "type_name": "STRING", "position": 6, "nullable": true},
  {"name": "zoho_status", "type_text": "string", "type_name": "STRING", "position": 7, "nullable": true},
  {"name": "first_in_ts", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 8, "nullable": true},
  {"name": "last_out_ts", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 9, "nullable": true},
  {"name": "work_minutes", "type_text": "int", "type_name": "INT", "position": 10, "nullable": true},
  {"name": "total_minutes", "type_text": "int", "type_name": "INT", "position": 11, "nullable": true},
  {"name": "deviation_time", "type_text": "string", "type_name": "STRING", "position": 12, "nullable": true}
]'

register_table "silver" "leave_cleaned" '[
  {"name": "employee_id", "type_text": "string", "type_name": "STRING", "position": 0, "nullable": true},
  {"name": "source_leave_id", "type_text": "long", "type_name": "LONG", "position": 1, "nullable": true},
  {"name": "zoho_record_id", "type_text": "string", "type_name": "STRING", "position": 2, "nullable": true},
  {"name": "leave_type", "type_text": "string", "type_name": "STRING", "position": 3, "nullable": true},
  {"name": "from_date", "type_text": "date", "type_name": "DATE", "position": 4, "nullable": true},
  {"name": "to_date", "type_text": "date", "type_name": "DATE", "position": 5, "nullable": true},
  {"name": "date_of_request", "type_text": "date", "type_name": "DATE", "position": 6, "nullable": true},
  {"name": "approval_status", "type_text": "string", "type_name": "STRING", "position": 7, "nullable": true},
  {"name": "leave_unit", "type_text": "string", "type_name": "STRING", "position": 8, "nullable": true},
  {"name": "leave_count", "type_text": "decimal(4,1)", "type_name": "DECIMAL", "position": 9, "nullable": true}
]'

register_table "silver" "timesheet_cleaned" '[
  {"name": "employee_id", "type_text": "string", "type_name": "STRING", "position": 0, "nullable": true},
  {"name": "work_date", "type_text": "date", "type_name": "DATE", "position": 1, "nullable": true},
  {"name": "source_log_id", "type_text": "int", "type_name": "INT", "position": 2, "nullable": true},
  {"name": "job_name", "type_text": "string", "type_name": "STRING", "position": 3, "nullable": true},
  {"name": "project_name", "type_text": "string", "type_name": "STRING", "position": 4, "nullable": true},
  {"name": "task_name", "type_text": "string", "type_name": "STRING", "position": 5, "nullable": true},
  {"name": "hours_str", "type_text": "string", "type_name": "STRING", "position": 6, "nullable": true},
  {"name": "minutes_int", "type_text": "int", "type_name": "INT", "position": 7, "nullable": true},
  {"name": "billing_status", "type_text": "string", "type_name": "STRING", "position": 8, "nullable": true}
]'

register_table "silver" "employee_cleaned" '[
  {"name": "employee_id", "type_text": "string", "type_name": "STRING", "position": 0, "nullable": true},
  {"name": "first_name", "type_text": "string", "type_name": "STRING", "position": 1, "nullable": true},
  {"name": "last_name", "type_text": "string", "type_name": "STRING", "position": 2, "nullable": true},
  {"name": "email_id", "type_text": "string", "type_name": "STRING", "position": 3, "nullable": true},
  {"name": "department", "type_text": "string", "type_name": "STRING", "position": 4, "nullable": true},
  {"name": "designation", "type_text": "string", "type_name": "STRING", "position": 5, "nullable": true},
  {"name": "date_of_joining", "type_text": "date", "type_name": "DATE", "position": 6, "nullable": true}
]'

# ============================================================
# GOLD LAYER
# ============================================================
register_table "gold" "dim_employee" '[
  {"name": "dim_employee_key", "type_text": "int", "type_name": "INT", "position": 0, "nullable": false},
  {"name": "employee_id", "type_text": "string", "type_name": "STRING", "position": 1, "nullable": true},
  {"name": "zoho_id", "type_text": "string", "type_name": "STRING", "position": 2, "nullable": true},
  {"name": "first_name", "type_text": "string", "type_name": "STRING", "position": 3, "nullable": true},
  {"name": "last_name", "type_text": "string", "type_name": "STRING", "position": 4, "nullable": true},
  {"name": "full_name", "type_text": "string", "type_name": "STRING", "position": 5, "nullable": true},
  {"name": "email_id", "type_text": "string", "type_name": "STRING", "position": 6, "nullable": true},
  {"name": "department", "type_text": "string", "type_name": "STRING", "position": 7, "nullable": true},
  {"name": "designation", "type_text": "string", "type_name": "STRING", "position": 8, "nullable": true},
  {"name": "date_of_joining", "type_text": "date", "type_name": "DATE", "position": 9, "nullable": true},
  {"name": "date_of_exit", "type_text": "date", "type_name": "DATE", "position": 10, "nullable": true},
  {"name": "is_current", "type_text": "int", "type_name": "INT", "position": 11, "nullable": true},
  {"name": "valid_from", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 12, "nullable": true},
  {"name": "valid_to", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 13, "nullable": true},
  {"name": "record_hash", "type_text": "string", "type_name": "STRING", "position": 14, "nullable": true},
  {"name": "source_of_hire", "type_text": "string", "type_name": "STRING", "position": 15, "nullable": true},
  {"name": "employee_status", "type_text": "string", "type_name": "STRING", "position": 16, "nullable": true},
  {"name": "dw_load_timestamp", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 17, "nullable": true}
]'

register_table "gold" "fact_attendance" '[
  {"name": "dim_employee_key", "type_text": "int", "type_name": "INT", "position": 0, "nullable": true},
  {"name": "dim_date_key", "type_text": "int", "type_name": "INT", "position": 1, "nullable": true},
  {"name": "source_attendance_id", "type_text": "string", "type_name": "STRING", "position": 2, "nullable": true},
  {"name": "work_date_str", "type_text": "string", "type_name": "STRING", "position": 3, "nullable": true},
  {"name": "deviation_time", "type_text": "string", "type_name": "STRING", "position": 4, "nullable": true},
  {"name": "first_in_building", "type_text": "string", "type_name": "STRING", "position": 5, "nullable": true},
  {"name": "first_in_location", "type_text": "string", "type_name": "STRING", "position": 6, "nullable": true},
  {"name": "last_out_building", "type_text": "string", "type_name": "STRING", "position": 7, "nullable": true},
  {"name": "last_out_location", "type_text": "string", "type_name": "STRING", "position": 8, "nullable": true},
  {"name": "shift_end_time_str", "type_text": "string", "type_name": "STRING", "position": 9, "nullable": true},
  {"name": "shift_name", "type_text": "string", "type_name": "STRING", "position": 10, "nullable": true},
  {"name": "zoho_status", "type_text": "string", "type_name": "STRING", "position": 11, "nullable": true},
  {"name": "zoho_total_hours_str", "type_text": "string", "type_name": "STRING", "position": 12, "nullable": true},
  {"name": "zoho_working_hour_str", "type_text": "string", "type_name": "STRING", "position": 13, "nullable": true},
  {"name": "ercno", "type_text": "long", "type_name": "LONG", "position": 14, "nullable": true},
  {"name": "shifttime_str", "type_text": "string", "type_name": "STRING", "position": 15, "nullable": true},
  {"name": "first_in_ts", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 16, "nullable": true},
  {"name": "last_out_ts", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 17, "nullable": true},
  {"name": "work_minutes", "type_text": "int", "type_name": "INT", "position": 18, "nullable": true},
  {"name": "total_minutes", "type_text": "int", "type_name": "INT", "position": 19, "nullable": true},
  {"name": "dw_load_timestamp", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 20, "nullable": true}
]'

register_table "gold" "fact_attendance_biometric" '[
  {"name": "dim_employee_key", "type_text": "int", "type_name": "INT", "position": 0, "nullable": true},
  {"name": "dim_date_key", "type_text": "int", "type_name": "INT", "position": 1, "nullable": true},
  {"name": "source_bio_id", "type_text": "string", "type_name": "STRING", "position": 2, "nullable": true},
  {"name": "name_in_bio", "type_text": "string", "type_name": "STRING", "position": 3, "nullable": true},
  {"name": "shift_in_bio", "type_text": "string", "type_name": "STRING", "position": 4, "nullable": true},
  {"name": "bio_in_time_str", "type_text": "string", "type_name": "STRING", "position": 5, "nullable": true},
  {"name": "bio_out_time_str", "type_text": "string", "type_name": "STRING", "position": 6, "nullable": true},
  {"name": "bio_work_dur_str", "type_text": "string", "type_name": "STRING", "position": 7, "nullable": true},
  {"name": "ot_str", "type_text": "string", "type_name": "STRING", "position": 8, "nullable": true},
  {"name": "bio_total_dur_str", "type_text": "string", "type_name": "STRING", "position": 9, "nullable": true},
  {"name": "bio_status", "type_text": "string", "type_name": "STRING", "position": 10, "nullable": true},
  {"name": "remarks", "type_text": "string", "type_name": "STRING", "position": 11, "nullable": true},
  {"name": "work_date", "type_text": "date", "type_name": "DATE", "position": 12, "nullable": true},
  {"name": "work_minutes", "type_text": "int", "type_name": "INT", "position": 13, "nullable": true},
  {"name": "total_minutes", "type_text": "int", "type_name": "INT", "position": 14, "nullable": true},
  {"name": "ot_minutes", "type_text": "int", "type_name": "INT", "position": 15, "nullable": true},
  {"name": "dw_load_timestamp", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 16, "nullable": true}
]'

register_table "gold" "fact_leave" '[
  {"name": "dim_employee_key", "type_text": "int", "type_name": "INT", "position": 0, "nullable": true},
  {"name": "dim_date_key", "type_text": "int", "type_name": "INT", "position": 1, "nullable": true},
  {"name": "source_leave_id", "type_text": "long", "type_name": "LONG", "position": 2, "nullable": true},
  {"name": "zoho_record_id", "type_text": "string", "type_name": "STRING", "position": 3, "nullable": true},
  {"name": "approval_status", "type_text": "string", "type_name": "STRING", "position": 4, "nullable": true},
  {"name": "date_of_request", "type_text": "date", "type_name": "DATE", "position": 5, "nullable": true},
  {"name": "from_date", "type_text": "date", "type_name": "DATE", "position": 6, "nullable": true},
  {"name": "to_date", "type_text": "date", "type_name": "DATE", "position": 7, "nullable": true},
  {"name": "leave_type", "type_text": "string", "type_name": "STRING", "position": 8, "nullable": true},
  {"name": "team_email_id", "type_text": "string", "type_name": "STRING", "position": 9, "nullable": true},
  {"name": "leave_unit", "type_text": "string", "type_name": "STRING", "position": 10, "nullable": true},
  {"name": "zuidin_leave", "type_text": "long", "type_name": "LONG", "position": 11, "nullable": true},
  {"name": "reason", "type_text": "string", "type_name": "STRING", "position": 12, "nullable": true},
  {"name": "leave_start_time", "type_text": "string", "type_name": "STRING", "position": 13, "nullable": true},
  {"name": "leave_end_time", "type_text": "string", "type_name": "STRING", "position": 14, "nullable": true},
  {"name": "leave_count", "type_text": "decimal(4,1)", "type_name": "DECIMAL", "position": 15, "nullable": true},
  {"name": "dw_load_timestamp", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 16, "nullable": true}
]'

register_table "gold" "fact_timesheet" '[
  {"name": "dim_employee_key", "type_text": "int", "type_name": "INT", "position": 0, "nullable": true},
  {"name": "dim_date_key", "type_text": "int", "type_name": "INT", "position": 1, "nullable": true},
  {"name": "source_log_id", "type_text": "int", "type_name": "INT", "position": 2, "nullable": true},
  {"name": "log_approval_status", "type_text": "string", "type_name": "STRING", "position": 3, "nullable": true},
  {"name": "billing_status", "type_text": "string", "type_name": "STRING", "position": 4, "nullable": true},
  {"name": "job_name", "type_text": "string", "type_name": "STRING", "position": 5, "nullable": true},
  {"name": "project_name", "type_text": "string", "type_name": "STRING", "position": 6, "nullable": true},
  {"name": "task_name", "type_text": "string", "type_name": "STRING", "position": 7, "nullable": true},
  {"name": "hours_str", "type_text": "string", "type_name": "STRING", "position": 8, "nullable": true},
  {"name": "minutes_int", "type_text": "int", "type_name": "INT", "position": 9, "nullable": true},
  {"name": "work_date", "type_text": "date", "type_name": "DATE", "position": 10, "nullable": true},
  {"name": "timelog_id", "type_text": "string", "type_name": "STRING", "position": 11, "nullable": true},
  {"name": "dw_load_timestamp", "type_text": "timestamp", "type_name": "TIMESTAMP", "position": 12, "nullable": true}
]'

# ============================================================
# VERIFY
# ============================================================
echo ""
echo "========================================"
echo "Verification — Tables in each schema:"
echo "========================================"

for schema in bronze silver gold; do
  echo ""
  echo "--- hr_analytics.$schema ---"
  curl -s "$UC_URL/tables?catalog_name=$CATALOG&schema_name=$schema" | \
    python3 -c "
import sys, json
data = json.load(sys.stdin)
tables = data.get('tables', [])
if not tables:
    print('  (no tables)')
for t in tables:
    print(f\"  ✅ {t['name']} | {t.get('data_source_format','?')} | {t.get('table_type','?')} | {t.get('storage_location','')}\")
"
done

echo ""
echo "Done! ✅"
