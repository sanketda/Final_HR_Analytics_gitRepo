

import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Add utils path
sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))

from utils.airbyte_connection_trigger import run_connection_trigger
from utils.attendance_fetch_part1 import run_sourcedynamic
from utils.attendance_fetch_part2 import run_sourcedynamic_2nd_itr

from utils.leave_fetch import run_zoho_leave
from utils.zoho_config import CONNECTION_ID_ATTENDANCE,CONNECTION_ID_LEAVE ,CONNECTION_ID_TIMESHEET
from utils.attendance_transformation import run_attendance_transform
from utils.leave_transformation import run_leave_transform

from utils.biometric_transformation import biometric_transformation

from utils.timesheet_fetch import run_zoho_timesheet
from utils.employee_transformation import run_employee_transform

from utils.airbyte_pat_utils import automate_airbyte_pat_update

from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

from utils.airbyte_pat_utils import get_airbyte_pat
from utils.timesheet_transformation import run_zoho_timesheet_transform


# AIRBYTE_PAT = get_airbyte_pat()

# ENV = os.getenv("ENV", "DEV").upper()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 3),
    "retries": 1,
    # "email": ["sanketzargad2@gmail.com", "yogeshkasar4898@gmail.com","anand.katti@shyenatechyarns.com","vikrantkulkarni74@gmail.com","vishakhast24@gmail.com"],
    "email_on_failure": False, #if ENV == "DEV" else True,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hr_analytics_dev",
    default_args=default_args,
    schedule_interval="0 14 * * 2-6", #None,   # 👈 important
    catchup=False,
    is_paused_upon_creation=False,
    description="Daily Zoho People → Airbyte pipeline",
) as dag:

    #automate_airbyte_pat_update
    change_airbyte_pat = PythonOperator(
        task_id = "update_airbyte_pat",
        python_callable = automate_airbyte_pat_update,
        execution_timeout=timedelta(minutes=30),
    )

    # Date change for attendance source @ index-0th
    airbyte_Attendance_1 = PythonOperator(
        task_id="airbyte_Attendance_1",
        python_callable=run_sourcedynamic,
        execution_timeout=timedelta(minutes=30),
    )

    # Trigger attendance Airbyte connection (first run)
    trigger_task_1 = PythonOperator(
        task_id="trigger_airbyte_connections_1",
        python_callable=run_connection_trigger,
        op_kwargs={"connection_id": CONNECTION_ID_ATTENDANCE},  # <-- exact match
        execution_timeout=timedelta(minutes=60),
    )

    # Date change for attendance source @ index-100th
    airbyte_Attendance_2 = PythonOperator(
        task_id="airbyte_Attendance_2",
        python_callable=run_sourcedynamic_2nd_itr,
        execution_timeout=timedelta(minutes=30),
    )

    # Trigger attendance Airbyte connection (second run)
    trigger_task_2 = PythonOperator(
        task_id="trigger_airbyte_connections_2",
        python_callable=run_connection_trigger,
        op_kwargs={"connection_id": CONNECTION_ID_ATTENDANCE},  # <-- exact match
        execution_timeout=timedelta(minutes=60),
    )

    # Leave source @ index 0th and date changed function
    leave_task = PythonOperator(
        task_id="leave_records_fetch",
        python_callable=run_zoho_leave,
        execution_timeout=timedelta(minutes=90),
    )

    # Trigger leave Airbyte connection
    trigger_task_3 = PythonOperator(
        task_id="trigger_airbyte_connections_3",
        python_callable=run_connection_trigger,
        op_kwargs={"connection_id": CONNECTION_ID_LEAVE},  # <-- correct param
        execution_timeout=timedelta(minutes=60),
    )

    #--------->
    # Timesheet source @ index 0th and date changed function
    timesheet_task = PythonOperator(
        task_id="timesheet_records_fetch",
        python_callable=run_zoho_timesheet,
        execution_timeout=timedelta(minutes=90),
    )

    # Trigger leave Airbyte connection
    trigger_task_4 = PythonOperator(
        task_id="trigger_airbyte_connections_4",
        python_callable=run_connection_trigger,
        op_kwargs={"connection_id": CONNECTION_ID_TIMESHEET},  # <-- correct param
        execution_timeout=timedelta(minutes=60),
    )
    #----------->
    # Employee transformation (SCD2)
    employee_transform = PythonOperator(
        task_id="transform_employee",
        python_callable=run_employee_transform,
        execution_timeout=timedelta(minutes=60),
    )
    # Attendance transformation
    attendance_transform = PythonOperator(
        task_id="transform_attendance",
        python_callable=run_attendance_transform,
        execution_timeout=timedelta(minutes=120),
    )

    # Leave transformation
    leave_transform = PythonOperator(
        task_id="transform_leave",
        python_callable=run_leave_transform,
        execution_timeout=timedelta(minutes=150),
    )

    # timesheet transform
    timesheet_transform = PythonOperator(
        task_id="transform_timesheet",
        python_callable=run_zoho_timesheet_transform,
        execution_timeout=timedelta(minutes=90),
    )

    # Biometric transformation
    biometric_transform = PythonOperator(
        task_id="transform_biometric",
        python_callable=biometric_transformation,
        execution_timeout=timedelta(minutes=150),
    )

    
    from utils.teams_notification import send_teams_alert

    # Teams Success Notification
    teams_success = PythonOperator(
        task_id='notify_teams_success',
        python_callable=send_teams_alert,
        provide_context=True,
        trigger_rule='all_success'
    )

    # Teams Failure Notification
    teams_failure = PythonOperator(
        task_id='notify_teams_failure',
        python_callable=send_teams_alert,
        provide_context=True,
        trigger_rule='one_failed'
    )

    # ============================================================
    # 🔗 Task Dependencies (Parallel Execution)
    # ============================================================

    # 1. Define Domain Fetch Flows
    attendance_fetch = (
        airbyte_Attendance_1 
        >> trigger_task_1 
        >> airbyte_Attendance_2 
        >> trigger_task_2
    )

    leave_fetch = leave_task >> trigger_task_3
    timesheet_fetch = timesheet_task >> trigger_task_4

    # 2. Parallel Orchestration
    # Parallelize credential update and initial domain steps
    change_airbyte_pat >> [attendance_fetch, leave_fetch, timesheet_fetch, employee_transform]
    
    # Fact transformations depend on Employee dimension being ready
    employee_transform >> [attendance_transform, leave_transform, timesheet_transform, biometric_transform]

    # Link domain fetches to their respective transformations
    attendance_fetch >> attendance_transform
    leave_fetch >> leave_transform
    timesheet_fetch >> timesheet_transform

    # 3. Notifications
    [attendance_transform, leave_transform, timesheet_transform, biometric_transform] >> teams_success

    # Failure notification triggered if any task in the flow fails
    [
        change_airbyte_pat, employee_transform,
        trigger_task_2, trigger_task_3, trigger_task_4,
        attendance_transform, leave_transform, timesheet_transform, biometric_transform
    ] >> teams_failure

    #  (
    #     change_airbyte_pat
    #     # >> airbyte_Attendance_1
    #     # >> trigger_task_1
    #     # >> airbyte_Attendance_2
    #     # >> trigger_task_2
    #     # >> leave_task
    #     # >> trigger_task_3
    #     # >> timesheet_task
    #     # >> trigger_task_4
    #     >> employee_transform
    #     # >> biometric_transform
    #     # >> attendance_transform
    #     # >> leave_transform
    #     # >> timesheet_transform
    #     # >> teams_success
    # )