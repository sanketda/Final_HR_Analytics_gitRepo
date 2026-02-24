import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load env variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")


def create_table():
    """Create databricks_runs table in Postgres if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS databricks_runs (
        id SERIAL PRIMARY KEY,
        task_run_id BIGINT,
        run_id BIGINT,
        job_id BIGINT,
        task_name TEXT,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        execution_duration BIGINT,
        status TEXT,
        error_message TEXT,
        life_cycle_state TEXT,
        result_state TEXT,
        user_cancelled_or_timeout TEXT,
        cluster_id TEXT,
        trigger TEXT,
        notebook_path TEXT,
        run_type TEXT,
        run_page_url TEXT
    );
    """

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(create_table_query)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Table 'databricks_runs' created (or already exists).")

    except Exception as e:
        print(f"❌ Error creating table: {e}")


if __name__ == "__main__":
    create_table()
