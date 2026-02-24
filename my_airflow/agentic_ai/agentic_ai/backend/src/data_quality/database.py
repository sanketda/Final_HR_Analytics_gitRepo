from sqlalchemy import create_engine, text
import pandas as pd
import simplejson as json
from datetime import datetime, timezone

# ---- DB Config ----
DB_USER = "new_user"
DB_PASS = "userpswd"
DB_HOST = "164.52.204.204"
DB_PORT = "5432"
DB_NAME = "api_usage_db"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True
)

def save_df_to_postgres(df: pd.DataFrame, table_name: str):
    if df.empty:
        print("DataFrame is empty")
        return

    try:
        df.to_sql(table_name, engine, if_exists="append", index=False, method="multi", chunksize=500)
        print(f"Successfully inserted {len(df)} rows into '{table_name}'")
    except Exception as e:
        print(f"Error inserting logs: {e}")

def get_full_name(table_id: str) -> str:
    query = text("SELECT full_name FROM tables WHERE table_id = :table_id")
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"table_id": table_id}).fetchone()
            if result:
                return result[0]
            else:
                print(f"No entry found for table_id={table_id}")
                return None
    except Exception as e:
        print(f"Error fetching full_name: {e}")
        return None

def get_metadata(table_id: str) -> dict:
    query = text("SELECT metadata FROM tables WHERE table_id = :table_id")
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"table_id": table_id}).fetchone()
            if result:
                return json.loads(result[0]) if isinstance(result[0], str) else result[0]
            else:
                print(f"No entry found for table_id={table_id}")
                return None
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return None
    
def update_last_dq_checked(table_id: str):
    query = text("""
        UPDATE tables
        SET last_dq_checked = :timestamp
        WHERE table_id = :table_id
    """)
    try:
        # Get current UTC time as epoch seconds (BIGINT)
        current_ts = int(datetime.now(timezone.utc).timestamp())
        
        with engine.begin() as conn:
            conn.execute(query, {
                "timestamp": current_ts,
                "table_id": table_id
            })
        print(f"Updated last_dq_checked={current_ts} for table_id={table_id}")
    except Exception as e:
        print(f"Error updating last_dq_checked: {e}")

def dq_check_tables():
    query = text("""
        SELECT table_id
        FROM tables
        WHERE updated_at > last_dq_checked
           OR last_dq_checked IS NULL
    """)
    try:
        with engine.connect() as conn:
            results = conn.execute(query).fetchall()
            table_ids = [row[0] for row in results]
            print(f"Tables needing DQ check: {table_ids}")
            return table_ids
    except Exception as e:
        print(f"Error fetching dq_check_tables: {e}")
        return []

if __name__ == "__main__":
    print(get_full_name("338c21da-10c1-459f-85bf-9251c30dd9b8"))
    print(get_metadata("338c21da-10c1-459f-85bf-9251c30dd9b8"))
