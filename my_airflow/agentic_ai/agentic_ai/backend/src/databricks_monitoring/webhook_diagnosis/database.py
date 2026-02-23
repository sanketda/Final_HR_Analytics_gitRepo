import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Read credentials
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# Create SQLAlchemy engine (connection pool)
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
