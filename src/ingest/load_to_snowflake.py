"""
Loads the cleaned Parquet output from spark_transform.py into a Snowflake
RAW schema table.

Requires a free Snowflake trial account (snowflake.com/trial) and the
following environment variables set:

    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA   (e.g. RAW)

Run: python src/ingest/load_to_snowflake.py
"""

import glob
import os

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

PROCESSED_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed")
TABLE_NAME = "RAW_TRANSACTIONS"


def load_processed_parquet() -> pd.DataFrame:
    files = glob.glob(os.path.join(PROCESSED_PATH, "**", "*.parquet"), recursive=True)
    if not files:
        raise FileNotFoundError(
            "No processed Parquet files found. Run src/ingest/spark_transform.py first."
        )
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
    )


def main():
    df = load_processed_parquet()
    # Snowflake connector expects upper-case column names by convention
    df.columns = [c.upper() for c in df.columns]

    conn = get_connection()
    try:
        success, num_chunks, num_rows, _ = write_pandas(
            conn, df, TABLE_NAME, auto_create_table=True, overwrite=True
        )
        print(f"Loaded {num_rows} rows into {TABLE_NAME} (success={success})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
