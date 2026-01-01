from __future__ import annotations

import os
import sqlite3
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from common.common_normalization import * 

import logging
log = logging.getLogger("airflow.task")

DB_DIR = os.environ.get("AIRFLOW_DB_DIR", "/opt/airflow/db")
DB_PATH = os.environ.get("SQLITE_DB_PATH", os.path.join(DB_DIR, "case-study.db"))

SRC_TABLE = "data_source_LEADS"
DST_TABLE = "dwh_LEADS"


@dag(
    dag_id="sqlite_etl_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sqlite", "etl"],
)
def sqlite_etl_3_tasks():
    """
    3-task ETL:
      1) extract from SQLite -> file
      2) transform in Python -> file
      3) load file -> SQLite table
    """

    @task
    def extract_to_file() -> str:
        """
        Reads rows from SRC_TABLE and writes to a parquet file in /opt/airflow/data.
        Returns the extracted file path (small string -> OK for XCom).
        """
        log.info("-------> extract_to_file")

        os.makedirs(DB_DIR, exist_ok=True)
        extract_path = os.path.join(DB_DIR, "extract.parquet")
        if os.path.exists(extract_path):
            os.remove(extract_path)

        with sqlite3.connect(DB_PATH) as conn:
            # Adjust query as needed; this loads all rows (~50k) into memory at once.
            # For much larger tables, use chunking.
            df = pd.read_sql_query(f"SELECT * FROM {SRC_TABLE};", conn)

        # Parquet is efficient, but requires pyarrow or fastparquet installed.
        # If you don't have that, switch to df.to_csv(..., index=False)
        df.to_parquet(extract_path, index=False)

        return extract_path

    @task
    def transform_python(extract_path: str) -> str:
        """
        Transforms data using Python (pandas).
        Writes transformed dataset to a new file.
        Returns the transformed file path.
        """
        log.info("-------> transform_python")

        transformed_path = os.path.join(DB_DIR, "transformed.parquet")
        if os.path.exists(transformed_path):
            os.remove(transformed_path)

        df = pd.read_parquet(extract_path)

        # Remove exact duplicated rows 
        df = remove_exact_duplicate_rows(df)

        # ---- Example transformations (edit to your needs) ----
        # 1) Standardize column names
        # df.columns = [c.strip().lower() for c in df.columns]

        # # 2) Add a derived column if columns exist
        # if "amount" in df.columns:
        #     df["amount_usd"] = df["amount"].astype(float) * 1.0

        # # 3) Drop duplicates if an id exists
        # if "id" in df.columns:
        #     df = df.drop_duplicates(subset=["id"])
        # -----------------------------------------------------

        df.to_parquet(transformed_path, index=False)
        return transformed_path

    @task
    def load_to_sqlite(transformed_path: str) -> None:
        """
        Loads transformed dataset into DST_TABLE in SQLite.
        Uses pandas to_sql for convenience.
        """
        log.info("-------> load_to_sqlite")

        df = pd.read_parquet(transformed_path)

        with sqlite3.connect(DB_PATH) as conn:
            # Create/replace destination table.
            # If you prefer append, use if_exists="append"
            df.to_sql(DST_TABLE, conn, if_exists="replace", index=False)

            # Optional: add indexes (example)
            # if "id" in df.columns:
            #     conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{DST_TABLE}_id ON {DST_TABLE}(id);")

    extracted = extract_to_file()
    transformed = transform_python(extracted)
    load_to_sqlite(transformed)


sqlite_etl_3_tasks()
