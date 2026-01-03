from __future__ import annotations

import os
import sqlite3
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from case_study_real_estate_dags.common.common_transformation import * 
from case_study_real_estate_dags.common.transformation_leads import * 
from case_study_real_estate_dags.common.common_python_operator import * 

import logging
log = logging.getLogger("airflow.task")

DB_DIR = os.environ.get("AIRFLOW_DB_DIR", "/opt/airflow/db")
DB_PATH = os.environ.get("SQLITE_DB_PATH", os.path.join(DB_DIR, "case-study.db"))

SRC_TABLE_LEADS = "data_source_LEADS"
DST_TABLE_LEADS = "dwh_LEADS"


@dag(
    dag_id="sqlite_etl_dag_2",
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
        extract_path = extract_db_2_parquet(DB_DIR, DB_PATH, SRC_TABLE_LEADS)
        return extract_path

    @task
    def transform_python(extract_path: str) -> str:
        
        dates_cols = ["date_of_last_request", "created_at", "updated_at", "date_of_last_contact"]
        bool_cols = ["buyer", "seller", "commercial", "merged", "do_not_call"]

        transformed_path = transform_data(DB_DIR, extract_path, SRC_TABLE_LEADS, dates_cols, bool_cols)
        return transformed_path

    @task
    def load_to_sqlite(transformed_path: str) -> None:
        load_parquet_2_db(DB_PATH, transformed_path, DST_TABLE_LEADS)

    extracted = extract_to_file()
    transformed = transform_python(extracted)
    load_to_sqlite(transformed)


sqlite_etl_3_tasks()
