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

SRC_TABLE_SALES = "data_source_SALES"
DST_TABLE_SALES = "dwh_SALES"


@dag(
    dag_id="sqlite_etl_dag_3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sqlite", "etl"],
)
def sqlite_etl_6_tasks():
    @task
    def extract_leads() -> str:
        extract_path = extract_db_2_parquet(DB_DIR, DB_PATH, SRC_TABLE_LEADS)
        return extract_path

    @task
    def transform_leads(extract_path: str) -> str:
        
        dates_cols = ["date_of_last_request", "created_at", "updated_at", "date_of_last_contact"]
        bool_cols = ["buyer", "seller", "commercial", "merged", "do_not_call"]

        transformed_path = transform_data(DB_DIR, extract_path, SRC_TABLE_LEADS, dates_cols, bool_cols)
        return transformed_path

    @task
    def load_leads(transformed_path: str) -> str:
        transformed_path = load_parquet_2_db(DB_PATH, transformed_path, DST_TABLE_LEADS)
        return transformed_path

    @task
    def extract_sales() -> str:
        extract_path = extract_db_2_parquet(DB_DIR, DB_PATH, SRC_TABLE_SALES)
        return extract_path

    @task
    def transform_sales(extract_path: str) -> str:        
        dates_cols = ["date_of_reservation", "reservation_update_date", "date_of_contraction"]
        
        transformed_path = transform_data(DB_DIR, extract_path, SRC_TABLE_SALES, dates_cols)
        return transformed_path

    @task
    def load_sales(transformed_path: str) -> str:
        transformed_path = load_parquet_2_db(DB_PATH, transformed_path, DST_TABLE_SALES)
        return transformed_path

    @task
    def join(load_leads_path: str, load_sales_path: str) -> None:
        print("----------------> DONE")
        print(load_leads_path)
        print(load_sales_path)

    extracted_leads = extract_leads()
    transformed_leads = transform_leads(extracted_leads)
    load_leads_path = load_leads(transformed_leads)

    extracted_sales = extract_sales()
    transformed_sales = transform_sales(extracted_sales)
    load_sales_path = load_sales(transformed_sales)

    join(load_leads_path, load_sales_path)


sqlite_etl_6_tasks()
