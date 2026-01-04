from __future__ import annotations

import os
import sqlite3
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from case_study_real_estate_dags.common.common_transformation import *
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
    dag_id="sqlite_etl_dag_4",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sqlite", "etl"],
)
def sqlite_etl_6_tasks():
    @task
    def extract_src_leads() -> str:
        extract_path = extract_db_2_parquet(DB_DIR, DB_PATH, SRC_TABLE_LEADS)
        return extract_path
    
    @task
    def extract_dst_leads() -> str:
        extract_path = extract_db_2_parquet(DB_DIR, DB_PATH, DST_TABLE_LEADS, True)
        return extract_path

    @task
    def exclude_common_leads(src_leads: str, dst_leads: str) -> str:
        src_uncommon_leads = exclude_common_data(src_leads, dst_leads)
        return src_uncommon_leads

    @task
    def transform_leads(extract_path: str) -> str:
        
        dates_cols = ["date_of_last_request", "created_at", "updated_at", "date_of_last_contact"]
        bool_cols = ["buyer", "seller", "commercial", "merged", "do_not_call"]

        transformed_path = transform_data(DB_DIR, extract_path, SRC_TABLE_LEADS, dates_cols, bool_cols)
        return transformed_path

    @task
    def load_leads(transformed_path: str, dst_table: str) -> str:
        transformed_path = load_parquet_2_db(DB_PATH, transformed_path, dst_table, DST_TABLE_LEADS)
        return transformed_path

    @task
    def extract_src_sales() -> str:
        extract_path = extract_db_2_parquet(DB_DIR, DB_PATH, SRC_TABLE_SALES)
        return extract_path

    @task
    def extract_dst_sales() -> str:
        extract_path = extract_db_2_parquet(DB_DIR, DB_PATH, DST_TABLE_SALES, True)
        return extract_path
    
    @task
    def exclude_common_sales(src_sales: str, dst_sales: str) -> str:
        src_uncommon_sales = exclude_common_data(src_sales, dst_sales)
        return src_uncommon_sales

    @task
    def transform_sales(extract_path: str) -> str:        
        dates_cols = ["date_of_reservation", "reservation_update_date", "date_of_contraction"]
        
        transformed_path = transform_data(DB_DIR, extract_path, SRC_TABLE_SALES, dates_cols)
        return transformed_path

    @task
    def load_sales(transformed_path: str, dst_table: str) -> str:
        transformed_path = load_parquet_2_db(DB_PATH, transformed_path, dst_table, DST_TABLE_SALES)
        return transformed_path


    src_leads_parquet_path = extract_src_leads()
    dst_leads_parquet_path = extract_dst_leads()
    transformed_leads_parquet_path = transform_leads(src_leads_parquet_path)
    transformed_uncommon_leads_parquet_path = exclude_common_leads(transformed_leads_parquet_path, dst_leads_parquet_path)
    load_leads_parquet_path = load_leads(transformed_uncommon_leads_parquet_path, dst_leads_parquet_path)

    src_sales_parquet_path = extract_src_sales()
    dst_sales_parquet_path = extract_dst_sales()
    transformed_sales_parquet_path = transform_sales(src_sales_parquet_path)
    transformed_uncommon_sales_parquet_path = exclude_common_sales(transformed_sales_parquet_path, dst_sales_parquet_path)
    load_sales_parquet_path = load_sales(transformed_uncommon_sales_parquet_path, dst_sales_parquet_path)


sqlite_etl_6_tasks()



