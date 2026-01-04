import pandas as pd 
from typing import List 
import os
import sqlite3
from datetime import datetime

from case_study_real_estate_dags.common.common_transformation import *
from case_study_real_estate_dags.common.common_scd_2 import * 

import logging
log = logging.getLogger("airflow.task")

def extract_db_2_parquet(db_dir: str, db_path: str, src_table: str) -> str: 
    """
    Reads rows from src_table and writes to a parquet file in DB_DIR.
    Returns the extracted file path.
    """
    log.info("[DB2Parquet] Start storing the db in parquet file")

    pname = f"extract-{src_table}.parquet"
    os.makedirs(db_dir, exist_ok=True)
    extract_path = os.path.join(db_dir, pname)
    
    log.info(f"[DB2Parquet] Extract data from {src_table} into {extract_path}")    

    if os.path.exists(extract_path):
        os.remove(extract_path)

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {src_table};", conn)

    log.info(f"[DB2Parquet] df.shape {df.shape}")

    df.to_parquet(extract_path, index=False)

    return extract_path

def exclude_common_data(src_path: str, dst_path: str) -> str: 
    log.info(f"[ExcludeCommon] Excluding common data between {src_path} and {dst_path}")
    df_src = pd.read_parquet(src_path)
    df_dst = pd.read_parquet(dst_path)

    # df_src = df_src.replace({None: ""})
    # df_dst = df_dst.replace({None: ""})

    df_src = fill_na_dtype_safe(df_src)
    df_dst = fill_na_dtype_safe(df_dst)
    df_dst[df_src.columns] = df_dst[df_src.columns].astype(df_src[df_src.columns].dtypes.to_dict())

    df_dst["id"] = df_dst["src_id"]
    df_dst = df_dst.drop(columns=["id"]).rename(columns={"src_id": "id"})

    not_in_dst = (
        df_src
            .merge(df_dst[df_src.columns].drop_duplicates(), how="left", indicator=True)
            .query('_merge == "left_only"')
            .drop(columns="_merge")
    )

    not_in_dst.to_parquet(src_path, index=False)
    
    log.info(f"[ExcludeCommon] not_in_dst.shape {not_in_dst.shape}")
    
    return src_path

def transform_data(db_dir: str, extract_path: str, src_table: str, dates_cols: List[str]=None, bool_cols: List[str]=None) -> str: 
    """
    Transforms data using Python (pandas).
    Writes transformed dataset to a new file.
    Returns the transformed file path.
    """
    log.info("[TransformData] Start data transformation")

    pname = f"transformed-{src_table}.parquet"
    transformed_path = os.path.join(db_dir, pname)
    if os.path.exists(transformed_path):
        os.remove(transformed_path)

    log.info(f"[TransformData] Results will be stored in {transformed_path}")

    df = pd.read_parquet(extract_path)

    # Remove exact duplicated rows 
    log.info(f"[TransformData] Remove exact duplicate rows from {src_table}")
    df = remove_exact_duplicate_rows(df)

    # Normalize all string columns 
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    
    if str_cols is not None and not str_cols.empty:
        log.info(f"[TransformData] Normalize all string columns in {src_table}")
        log.info(f"[TransformData] String columns:\n{str_cols}")
        df[str_cols] = df[str_cols].apply(normalize_str)

        log.info(f"[TransformData] Decoding arabic words in {src_table}")
        log.info(f"[TransformData] String columns:\n{str_cols}")
        df[str_cols] = df[str_cols].apply(decode_str)
        

    # Normalize dates
    if dates_cols is not None and len(dates_cols) > 0:
        log.info(f"[TransformData] Normalize all dates in {src_table}")
        log.info(f"[TransformData] Dates columns:\n{dates_cols}")
        df[dates_cols] = df[dates_cols].apply(fix_dates)

    # Fix booleans 
    if bool_cols is not None and len(bool_cols) > 0:
        log.info(f"[TransformData] Normalize booleans in {src_table}")
        log.info(f"[TransformData] Boolean columns:\n{bool_cols}")
        df[bool_cols] = df[bool_cols].apply(fix_bools) 

    # Correct free-text in best_time_to_call using cahce llm 
    # if "best_time_to_call" in df.columns: 
    #     log.info(f"[LLM] Correct free text in 'best_time_to_call'")
    #     correct_free_text        
        

    log.info(f"[TransformData] df.shape {df.shape}")
    df.to_parquet(transformed_path, index=False)

    return transformed_path  

def load_parquet_2_db(db_path: str, transformed_path: str, dst_path: str, dst_table: str) -> str:
    """
    Loads transformed dataset into DST_TABLE in SQLite.
    Uses pandas to_sql for convenience.
    """
    log.info(f"[LoadData] Start loading data")
    log.info(f"[LoadData] From {transformed_path} to table {dst_table} in {db_path}")

    df_transformed = pd.read_parquet(transformed_path)
    df_dst = pd.read_parquet(dst_path)

    """
    - for every row in df_transformer 
        if id exists in df_dst -> update based on the last version and insert in df_dst
        if not insert into df_dst
        change df_dst and send it to the db 
    """
    df_transformed["src_id"] = df_transformed["id"]
    df_transformed["effective_start_date"] = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    df_transformed["effective_end_date"] = pd.NA
    df_transformed["is_current"] = False
    df_transformed["version_id"] = -1

    log.info(f"[LoadData] df_transformed.shape {df_transformed.shape}")

    with sqlite3.connect(db_path) as conn:
        # conn.execute(f"DELETE FROM {dst_table}")
        # Append with respect to the existing table schema 
        df_transformed.to_sql(dst_table, conn, if_exists="append", index=False)

        # Optional: add indexes (example)
        # if "id" in df.columns:
        #     conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{DST_TABLE}_id ON {DST_TABLE}(id);")

    return transformed_path





















