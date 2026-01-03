import pandas as pd 

from case_study_real_estate_dags.common.common_transformation import * 

def transform_leads(df: pd.DataFrame) -> pd.DataFrame: 
    # Remove exact duplicated rows 
    log.debug("[Transformation LEADS] Remove exact duplicate rows")
    df = remove_exact_duplicate_rows(df)

    # Normalize all string columns 
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    
    log.debug("[Transformation LEADS] Normalize all string columns")
    df[str_cols] = df[str_cols].apply(normalize_str)
    
    log.debug("[Transformation LEADS] Decode arabic")
    df[str_cols] = df[str_cols].apply(decode_str)
    

    # Normalize dates
    log.debug("[Transformation LEADS] Normalize dates")
    dates_cols = ["date_of_last_request", "created_at", "updated_at", "date_of_last_contact"]
    df[dates_cols] = df[dates_cols].apply(fix_dates)

    # Fix booleans 
    log.debug("[Transformation LEADS] Normalize booleans")
    bool_cols = ["buyer", "seller", "commercial", "merged", "do_not_call"]
    df[bool_cols] = df[bool_cols].apply(fix_bools) 

    return df 
