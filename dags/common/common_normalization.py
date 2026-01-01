import pandas as pd 

import logging
log = logging.getLogger("airflow.task")

def remove_exact_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows that are fully identical across all columns (keep first) and log deletions."""
    before = len(df)
    df_out = df.loc[~df.duplicated(keep="first")].copy()
    deleted = before - len(df_out)
    log.info(f"[dedupe] Removed {deleted} exact duplicate row(s). (before={before}, after={len(df_out)})")
    return df_out
