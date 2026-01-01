import re
import pandas as pd 

import logging
log = logging.getLogger("airflow.task")

def remove_exact_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows that are fully identical across all columns (keep first) and log deletions."""
    before = len(df)
    df_out = df.loc[~df.duplicated(keep="first")].copy()
    deleted = before - len(df_out)
    log.debug(f"[dedupe] Removed {deleted} exact duplicate row(s). (before={before}, after={len(df_out)})")
    return df_out

def fix_dates(col: pd.Series, output_format: str = "%m/%d/%Y %H:%M:%S") -> pd.Series:
    """
    Takes a DataFrame column (pd.Series) and returns a new Series formatted as output_format.
    Unparseable / empty values become "" (empty string), matching your original behavior.
    """

    # Start with all empty strings
    out = pd.Series("", index=col.index, dtype="object")

    # Treat NaN/None as empty
    non_null = col.notna()
    if not non_null.any():
        return out

    # Work on strings for parsing
    s = col.astype(str).str.strip()
    invalid_str = s.eq("") | s.str.lower().isin({"nan", "none", "null"})
    valid = non_null & ~invalid_str
    if not valid.any():
        return out

    dt = pd.Series(pd.NaT, index=col.index, dtype="datetime64[ns]")

    # --- 0) Space-separated numeric formats FIRST ---
    v = s[valid]

    mask_ymd = v.str.match(r"^\d{4}\s+\d{1,2}\s+\d{1,2}$", na=False)  # YYYY MM DD
    if mask_ymd.any():
        idx = v[mask_ymd].index
        dt.loc[idx] = pd.to_datetime(v.loc[idx], errors="coerce", format="%Y %m %d")

    mask_dmy = v.str.match(r"^\d{1,2}\s+\d{1,2}\s+\d{4}$", na=False)  # DD MM YYYY
    if mask_dmy.any():
        idx = v[mask_dmy].index
        dt.loc[idx] = pd.to_datetime(v.loc[idx], errors="coerce", format="%d %m %Y")

    # --- 1) General parse ---
    remaining = valid & dt.isna()

    # ISO: YYYY-MM-DD (or YYYY-M-D)
    if remaining.any():
        v2 = s[remaining]
        iso = v2.str.match(r"^\d{4}-\d{1,2}-\d{1,2}$", na=False)
        if iso.any():
            idx = v2[iso].index
            dt.loc[idx] = pd.to_datetime(v2.loc[idx], errors="coerce", format="%Y-%m-%d")

    # Everything else (slashes, month names, etc.)
    remaining = valid & dt.isna()
    if remaining.any():
        idx = s[remaining].index
        dt.loc[idx] = pd.to_datetime(s.loc[idx], errors="coerce", dayfirst=True)

    # --- 2) Fallback: yearfirst ---
    remaining = valid & dt.isna()
    if remaining.any():
        idx = s[remaining].index
        dt.loc[idx] = pd.to_datetime(s.loc[idx], errors="coerce", yearfirst=True)

    # Log failures (optional; can be noisy for big columns)
    failed = valid & dt.isna()
    if failed.any():
        # show up to first 10 unique failures to avoid huge logs
        unparsed_dates = s.loc[failed].dropna().unique()
        log.debug(f"[FIX-DATE] Unparsed dates:\n{unparsed_dates}")

    # Format successes
    ok = valid & dt.notna()
    out.loc[ok] = dt.loc[ok].dt.strftime(output_format)

    return out