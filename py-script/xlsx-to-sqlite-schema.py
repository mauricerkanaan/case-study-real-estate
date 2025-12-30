import sqlite3
from typing import Tuple
import pandas as pd
import numpy as np 
import re

# =========================
# Edit these variables only
# =========================
EXCEL_PATH = r"D:/whaw/case-study-real-estate/data/Case_Study_Data___Data_Engineering__1_.xlsx"
SHEET_NAME = "DE SALES"          # exact sheet name
TABLE_NAME = "data_source_SALES"        # desired SQLite table name
SQLITE_DB_PATH = r"D:/whaw/case-study-real-estate/db/case-study.db"   # where to create table (optional)
EXECUTE_CREATE_INSERT = True          # True = run CREATE TABLE, False = just print SQL

# How many rows to sample for type inference (None = all rows)
INFER_SAMPLE_ROWS = 500

# If True, will add "NOT NULL" where the sampled data has no nulls
ADD_NOT_NULL_CONSTRAINTS = False

# If True, will add PRIMARY KEY if a column name matches one of these candidates
# AUTO_PK_CANDIDATES = {"id", "pk", "key"}
AUTO_PK_CANDIDATES = {}
# =========================


# Datetime formats to try (add yours if needed)
COMMON_DATETIME_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
]
# =========================


def sqlite_quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def normalize_identifier(name: str) -> str:
    s = str(name).strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9a-zA-Z_]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"col_{s}"
    return s


def try_datetime_ratio(series: pd.Series) -> float:
    """
    Return the best 'datetime parse success ratio' using only known formats.
    We DO NOT call pd.to_datetime without a format, to avoid warnings and
    inconsistent per-element parsing.
    """
    s = series.dropna()
    if s.empty:
        return 0.0

    best_ratio = 0.0
    for fmt in COMMON_DATETIME_FORMATS:
        parsed = pd.to_datetime(s, errors="coerce", format=fmt)
        ratio = parsed.notna().mean()
        if ratio > best_ratio:
            best_ratio = ratio
        if best_ratio >= 0.99:
            break

    return best_ratio


def infer_sqlite_type(series: pd.Series) -> str:
    # If all null, default to TEXT
    if series.dropna().empty:
        return "TEXT"

    dtype = series.dtype

    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "REAL"
    if pd.api.types.is_bool_dtype(dtype):
        return "INTEGER"  # store booleans as 0/1
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TEXT"
    if pd.api.types.is_timedelta64_dtype(dtype):
        return "TEXT"

    # Object/string: detect numbers
    s = series.dropna()

    coerced_num = pd.to_numeric(s, errors="coerce")
    num_ratio = coerced_num.notna().mean()
    if num_ratio > 0.95:
        fractional = (coerced_num.dropna() % 1 != 0).any()
        return "REAL" if fractional else "INTEGER"

    # Object/string: detect datetime only via known formats (no warning fallback)
    dt_ratio = try_datetime_ratio(series)
    if dt_ratio > 0.95:
        return "TEXT"

    return "TEXT"


def build_create_table_sql(df: pd.DataFrame) -> Tuple[str, str]:
    original_cols = list(df.columns)
    normalized = [normalize_identifier(c) for c in original_cols]

    # ensure unique column names after normalization
    seen = {}
    final_cols = []
    for c in normalized:
        if c not in seen:
            seen[c] = 0
            final_cols.append(c)
        else:
            seen[c] += 1
            final_cols.append(f"{c}_{seen[c]}")

    sample_df = df.head(INFER_SAMPLE_ROWS) if INFER_SAMPLE_ROWS else df
    
    # Optional PK detection
    pk_col = None
    for c in final_cols:
        if c.lower() in AUTO_PK_CANDIDATES:
            pk_col = c
            break
    
    col_defs = []
    for orig, col in zip(original_cols, final_cols):
        series = sample_df[orig]
        col_type = infer_sqlite_type(series)

        parts = [sqlite_quote_ident(col), col_type]

        if ADD_NOT_NULL_CONSTRAINTS and series.isna().sum() == 0:
            parts.append("NOT NULL")

        if pk_col and col == pk_col:
            parts.append("PRIMARY KEY")

        col_defs.append(" ".join(parts))
    
    return  (
        f"CREATE TABLE IF NOT EXISTS {sqlite_quote_ident(TABLE_NAME)} (\n  "
        + ",\n  ".join(col_defs)
        + "\n);"
    )

def infer_sqlite_type(dtype) -> str:
    """Map pandas dtype -> SQLite type."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "REAL"
    if pd.api.types.is_bool_dtype(dtype):
        return "INTEGER"  # store booleans as 0/1
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TEXT"     # store ISO strings
    return "TEXT"

def quote_ident(name: str) -> str:
    """Safely quote SQLite identifiers (table/column names)."""
    name = name.replace('"', '""')
    return f'"{name}"'

def normalize_df_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetimes to ISO strings and NaN to None for SQLite."""
    out = df.copy()

    # Convert datetimes to ISO strings
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert NaN/NaT -> None
    out = out.replace({np.nan: None})
    return out

def main():
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

    create_sql_ds = build_create_table_sql(df)
    print("---- Generated CREATE TABLE SQL ----")
    print(create_sql_ds)

    if EXECUTE_CREATE_INSERT:
        try:
            conn = sqlite3.connect(SQLITE_DB_PATH)
            conn.execute(create_sql_ds)
            conn.commit()
            print(f"\nTable '{TABLE_NAME}' created (or already exists) in: {SQLITE_DB_PATH}")

            conn.execute(f"DELETE FROM {TABLE_NAME};")
            conn.commit()
            print(f"\nAll data are deleted from Table '{TABLE_NAME}'")

            df2 = normalize_df_for_sqlite(df)
            df2.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
            conn.commit()
            print(f"Inserted {len(df2)} rows into '{TABLE_NAME}'.")
            
        finally:
            conn.close()
    

if __name__ == "__main__":
    main()