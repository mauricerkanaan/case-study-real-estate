import pandas as pd 
from datetime import datetime
import numpy as np

def reset_id_with_src_id(df: pd.DataFrame, src_id_col: str="id") -> pd.DataFrame: 
    df["src_id"] = df[src_id_col]
    df[src_id_col] = range(1, len(df) + 1)

    return df 

def upsert(df_transformed: pd.DataFrame, df_dst: pd.DataFrame, cid: int) -> pd.DataFrame: 
    cols_to_compare = df_transformed.columns.drop("id") 
    id_cpt = cid
    for idx, row in df_transformed.iterrows(): 
        id = row["id"]
        dst_rows = df_dst[(df_dst["src_id"] == id) & (df_dst["is_current"] == True)]
        if dst_rows is None or dst_rows.empty: 
            id = len(df_dst)
            df_dst.loc[id, df_transformed.columns] = row
            df_dst.loc[id, "effective_start_date"] = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            df_dst.loc[id, "is_current"] = True
            df_dst.loc[id, "version_id"] = 1
            df_dst.loc[id, "src_id"] = row["id"]
            df_dst.loc[id, "id"] = id_cpt
            id_cpt = id_cpt  + 1
        elif not row[cols_to_compare].equals(dst_rows.squeeze()[cols_to_compare]): 
            id = len(df_dst)
            df_dst.loc[id, df_transformed.columns] = row
            df_dst.loc[id, "effective_start_date"] = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            df_dst.loc[id, "is_current"] = True
            df_dst.loc[id, "version_id"] = df_dst.loc[dst_rows.index, "version_id"].max() + 1
            df_dst.loc[id, "src_id"] = row["id"]
            
            df_dst.loc[dst_rows.index, "effective_end_date"] = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            df_dst.loc[dst_rows.index, "is_current"] = False
            df_dst.loc[dst_rows.index, "src_id"] = row["id"]
            df_dst.loc[id, "id"] = id_cpt
            id_cpt = id_cpt  + 1

    # df_dst["id"] = range(cid, cid+len(df_dst)+1)

    return df_dst


def upsert_2(df_transformed: pd.DataFrame, df_dst: pd.DataFrame, cid: int) -> pd.DataFrame:
    """
    SCD Type 2 upsert (fast, vectorized).
    - Business key in source: df_transformed['id']
    - Stored in destination as: df_dst['src_id']
    - Surrogate key: df_dst['id'] allocated starting from `cid` (inclusive), avoiding collisions.

    SCD columns:
      effective_start_date = current date (UTC, normalized to date)
      effective_end_date   = NULL for current rows; set to current date when expired
      is_current           = True/False
      version_id              = 1..n per src_id

    Change detection compares all columns in df_transformed except 'id'.
    """
    print("df_transformed.columns")
    print(df_transformed.columns)
    
    print("df_dst.columns")
    print(df_dst.columns)
    
    AS_OF = datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    if df_transformed is None or df_transformed.empty:
        return (df_dst.copy() if df_dst is not None else pd.DataFrame())

    src = df_transformed.copy()
    if "id" not in src.columns:
        raise ValueError("df_transformed must contain an 'id' column (business key).")

    # one row per business key per batch
    src = src.drop_duplicates(subset=["id"], keep="last")

    dst = df_dst.copy() if df_dst is not None else pd.DataFrame()

    # Ensure SCD columns exist
    scd_cols = ["id", "src_id", "effective_start_date", "effective_end_date", "is_current", "version_id"]
    for c in scd_cols:
        if c not in dst.columns:
            dst[c] = pd.Series(dtype="object")

    # Attribute columns = all source columns except business key
    attr_cols = [c for c in src.columns if c != "id"]
    for c in attr_cols:
        if c not in dst.columns:
            dst[c] = pd.NA

    # Allocate surrogate ids starting at cid, skipping any already used ids
    def _alloc_ids(n: int, existing_ids: pd.Series, start: int) -> np.ndarray:
        used = pd.to_numeric(existing_ids, errors="coerce").dropna().astype(np.int64).to_numpy()
        if used.size == 0:
            return np.arange(start, start + n, dtype=np.int64)

        mx = used.max()
        if start > mx:
            return np.arange(start, start + n, dtype=np.int64)

        used_set = np.unique(used)
        need_len = (mx - start + 1) + n + 8  # slack
        mask = np.zeros(need_len, dtype=bool)
        in_range = used_set[(used_set >= start) & (used_set < start + need_len)]
        mask[in_range - start] = True
        free = np.flatnonzero(~mask)

        while free.size < n:
            old = mask
            mask = np.zeros(old.size * 2, dtype=bool)
            mask[: old.size] = old
            in_range = used_set[(used_set >= start) & (used_set < start + mask.size)]
            mask[in_range - start] = True
            free = np.flatnonzero(~mask)

        return (start + free[:n]).astype(np.int64)

    # If dst has no current rows / empty: insert everything as current, end_date NULL
    if dst.empty or dst["is_current"].fillna(False).astype(bool).sum() == 0:
        inserts = src.copy()
        inserts["src_id"] = inserts["id"]
        inserts = inserts.drop(columns=["id"])

        inserts.insert(0, "id", _alloc_ids(len(inserts), dst["id"], cid))
        inserts["effective_start_date"] = AS_OF
        inserts["effective_end_date"] = pd.NaT
        inserts["is_current"] = True
        inserts["version_id"] = 1

        cols = list(dict.fromkeys(list(dst.columns) + list(inserts.columns)))
        return pd.concat([dst.reindex(columns=cols).iloc[0:0], inserts.reindex(columns=cols)], ignore_index=True)

    # Work with current rows only
    cur = dst[dst["is_current"].fillna(False).astype(bool)].copy()
    cur["_dst_idx"] = cur.index

    left = src.copy()
    left["src_id"] = left["id"]

    cur_keep = ["src_id", "_dst_idx", "version_id"] + attr_cols
    merged = left.merge(cur[cur_keep], on="src_id", how="left", suffixes=("", "_cur"))

    is_new = merged["_dst_idx"].isna().to_numpy(dtype=bool)

    if attr_cols:
        is_changed = np.zeros(len(merged), dtype=bool)

        for c in attr_cols:
            s = merged[c]
            t = merged[f"{c}_cur"]

            s_na = s.isna().to_numpy(dtype=bool)
            t_na = t.isna().to_numpy(dtype=bool)

            one_na = s_na ^ t_na                      # exactly one is NULL -> changed
            both_not_na = ~(s_na | t_na)              # both present

            neq = np.zeros(len(merged), dtype=bool)   # compare only where both are not NULL
            if both_not_na.any():
                sa = s.to_numpy(dtype=object)
                ta = t.to_numpy(dtype=object)
                neq[both_not_na] = sa[both_not_na] != ta[both_not_na]

            is_changed |= (one_na | neq)

        is_changed &= ~is_new
    else:
        is_changed = np.zeros(len(merged), dtype=bool)

    # Expire current rows for changed keys: end_date = AS_OF (date), is_current=False
    if is_changed.any():
        idx_to_expire = merged.loc[is_changed, "_dst_idx"].astype(int).to_numpy()
        dst.loc[idx_to_expire, "effective_end_date"] = AS_OF
        dst.loc[idx_to_expire, "is_current"] = False

    # Insert rows for new + changed keys
    to_insert = is_new | is_changed
    if not to_insert.any():
        return dst

    ins = merged.loc[to_insert, ["src_id"] + attr_cols].copy()

    # version_id: 1 for new, old+1 for changed
    v = pd.Series(1, index=ins.index, dtype="int64")
    if is_changed.any():
        vmap = dict(
            zip(
                merged.loc[is_changed, "src_id"].to_numpy(),
                (merged.loc[is_changed, "version_id"].astype("int64") + 1).to_numpy(),
            )
        )
        v = ins["src_id"].map(vmap).fillna(1).astype("int64")
    ins["version_id"] = v

    ins["effective_start_date"] = AS_OF
    ins["effective_end_date"] = pd.NaT
    ins["is_current"] = True

    ins.insert(0, "id", _alloc_ids(len(ins), dst["id"], cid))

    cols = list(dict.fromkeys(list(dst.columns) + list(ins.columns)))
    return pd.concat([dst.reindex(columns=cols), ins.reindex(columns=cols)], ignore_index=True)


