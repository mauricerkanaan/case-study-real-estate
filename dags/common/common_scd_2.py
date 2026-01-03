import pandas as pd 

def reset_id_with_src_id(df: pd.DataFrame, src_id_col: str="id") -> pd.DataFrame: 
    df["src_id"] = df[src_id_col]
    df[src_id_col] = range(1, len(df) + 1)

    return df 