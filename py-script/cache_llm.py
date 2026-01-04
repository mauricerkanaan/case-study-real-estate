import pandas as pd 
import os 
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama  

MODEL = "qwen2.5:latest" #  phi3.5:latest , qwen2.5:latest
BASE_URL = "http://host.docker.internal:11434"
SYSTEM_PROMPT = "You are a data engineer. Clean, correct, and standardize the following free-text value while preserving its meaning. Fix spelling/case, normalize whitespace/punctuation, expand obvious abbreviations, standardize dates/times/units, and remove noise. Normalize times to 12-hour format with AM/PM and include timezone if stated. Return only the cleaned text—no explanations, no labels, no quotes."

TEMPERATURE = 1
TIMEOUT_SECONDS = 120  # request timeout


def call_llm(v: str) -> str: 
    llm = ChatOllama(
        model=MODEL,
        base_url=BASE_URL,
        temperature=TEMPERATURE,
        timeout=TIMEOUT_SECONDS,
    )

    # No history: ONLY system + current user message each run
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            ("human", "{user_input}"),
        ]
    )

    chain = prompt | llm

    print("call_llm for ", v)
    correction = chain.invoke({"user_input": v}).content
    print("correction", correction)
    return correction

def correct_free_text(col: pd.Series,
    cache_path: str = "D:/whaw/case-study-real-estate/db/result_cache_llm.csv",
) -> pd.Series:
    # --- Load cache (fast + safe) ---
    if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        df_cache = pd.read_csv(
            cache_path,
            usecols=["best_time_to_call", "correction_best_time_to_call"],
            dtype={"best_time_to_call": "string", "correction_best_time_to_call": "string"},
        )
        dict_cache = dict(zip(df_cache["best_time_to_call"], df_cache["correction_best_time_to_call"]))
    else:
        dict_cache = {}

    out = col.copy()

    # Keep None/NaN/"" as-is (do nothing)
    mask_valid = col.notna() & (col != "")
    if not mask_valid.any():
        return out

    # Work on strings for consistent cache keying
    keys = col.loc[mask_valid].astype("string")

    # --- Vectorized cache replacement ---
    mapped = keys.map(dict_cache)              # cache hits -> value, misses -> <NA>
    hit_mask = mapped.notna()

    # Write back hits
    out.loc[mapped.index[hit_mask]] = mapped.loc[hit_mask].astype(object).values

    # --- LLM calls only for unique misses ---
    miss_idx = mapped.index[~hit_mask]
    if len(miss_idx) > 0:
        miss_vals = keys.loc[miss_idx]
        unique_misses = pd.unique(miss_vals.dropna())  # unique unseen strings

        new_rows = []
        for v in unique_misses:
            v_str = str(v)
            corr = call_llm(v_str)
            dict_cache[v_str] = corr
            new_rows.append((v_str, corr))

        # Append new cache rows to disk (fast; no full rewrite)
        if new_rows:
            new_df = pd.DataFrame(
                new_rows,
                columns=["best_time_to_call", "correction_best_time_to_call"],
            )
            write_header = not (os.path.exists(cache_path) and os.path.getsize(cache_path) > 0)
            new_df.to_csv(cache_path, mode="a", header=write_header, index=False)

        # Map again using updated cache and fill misses
        out.loc[miss_idx] = miss_vals.astype("string").map(dict_cache).astype(object).values

    return out

df_leads = pd.read_parquet("D:/whaw/case-study-real-estate/db/extract-dwh_LEADS.parquet") 
print(f"df_leads.shape {df_leads.shape}")
print(type(df_leads["best_time_to_call"]))

df_leads["new_best_time_to_call"] = correct_free_text(df_leads["best_time_to_call"])

df = df_leads[(df_leads["best_time_to_call"].notna()) & (df_leads["best_time_to_call"] != "")][["best_time_to_call", "new_best_time_to_call"]]
print(df.head())

    
    