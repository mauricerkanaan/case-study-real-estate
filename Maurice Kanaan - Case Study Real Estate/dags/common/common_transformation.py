import re
import pandas as pd 
import urllib.parse
from typing import Any
import os
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama  

import logging
log = logging.getLogger("airflow.task")

MODEL = "qwen2.5:latest" #  phi3.5:latest , qwen2.5:latest
BASE_URL = "http://host.docker.internal:11434"
SYSTEM_PROMPT = "You are a data engineer. Clean, correct, and standardize the following free-text value while preserving its meaning. Fix spelling/case, normalize whitespace/punctuation, expand obvious abbreviations, standardize dates/times/units, and remove noise. Normalize times to 12-hour format with AM/PM and include timezone if stated. Return only the cleaned text—no explanations, no labels, no quotes."

TEMPERATURE = 1
TIMEOUT_SECONDS = 120  # request timeout
CACHE_PATH = "/opt/airflow/db/cache_llm.csv"

def fill_na_dtype_safe(df: pd.DataFrame, fill_str: str = "", fill_bool=None, fill_num=None, fill_dt=None) -> pd.DataFrame:
    out = df.copy()

    # strings/objects -> ""
    str_cols = out.select_dtypes(include=["object", "string"]).columns
    if len(str_cols):
        out[str_cols] = out[str_cols].fillna(fill_str)

    # pandas nullable booleans / numpy bool
    bool_cols = out.select_dtypes(include=["boolean", "bool"]).columns
    if len(bool_cols) and fill_bool is not None:
        out[bool_cols] = out[bool_cols].fillna(fill_bool).astype("boolean")

    # numbers
    num_cols = out.select_dtypes(include=["number"]).columns
    if len(num_cols) and fill_num is not None:
        out[num_cols] = out[num_cols].fillna(fill_num)

    # datetimes
    dt_cols = out.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns
    if len(dt_cols) and fill_dt is not None:
        out[dt_cols] = out[dt_cols].fillna(fill_dt)

    return out

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

def fix_bools(col: pd.Series) -> pd.Series:
    """
    Normalize a DataFrame column to pandas nullable boolean (dtype 'boolean').

    Accepts: True/False, 1/0, and strings like: yes/no, y/n, true/false, t/f, on/off.
    Unparseable values -> <NA>.
    """
    true_set = {"true", "t", "yes", "y", "1", "on"}
    false_set = {"false", "f", "no", "n", "0", "off"}

    out = pd.Series(pd.NA, index=col.index, dtype="boolean")

    # 1) Native bools
    is_bool = col.map(lambda x: isinstance(x, bool))
    out.loc[is_bool] = col.loc[is_bool].astype("boolean")

    # 2) Numbers (including numeric strings)
    remaining = out.isna() & col.notna()
    if remaining.any():
        num = pd.to_numeric(col.loc[remaining], errors="coerce")
        out.loc[num[num == 1].index] = True
        out.loc[num[num == 0].index] = False

    # 3) Strings
    remaining = out.isna() & col.notna()
    if remaining.any():
        s = col.loc[remaining].astype(str).str.strip()
        s_lower = s.str.lower()

        # treat null-like strings as missing
        nullish = s_lower.isin({"", "nan", "none", "null"})
        s_lower = s_lower[~nullish]

        out.loc[s_lower[s_lower.isin(true_set)].index] = True
        out.loc[s_lower[s_lower.isin(false_set)].index] = False

    # Log failures (sample to avoid noise)
    failed = col.notna() & out.isna()
    if failed.any():
        unparsed_values = col.loc[failed].astype(str).str.strip().unique()
        log.debug(f"[FIX-BOOL] Unparsed values:\n{unparsed_values}")

    return out

def normalize_str(s: pd.Series) -> pd.Series:
    # 1) Ensure string dtype but keep NaNs as <NA>
    s = s.astype("string")

    # 2) Basic trim
    s = s.str.strip()

    # 3) Standardize separators (your part + more)
    s = (s
         .str.replace(r"[-_/]+", " ", regex=True)     # -, _, / -> space
         .str.replace("&", " and ", regex=False)      # & -> and
         .str.replace(r"\s+", " ", regex=True)        # collapse whitespace
    )
    
    # 4) Remove punctuation (keep letters/numbers/spaces)
    #    Example: "Tiara Res." -> "Tiara Res"
    # s = s.str.replace(r"[^\w\s]", " ", regex=True).str.replace(r"\s+", " ", regex=True)

    # 5) Lowercase before abbreviation expansion (so regex is easier)
    s = s.str.lower()

    # 6) Expand common abbreviations
    abbrev = {
        r"\bres\.?\b": "residences",
        r"\bapt\.?\b": "apartments",
        r"\bapts\.?\b": "apartments",
        r"\btwr\.?\b": "tower",
        r"\bintl\.?\b": "international",
    }
    for pat, repl in abbrev.items():
        s = s.str.replace(pat, repl, regex=True)

    # 7) Optional: drop leading "the" to reduce duplicates
    #    (If you want to keep official branding, comment this out)
    s = s.str.replace(r"^the\s+", "", regex=True)

    # 8) Final cleanup + Title Case (your part)
    s = (s
         .str.replace(r"\s+", " ", regex=True)
         .str.strip()
         .str.title()
    )

    # 9) Convert empty strings to NA
    s = s.replace("", pd.NA)

    return s

def decode_str(values: pd.Series) -> pd.Series:
    """
    Vectorized entrypoint: takes a pd.Series and returns a pd.Series.
    Behavior for string/None matches the original fix_value; missing values are preserved.
    """

    ARABIC_RE = re.compile(r"[\u0600-\u06FF]")
    CONTROL_CHARS_RE = re.compile(r"[\x00-\x1F\x7F-\x9F]")
    REPLACEMENT_CHAR = "\uFFFD"  # '�'

    # Matches common textual representations of control chars
    TEXTUAL_CONTROL_RE = re.compile(
        r"(?:"  # non-capturing group
        r"X(?P<x4>[0-9A-Fa-f]{4})"          # X0081
        r"|U\+(?P<uplus>[0-9A-Fa-f]{4})"    # U+0081
        r"|\\u(?P<uslash>[0-9A-Fa-f]{4})"   # \u0081
        r"|\\x(?P<x2>[0-9A-Fa-f]{2})"       # \x81
        r"|&#x(?P<html>[0-9A-Fa-f]{2,4});"  # &#x81; or &#x0081;
        r")"
    )

    URL_HEX_RE = re.compile(r"%[0-9A-Fa-f]{2}")
    MOJIBAKE_MARKERS = ("Ø", "Ù", "Ã", "Â")

    def strip_textual_controls(text: str) -> str:
        def repl(m: re.Match) -> str:
            gd = m.groupdict()
            hex_str = (
                gd.get("x4") or gd.get("uplus") or gd.get("uslash") or gd.get("x2") or gd.get("html")
            )
            # hex_str will always exist due to the regex
            codepoint = int(hex_str, 16)

            # remove only if it's a control char (C0 + DEL + C1)
            if codepoint <= 0x1F or codepoint == 0x7F or (0x80 <= codepoint <= 0x9F):
                return ""
            return m.group(0)

        return TEXTUAL_CONTROL_RE.sub(repl, text)

    def fix_mojibake(text: str) -> str:
        # Attempt 1: cp1252 bytes interpreted as utf-8
        try:
            cand = text.encode("cp1252").decode("utf-8")
            if ARABIC_RE.search(cand):
                return cand
        except Exception:
            pass

        # Attempt 2: patch common bad bytes then decode as utf-8
        try:
            b = bytearray(text.encode("cp1252", errors="ignore"))

            for i, x in enumerate(b):
                if x == 0xF8:      # ø
                    b[i] = 0xD8
                elif x == 0xF9:    # ù
                    b[i] = 0xD9

            for i in range(len(b) - 1):
                if b[i] == 0xD9 and b[i + 1] == 0x9A:
                    b[i + 1] = 0x8A

            cand = b.decode("utf-8", errors="replace")
            if ARABIC_RE.search(cand):
                return cand
        except Exception:
            pass

        return text

    def fix_value_scalar(value: Any) -> Any:
        if value is None:
            return None

        # Preserve pandas missing values (e.g., NaN / pd.NA) as-is
        if pd.isna(value):
            return value

        # Old code assumed strings; keep non-strings untouched to avoid surprises
        if not isinstance(value, str):
            return value

        out = value

        # 0) Remove textual placeholders like X0081 / \x81 / \u0081 / U+0081 / &#x81;
        out = strip_textual_controls(out)

        # 1) URL-encoded (e.g., %D8%...)
        if "%" in out and URL_HEX_RE.search(out):
            try:
                decoded = urllib.parse.unquote(out)
                if ARABIC_RE.search(decoded):
                    out = decoded
            except Exception:
                pass

        # 2) Mojibake (Ø Ù etc)
        if any(ch in out for ch in MOJIBAKE_MARKERS):
            out = fix_mojibake(out)

        # 3) Remove actual control chars (including U+0081 if present as the real char)
        out = CONTROL_CHARS_RE.sub("", out)

        # 4) Optional: remove the replacement char '�'
        out = out.replace(REPLACEMENT_CHAR, "")

        # 5) Optional: trim extra spaces created by removals
        out = re.sub(r"\s+", " ", out).strip()

        return out

    return values.map(fix_value_scalar)

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

def correct_free_text(col: pd.Series) -> pd.Series:
    # --- Load cache (fast + safe) ---
    if os.path.exists(CACHE_PATH) and os.path.getsize(CACHE_PATH) > 0:
        df_cache = pd.read_csv(
            CACHE_PATH,
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
            write_header = not (os.path.exists(CACHE_PATH) and os.path.getsize(CACHE_PATH) > 0)
            new_df.to_csv(CACHE_PATH, mode="a", header=write_header, index=False)

        # Map again using updated cache and fill misses
        out.loc[miss_idx] = miss_vals.astype("string").map(dict_cache).astype(object).values

    return out


















