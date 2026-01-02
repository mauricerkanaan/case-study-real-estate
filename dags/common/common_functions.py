import re
import pandas as pd 
import urllib.parse

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

def decode_str(encoded: pd.Series) -> pd.Series:
    """
    Handle both URL-encoded and corrupted Arabic text for a pandas Series.

    - URL-decodes each non-null value
    - Fixes common mojibake/corruption patterns when detected
    - Preserves index and name
    """
    out: list[object] = []

    for val in encoded.to_list():
        if pd.isna(val):
            out.append(pd.NA)
            continue

        text = str(val)
        result = urllib.parse.unquote(text)

        if ("Ù" in result) or ("Ø" in result) or ("ø" in result):
            try:
                raw_bytes = result.encode("windows-1252")
                fixed_bytes = raw_bytes.replace(b"\xf8", b"\xd8")
                result = fixed_bytes.decode("utf-8")
            except Exception:
                result = result.replace("ø¬", "ج")
                result = result.replace("التø", "الت")

        out.append(result)

    return pd.Series(out, index=encoded.index, name=encoded.name)


ARABIC_RE = re.compile(r"[\u0600-\u06FF]")
CONTROL_CHARS_RE = re.compile(r"[\x00-\x1F\x7F-\x9F]")
REPLACEMENT_CHAR = "\uFFFD"  # '�'


def fix_mojibake(s: str) -> str:
    # Try the common fix first
    try:
        cand = s.encode("cp1252").decode("utf-8")
        if ARABIC_RE.search(cand):
            return cand
    except Exception:
        pass

    # Extra repair for cases like your samples (ø/ù used instead of Ø/Ù)
    try:
        b = bytearray(s.encode("cp1252", errors="ignore"))
        for i, x in enumerate(b):
            if x == 0xF8:  # ø
                b[i] = 0xD8
            elif x == 0xF9:  # ù
                b[i] = 0xD9

        # Optional small tweak seen in your text (ي)
        for i in range(len(b) - 1):
            if b[i] == 0xD9 and b[i + 1] == 0x9A:
                b[i + 1] = 0x8A

        cand = b.decode("utf-8", errors="replace")
        if ARABIC_RE.search(cand):
            return cand
    except Exception:
        pass

    return s

def fix_value(s: str | None) -> str | None:
    if s is None:
        return None

    out = s

    # 1) URL-encoded (e.g., %D8%...)
    if "%" in out and re.search(r"%[0-9A-Fa-f]{2}", out):
        try:
            decoded = urllib.parse.unquote(out)
            # accept if it actually turned into Arabic (or just accept always if you prefer)
            if ARABIC_RE.search(decoded):
                out = decoded
        except Exception:
            pass

    # 2) Mojibake (Ø Ù etc)
    if any(ch in out for ch in ("Ø", "Ù", "Ã", "Â")):
        out = fix_mojibake(out)

    # 3) Remove control chars like U+0081 (shown as \x81 / X0081 in some systems)
    out = CONTROL_CHARS_RE.sub("", out)

    # 4) Optional: remove the replacement char '�'
    # Note: '�' means data was already lost; removing it just cleans display.
    out = out.replace(REPLACEMENT_CHAR, "")

    # 5) Optional: trim extra spaces created by removals
    out = re.sub(r"\s+", " ", out).strip()

    return out



















