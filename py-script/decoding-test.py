# from urllib.parse import unquote

# # s = "Ù…Ù‚Ø·Ù… Ø§Ù„Ù…Ù‚Ø·Ù… Ø§Ù„Ø´Ø§Ø±Ø¹ Ø§Ù„Øªø¬Ø§Ø±Ù‰"
# s = "%D8%B1%D9%8A%D9%81%D8%B1%D8%B2 %D9%85%D9%86 %D8%Aa%D8%B7%D9%88%D9%8A%D8%B1 %D9%85%D8%B5%D8%B1"

# fixed = s.encode("cp1252").decode("utf-8", errors="ignore")   # or errors="replace"
# print(fixed)

import re 
import pandas as pd 
import urllib.parse
import codecs


def decode_arabic_with_corruption_fix(encoded_string):
    """
    Simple function to handle both URL-encoded and corrupted Arabic text.
    """
    # First, URL decode
    result = urllib.parse.unquote(encoded_string)
    
    # If we see mojibake patterns, fix them
    if any(c in result for c in ['Ù', 'Ø', 'ø']):
        try:
            # For the specific corruption: replace ø with correct byte
            # Convert to bytes assuming windows-1252, fix, then decode as UTF-8
            result = result.encode('windows-1252').replace(b'\xf8', b'\xd8').decode('utf-8')
        except:
            # Fallback: simple replacement for the specific corruption
            result = result.replace('ø¬', 'ج').replace('التø', 'الت')
    
    return result


def decode_str(encoded: pd.Series) -> pd.Series:
    """
    Handle both URL-encoded and corrupted Arabic text for a pandas Series.
    """
    out: list[object] = []
    
    for val in encoded.to_list():
        if pd.isna(val):
            out.append(pd.NA)
            continue

        text = str(val)
        result = urllib.parse.unquote(text)
        
        # Direct approach: Try to fix the specific corruption
        try:
            # For strings that look like Windows-1252 misinterpretation of UTF-8
            if any(c in result for c in ['Ù', 'Ø', 'ø', 'ù']):
                # Encode as Windows-1252 to get the original bytes
                bytes_data = result.encode('windows-1252')
                # Decode those bytes as UTF-8
                result = bytes_data.decode('utf-8')
        except Exception as e:
            # If it fails, keep the original
            pass
            
        out.append(result)
    
    return pd.Series(out, index=encoded.index, name=encoded.name)



ARABIC_RE = re.compile(r"[\u0600-\u06FF]")

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

def fix_value(s):
    if s is None:
        return None

    out = s

    # If it looks URL-encoded (e.g., %D8%...)
    if "%" in out and re.search(r"%[0-9A-Fa-f]{2}", out):
        try:
            decoded = urllib.parse.unquote(out)
            if ARABIC_RE.search(decoded):
                out = decoded
        except Exception:
            pass

    # If it looks like mojibake (Ø Ù etc)
    if any(ch in out for ch in ("Ø", "Ù", "Ã", "Â")):
        out = fix_mojibake(out)

    return out

# Test
test1 = "%D8%B1%D9%8A%D9%81%D8%B1%D8%B2 %D9%85%D9%86 %D8%AA%D8%B7%D9%88%D9%8A%D8%B1 %D9%85%D8%B5%D8%B1"
test2 = "Ù…Ù‚Ø·Ù… Ø§Ù„Ù…Ù‚Ø·Ù… Ø§Ù„Ø´Ø§Ø±Ø¹ Ø§Ù„Øªø¬Ø§Ø±Ù‰"
test3 = "Ù†ÙˆØ±Ø« Ø§ÙŠØ¯Ø¬ Ø§Ù„Ø¹Ù„Ù…ÙŠÙ† Ø§Ù„Ø¬Ø¯ÙŠØ¯Ø©"
test4 = "Ø´Ù‚Ù‚ Ùƒù…Ø¨ÙˆÙ†Ø¯ Øµù† Ùƒø§Ø¨Ùšøªø§Ù„ Ø¨Ø§Ù„Ø³Ø§Ø¯Ø³ Ù…Ù† Ø§ÙƒøªùˆØ¨Ø±"


with open('decoded_results.txt', 'w', encoding='utf-8') as f:
    f.write(f"{fix_value(test1)}\n") 
    f.write(f"{fix_value(test2)}\n") 
    f.write(f"{fix_value(test3)}\n") 
    f.write(f"{fix_value(test4)}\n") 
    
print("\nResults saved to 'decoded_results.txt'")