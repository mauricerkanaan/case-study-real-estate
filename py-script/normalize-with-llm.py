import pandas as pd
from itertools import islice
from typing import Iterable

df = pd.read_excel("D:/whaw/case-study-real-estate/data/Case_Study_Data___Data_Engineering__1_.xlsx", sheet_name="DE LEADS", nrows=1000)

def chunked(it: Iterable[str], size: int):
    it = iter(it)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


def normalize_column(s: pd.Series) -> pd.Series: 
    return s

def normalize_text_batch(values: list[str]) -> list[str]:
    print("normalize_text_batch", values)
    # your heavy batch method here
    return [str(v).strip().lower() for v in values]


col = "best_time_to_call"
batch_size = 10  # <-- your integer parameter

# 1) unique non-null values (keeps first-seen order)
unique_vals = df[col].dropna().unique().tolist()

# 2) call the heavy method in batches and build a mapping
norm_map = {}
for batch in chunked(unique_vals, batch_size):
    normalized = normalize_text_batch(batch)

    # safety checks (optional but recommended)
    if len(normalized) != len(batch):
        raise ValueError(
            f"Batch normalizer returned {len(normalized)} values for {len(batch)} inputs."
        )

    norm_map.update(dict(zip(batch, normalized)))

# 3) map back; nulls stay null
df[col + "_normalized"] = df[col].map(norm_map)
