import pandas as pd 
import sys
import time 
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_ollama import ChatOllama  # pip install langchain-ollama

# -----------------------------
# CONFIG (edit these variables)
# -----------------------------
MODEL = "qwen2.5:latest" #  phi3.5:latest , qwen2.5:latest
BASE_URL = "http://localhost:11434"
SYSTEM_PROMPT = "You are a data engineer. Clean, correct, and standardize the following free-text value while preserving its meaning. Fix spelling/case, normalize whitespace/punctuation, expand obvious abbreviations, standardize dates/times/units, and remove noise. Normalize times to 12-hour format with AM/PM and include timezone if stated. Return only the cleaned text—no explanations, no labels, no quotes."

TEMPERATURE = 1
TIMEOUT_SECONDS = 120  # request timeout


if __name__ == "__main__":

    df_cache_llm = pd.read_csv("D:/whaw/case-study-real-estate/db/cache_llm.csv")#, nrows=3)
    df_cache_llm = df_cache_llm.astype(str)
    print(f"df_cache_llmn.shape {df_cache_llm.shape}")

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

    
    st = time.time()

    for idx, row in df_cache_llm.iterrows():
        # print(idx, row["best_time_to_call"])
        bttc = row["best_time_to_call"]
        result = chain.invoke({"user_input": bttc}).content
        df_cache_llm.loc[idx, "correction_best_time_to_call"] = result
        print(f"{idx+1}/884: {bttc} -> {result}")
        
    print(f"# Answered in {time.time() - st:.2f} sec.")
    
    df_cache_llm.to_csv("D:/whaw/case-study-real-estate/db/result_cache_llm.csv", index=False)
        
        