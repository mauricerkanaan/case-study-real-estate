#!/usr/bin/env python3
"""
ollama_langchain_no_history_vars.py

A minimal LangChain + Ollama local script (NO chat history) where all arguments
are set as variables inside the script.

Requirements:
  - Ollama running locally (default: http://localhost:11434)
  - A model pulled in Ollama, e.g.: ollama pull llama3
  - pip install langchain langchain-community
    (or pip install langchain-ollama, depending on your setup)
"""

# -----------------------------
# CONFIG (edit these variables)
# -----------------------------
MODEL = "qwen2.5:latest" #  phi3.5:latest , qwen2.5:latest
BASE_URL = "http://localhost:11434"

# SYSTEM_PROMPT = "You are a helpful assistant. Be concise."
# USER_INPUT = "Explain what LangChain is in 2 sentences."
# USER_INPUT = "What is the capital of Lebanon? Answer without any explanation!"

# SYSTEM_PROMPT = "Act as a **data engineer**. Clean and standardize the real-estate field **“best time to call”** from messy free text into a consistent **single free-text** format. Correct it and **Return ONLY the standardized values, one per line, in the same order, with no extra text.**"
SYSTEM_PROMPT = "You are a data engineer. Clean, correct, and standardize the following free-text value while preserving its meaning. Fix spelling/case, normalize whitespace/punctuation, expand obvious abbreviations, standardize dates/times/units, and remove noise. Normalize times to 12-hour format with AM/PM and include timezone if stated. Return only the cleaned text—no explanations, no labels, no quotes."

TEMPERATURE = 0.2
TIMEOUT_SECONDS = 120  # request timeout

# -----------------------------
# IMPLEMENTATION
# -----------------------------
import sys
import time 
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate


# Prefer the newer dedicated package if available; fall back to community integration.
from langchain_ollama import ChatOllama  # pip install langchain-ollama


def main() -> int:
    best_time_to_call = ["All day , any day", "12 oâ€™clock", "Evenings from 7 to 12 pm", "13-11-2023, 9:45 PM", "from 3pm till 9pm", "After 7:00 pm", "from  12 noon", "12:00:00", "12-12-2023, 6:00 PM", "After 3.00 pm"]

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

    try:
        st = time.time()

        for b in best_time_to_call: 
            USER_INPUT = f"{b}"
            result = chain.invoke({"user_input": USER_INPUT})
            print("input\n", b)
            print("output\n", result.content)
            
        print(f"# Answered in {time.time() - st:.2f} sec.")
        
        
        return 0
    except Exception as e:
        print(f"Error calling Ollama via LangChain: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
