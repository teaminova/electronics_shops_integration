import os
import pandas as pd
from groq import AsyncGroq
import asyncio
import random
from typing import Tuple

EXTRACTED_DATA_CSV = 'cleaned_file.csv'
OUTPUT_CSV = 'anhoch_names_final.csv'

CONCURRENT_BATCH_SIZE = 20
DELAY_BETWEEN_BATCHES_S = 2

MAX_RETRIES = 10
BASE_DELAY = 2
MAX_DELAY = 60
BACKOFF_MULTIPLIER = 2
JITTER_RANGE = 0.2

try:
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY environment variable not set.")
    client = AsyncGroq(api_key=api_key)
except Exception as e:
    print(f"Error initializing Groq client: {e}")
    exit()

def calculate_delay(attempt: int) -> float:
    delay = min(BASE_DELAY * (BACKOFF_MULTIPLIER ** attempt), MAX_DELAY)
    jitter = delay * JITTER_RANGE * random.random()
    return delay + jitter

def is_retryable_error(error: Exception) -> bool:
    error_str = str(error).lower()
    retryable_indicators = ['429', '500', '502', '503', '504', 'timeout', 'connection error', 'server error']
    return any(indicator in error_str for indicator in retryable_indicators)

async def get_model_name_with_retry(title: str, original_index: int) -> Tuple[int, str]:
    if pd.isna(title):
        return original_index, "No Title"

    prompt = f"""
        You are a highly precise product model name extractor. Your task is to extract the core model name from the product title.

        RULES:
        - Output ONLY the raw text of the model name.
        - Do NOT include any prefixes like "The model name is:".
        - Do NOT include explanations or quotation marks.

        EXAMPLES:
        1. Product Title: "Gigabyte GeForce RTX 4070 GAMING OC 12GB GDDR6X"
           Model Name: GeForce RTX 4070 GAMING OC
        2. Product Title: "CPU Intel Core i9-14900K 3.2GHz FC-LGA16A"
           Model Name: Core i9-14900K
        3. Product Title: "Samsung 55" The Frame Customisable Bezel, White VG-SCFT55WT/XC"
           Model Name: VG-SCFT55WT

        ---

        Product Title: "{title}"
        Model Name:
        """
    for attempt in range(MAX_RETRIES):
        try:
            chat_completion = await client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama3-8b-8192",
                temperature=0.0,
            )
            model_name = chat_completion.choices[0].message.content.strip()
            print(f"  [Row {original_index + 1}] Success: {title[:40]}... -> {model_name}")
            return original_index, model_name
        except Exception as e:
            if is_retryable_error(e):
                delay = calculate_delay(attempt)
                print(f"  [Row {original_index + 1}] Retryable error (attempt {attempt + 1}/{MAX_RETRIES}). Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
            else:
                print(f"  [Row {original_index + 1}] NON-RETRYABLE ERROR: {e}")
                return original_index, "Extraction Error"

    print(f"  [Row {original_index + 1}] FINAL ERROR after {MAX_RETRIES} retries.")
    return original_index, "Extraction Error - Retries Failed"

async def main():
    if not os.path.exists(EXTRACTED_DATA_CSV):
        print(f"Error: Input file '{EXTRACTED_DATA_CSV}' not found.")
        return

    if os.path.exists(OUTPUT_CSV):
        print(f"Resuming from existing file: '{OUTPUT_CSV}'")
        df = pd.read_csv(OUTPUT_CSV, low_memory=False)
    else:
        print(f"Starting a new extraction. Output will be saved to '{OUTPUT_CSV}'")
        df = pd.read_csv(EXTRACTED_DATA_CSV, low_memory=False)
        df['Model Name'] = '' # Initialize the new column

    pending_df = df[df['Model Name'].isnull() | (df['Model Name'] == '')].copy()

    if pending_df.empty:
        print("🎉 All model names have already been extracted. Nothing to do!")
        return

    total_pending = len(pending_df)
    print(f"Found {len(df)} total products. {total_pending} products are pending model name extraction.")

    for start in range(0, total_pending, CONCURRENT_BATCH_SIZE):
        end = min(start + CONCURRENT_BATCH_SIZE, total_pending)
        chunk_df = pending_df.iloc[start:end]

        print(f"\n--- Processing batch for pending items {start + 1}-{end} of {total_pending} ---")

        tasks = [get_model_name_with_retry(row['Title'], index) for index, row in chunk_df.iterrows()]
        results = await asyncio.gather(*tasks)

        for index, model_name in results:
            df.at[index, 'Model Name'] = model_name

        df.to_csv(OUTPUT_CSV, index=False)
        print(f"--- Batch complete. Progress saved. Cooling down for {DELAY_BETWEEN_BATCHES_S}s... ---")
        await asyncio.sleep(DELAY_BETWEEN_BATCHES_S)

    print("\n✅ All model names extracted. Your final dataset is ready!")

if __name__ == "__main__":
    asyncio.run(main())