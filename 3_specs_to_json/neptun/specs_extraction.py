import os
import pandas as pd
from groq import AsyncGroq
import asyncio
import json
import random
from typing import Tuple, Dict, Any

PREPROCESSED_CSV = 'products_preprocessed.csv'
SCHEMA_DIRECTORY = './schemas'
OUTPUT_CSV = 'products_extracted.csv'

CONCURRENT_BATCH_SIZE = 15
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


async def extract_data_with_retry(title: str, specs: str, schema: Dict[str, Any], original_index: int) -> Tuple[
    int, Dict[str, Any]]:
    if pd.isna(specs):
        return original_index, {"extraction_status": "skipped_no_specs"}

    prompt = f"""
    You are a data extraction assistant. Your task is to extract the specified information from the product's details and format it according to the provided JSON schema.
    Ensure the output is only the completed JSON and nothing else.
    Translate any values from Macedonian to English. If a value is not found, use null.

    Product Title: {title}
    Product Specifications: {specs}

    JSON Schema to fill:
    {json.dumps(schema, indent=4)}

    Extracted Data:
    """
    for attempt in range(MAX_RETRIES):
        try:
            chat_completion = await client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama3-8b-8192",
                response_format={"type": "json_object"},
            )
            extracted_json = json.loads(chat_completion.choices[0].message.content)
            extracted_json["extraction_status"] = "complete"
            print(f"  [Row {original_index + 1}] Success: Extracted data for {title[:40]}...")
            return original_index, extracted_json
        except Exception as e:
            if is_retryable_error(e):
                delay = calculate_delay(attempt)
                print(
                    f"  [Row {original_index + 1}] Retryable error (attempt {attempt + 1}/{MAX_RETRIES}). Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
            else:
                print(f"  [Row {original_index + 1}] NON-RETRYABLE ERROR: {e}")
                return original_index, {"extraction_status": "error"}

    print(f"  [Row {original_index + 1}] FINAL ERROR after {MAX_RETRIES} retries.")
    return original_index, {"extraction_status": "error_retries_failed"}


async def main():
    if not os.path.exists(SCHEMA_DIRECTORY):
        print(f"Error: Schema directory '{SCHEMA_DIRECTORY}' not found.")
        return
    schemas = {}
    for filename in os.listdir(SCHEMA_DIRECTORY):
        if filename.endswith('.json'):
            category_name = filename.replace('schema_', '').replace('.json', '').replace('_', ' ')
            with open(os.path.join(SCHEMA_DIRECTORY, filename), 'r') as f:
                schemas[category_name] = json.load(f)
    print(f"Loaded {len(schemas)} schemas.")

    if not os.path.exists(PREPROCESSED_CSV):
        print(f"Error: Preprocessed CSV '{PREPROCESSED_CSV}' not found.")
        return

    source_df = pd.read_csv(PREPROCESSED_CSV)

    if os.path.exists(OUTPUT_CSV):
        print(f"Resuming from existing file: '{OUTPUT_CSV}'")
        df = pd.read_csv(OUTPUT_CSV)
    else:
        print(f"Starting a new extraction. Output will be saved to '{OUTPUT_CSV}'")
        df = source_df.copy()
        df['extraction_status'] = ''
        df['extracted_specs'] = ''

    pending_df = df[df['extraction_status'] != 'complete'].copy()

    if pending_df.empty:
        print("🎉 All products have already been processed. Nothing to do!")
        return

    total_pending = len(pending_df)
    print(f"Found {len(df)} total products. {total_pending} products are pending extraction.")

    for start in range(0, total_pending, CONCURRENT_BATCH_SIZE):
        end = min(start + CONCURRENT_BATCH_SIZE, total_pending)
        chunk_df = pending_df.iloc[start:end]

        print(f"\n--- Processing batch for pending items {start + 1}-{end} of {total_pending} ---")

        tasks = []
        for index, row in chunk_df.iterrows():
            category = row['Category']
            if pd.notna(category) and category in schemas:
                tasks.append(extract_data_with_retry(row['Title'], row['Specs'], schemas[category], index))
            else:
                df.at[index, 'extraction_status'] = 'skipped_no_schema'

        if not tasks:
            print("--- No tasks to run in this batch (all items skipped). ---")
            continue

        batch_results = await asyncio.gather(*tasks)

        for index, extracted_data in batch_results:
            if extracted_data:
                status = extracted_data.pop('extraction_status', 'error')
                df.at[index, 'extraction_status'] = status

                df.at[index, 'extracted_specs'] = json.dumps(extracted_data)
            else:
                df.at[index, 'extraction_status'] = 'error'

        df.to_csv(OUTPUT_CSV, index=False)
        print(f"--- Batch complete. Progress saved. Cooling down for {DELAY_BETWEEN_BATCHES_S}s... ---")
        await asyncio.sleep(DELAY_BETWEEN_BATCHES_S)

    print("\n✅ All product data extracted successfully!")


if __name__ == "__main__":
    asyncio.run(main())