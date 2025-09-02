import os
import pandas as pd
from groq import AsyncGroq
import asyncio
import json
import random
from typing import Tuple, Dict, Any

# --- Configuration ---
PREPROCESSED_CSV = 'anhoch_products_preprocessed.csv'
SCHEMA_DIRECTORY = './schemas'
OUTPUT_CSV = 'anhoch_products_extracted_specs.csv'

CONCURRENT_BATCH_SIZE = 15
DELAY_BETWEEN_BATCHES_S = 60

# --- Retry Configuration ---
MAX_RETRIES = 10
BASE_DELAY = 2
MAX_DELAY = 60
BACKOFF_MULTIPLIER = 2
JITTER_RANGE = 0.2

# --- Initialization ---
try:
    api_key = "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"
    if not api_key:
        raise ValueError("GROQ_API_KEY environment variable not set.")
    client = AsyncGroq(api_key=api_key)
except Exception as e:
    print(f"Error initializing Groq client: {e}")
    exit()


# --- Helper & Core Extraction Functions (No Changes Here) ---

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
    Ensure the output is only the completed JSON and nothing else and be careful to use the escape symbol when having inches in the values (\").
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


# --- Main Application Logic (With Modifications) ---
async def main():
    """Main function to load data, manage state, and run extraction concurrently."""
    # 1. Load Schemas
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

    # 2. Prepare DataFrame and Handle Resume Logic
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
        # MODIFICATION 1: Add the new columns for status and the JSON string
        df['extraction_status'] = ''
        df['extracted_specs'] = ''

    # 3. Identify Pending Work
    pending_df = df[df['extraction_status'] != 'complete'].copy()

    if pending_df.empty:
        print("🎉 All products have already been processed. Nothing to do!")
        return

    total_pending = len(pending_df)
    print(f"Found {len(df)} total products. {total_pending} products are pending extraction.")

    # 4. Process in Batches
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

        # 5. MODIFICATION 2: Update DataFrame and Save Progress
        for index, extracted_data in batch_results:
            await asyncio.sleep(1)
            if extracted_data:
                # Pop the status from the dict to keep the JSON clean
                status = extracted_data.pop('extraction_status', 'error')
                df.at[index, 'extraction_status'] = status

                # Convert the remaining dictionary to a JSON string and save it
                df.at[index, 'extracted_specs'] = json.dumps(extracted_data)
            else:
                # Handle cases where the extraction task itself failed and returned None
                df.at[index, 'extraction_status'] = 'error'

        df.to_csv(OUTPUT_CSV, index=False)
        print(f"--- Batch complete. Progress saved. Cooling down for {DELAY_BETWEEN_BATCHES_S}s... ---")
        await asyncio.sleep(DELAY_BETWEEN_BATCHES_S)

    print("\n✅ All product data extracted successfully!")


if __name__ == "__main__":
    asyncio.run(main())