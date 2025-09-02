import os
import pandas as pd
from groq import AsyncGroq
import asyncio
import random
from typing import Tuple

# --- Configuration ---
# IMPORTANT: Make sure this points to your file with the partial results
INPUT_AND_OUTPUT_CSV = 'anhoch_products_categorized.csv'
CONCURRENT_BATCH_SIZE = 20
DELAY_BETWEEN_BATCHES_S = 120

# --- Retry Configuration ---
MAX_RETRIES = 10
BASE_DELAY = 2
MAX_DELAY = 60
BACKOFF_MULTIPLIER = 2
JITTER_RANGE = 0.2

# --- Initialization ---
try:
    # api_key = os.environ.get("GROQ_API_KEY")
    api_key = "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"
    if not api_key:
        raise ValueError("GROQ_API_KEY environment variable not set.")
    client = AsyncGroq(api_key=api_key)
except Exception as e:
    print(f"Error initializing Groq client: {e}")
    exit()


def is_retryable_error(error: Exception) -> bool:
    """Check if error is retryable (rate limits, timeouts, server errors)."""
    error_str = str(error).lower()
    retryable_indicators = ['429', '500', '502', '503', '504', 'timeout', 'connection error', 'server error']
    return any(indicator in error_str for indicator in retryable_indicators)

def calculate_delay(attempt: int) -> float:
    """Calculate delay with exponential backoff and jitter."""
    delay = min(BASE_DELAY * (BACKOFF_MULTIPLIER ** attempt), MAX_DELAY)
    jitter = delay * JITTER_RANGE * random.random()
    return delay + jitter

# (The helper functions calculate_delay, is_retryable_error, and get_category_with_retry remain exactly the same)

async def get_category_with_retry(title, specs, original_index) -> Tuple[int, str]:
    """Asynchronously categorizes a single product with retry logic."""
    if pd.isna(title) or pd.isna(specs):
        return original_index, "Unknown"

    prompt = f"""
    You are a precise product categorization assistant for tech products.
    Your task is to identify the most appropriate category for the given product based on its title and specifications.
    Provide only the single, most specific category name in your response (e.g., "Processor", "Motherboard", "Graphics Card", "RAM").

    Title: {title}
    Specifications: {specs}

    Category:
    """

    for attempt in range(MAX_RETRIES):
        try:
            chat_completion = await client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama3-8b-8192",
                temperature=0.0,
            )
            category = chat_completion.choices[0].message.content.strip()
            retry_info = f" (on attempt {attempt + 1})" if attempt > 0 else ""
            print(f"  [Row {original_index + 1}] Success{retry_info}: {title[:40]}... -> {category}")
            return original_index, category

        except Exception as e:
            if is_retryable_error(e):
                delay = calculate_delay(attempt)
                print(
                    f"  [Row {original_index + 1}] Retryable error (attempt {attempt + 1}/{MAX_RETRIES}). Retrying in {delay:.1f}s... Error: {e}")
                await asyncio.sleep(delay)
            else:
                print(f"  [Row {original_index + 1}] NON-RETRYABLE ERROR: {e}")
                return original_index, "Categorization Error"

    print(f"  [Row {original_index + 1}] FINAL ERROR after {MAX_RETRIES} retries.")
    return original_index, "Categorization Error"


async def main():
    """Main function that resumes categorization only for 'Categorization Error' entries."""

    # --- Load the existing output file ---
    if not os.path.exists(INPUT_AND_OUTPUT_CSV):
        print(f"❌ Error: The file '{INPUT_AND_OUTPUT_CSV}' was not found. Please check the filename.")
        return

    print(f"📂 Loading existing data from '{INPUT_AND_OUTPUT_CSV}'...")
    df = pd.read_csv(INPUT_AND_OUTPUT_CSV)

    # --- Filter only rows that have 'Categorization Error' ---
    pending_df = df[df['Category'].astype(str).str.strip().str.lower() == 'categorization error'].copy()

    if pending_df.empty:
        print("🎉 No products left with 'Categorization Error'. Nothing to fix!")
        return

    total_pending = len(pending_df)
    print(f"🔍 Found {len(df)} total products. {total_pending} need recategorization.")
    print(f"🚀 Running with {CONCURRENT_BATCH_SIZE} concurrent requests per batch.\n")

    for start in range(0, total_pending, CONCURRENT_BATCH_SIZE):
        end = min(start + CONCURRENT_BATCH_SIZE, total_pending)
        chunk_df = pending_df.iloc[start:end]

        print(f"--- Processing batch for rows {start + 1}-{end} of {total_pending} ---")

        tasks = [get_category_with_retry(row['Title'], row['Specs'], index) for index, row in chunk_df.iterrows()]
        results = await asyncio.gather(*tasks)

        for index, category in results:
            df.at[index, 'Category'] = category

        df.to_csv(INPUT_AND_OUTPUT_CSV, index=False)
        print(f"✅ Batch complete. Progress saved. Sleeping for {DELAY_BETWEEN_BATCHES_S}s...\n")
        await asyncio.sleep(DELAY_BETWEEN_BATCHES_S)

    print("🏁 All 'Categorization Error' entries reprocessed!")



if __name__ == "__main__":
    asyncio.run(main())