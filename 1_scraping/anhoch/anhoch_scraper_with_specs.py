import pandas as pd
from playwright.sync_api import sync_playwright
import time
from tqdm import tqdm

INPUT_CSV = "anhoch_products_basic_info.csv"
OUTPUT_CSV = "anhoch_products_with_specs.csv"
BATCH_SIZE = 200


def scrape_specifications(batch_df):
    specs = []

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=False)
        page = browser.new_page()

        for idx, row in tqdm(batch_df.iterrows(), total=len(batch_df), desc="Scraping specs"):
            url = row["Link"]
            try:
                page.goto(url, timeout=20000)
                page.wait_for_timeout(2000)
                page.wait_for_selector("div#description", timeout=10000)
                html = page.inner_html("div#description")
                specs.append(html.strip())
            except Exception:
                specs.append("")

        browser.close()

    return specs


def main():
    # Load existing CSV
    df = pd.read_csv(INPUT_CSV)

    # Make sure Specifications column exists
    if "Specifications" not in df.columns:
        df["Specifications"] = ""

    # Identify rows with missing or empty Specifications
    missing_specs_df = df[df["Specifications"].isnull() | (df["Specifications"].str.strip() == "")]
    total_missing = len(missing_specs_df)
    print(f"Total products with missing specs: {total_missing}")

    for start in range(0, total_missing, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total_missing)
        print(f"\nProcessing batch {start} to {end}...")

        batch_df = missing_specs_df.iloc[start:end]

        # Scrape and update specs
        new_specs = scrape_specifications(batch_df)
        df.loc[batch_df.index, "Specifications"] = new_specs

        # Save progress
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Batch {start}–{end} saved.\nSleeping 5 seconds before next batch...")
        time.sleep(5)

    print("\n✅ Done! All missing specs filled and saved to", OUTPUT_CSV)


if __name__ == "__main__":
    main()
