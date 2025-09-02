import os
import pandas as pd
from groq import Groq
import json
import time

CATEGORIZED_PRODUCTS_CSV = 'anhoch_products_preprocessed.csv'
SCHEMA_OUTPUT_DIRECTORY = './schemas'

try:
    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
except Exception as e:
    print(f"Error initializing Groq client: {e}")
    exit()


def generate_schema_for_category(category):
    """
    Uses Llama 3 to generate a JSON schema for a given product category.
    """

    prompt = f"""
        You are a data schema generator for tech products. Your task is to generate a simple, flat JSON object that lists the most important and common specifications for the given product category.

        **Rules:**
        - The output must be only the raw JSON object. No explanations or markdown.
        - All keys must be in `snake_case`.
        - The value for each key must be an empty string.

        **Example for "Monitor":**
        ```json
        {{
            "brand": "",
            "model": "",
            "screen_size": "",
            "resolution": "",
            "aspect_ratio": "",
            "panel_type": "",
            "refresh_rate": "",
            "response_time": ""
        }}
        ```

        **Category to process:** "{category}"

        **JSON Output:**
        """
    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model="llama3-70b-8192",
            temperature=0.1,
        )

        response_text = chat_completion.choices[0].message.content.strip()

        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]

        return json.loads(response_text)
    except Exception as e:
        print(f"An API or JSON parsing error occurred for category '{category}': {e}")
        return None


def main():
    if not os.path.exists(CATEGORIZED_PRODUCTS_CSV):
        print(f"Error: The input file '{CATEGORIZED_PRODUCTS_CSV}' was not found.")
        print("Please run Step 1 first.")
        return

    if not os.path.exists(SCHEMA_OUTPUT_DIRECTORY):
        os.makedirs(SCHEMA_OUTPUT_DIRECTORY)

    df = pd.read_csv(CATEGORIZED_PRODUCTS_CSV)
    unique_categories = df['Category'].unique()

    print(f"Found unique categories: {list(unique_categories)}")

    for category in unique_categories:
        if pd.isna(category) or category in ["Unknown", "Categorization Error"]:
            continue

        print(f"Generating schema for category: {category}...")
        schema = generate_schema_for_category(category)

        if schema:
            filename = f"schema_{category.replace(' ', '_').replace('/', '_')}.json"
            filepath = os.path.join(SCHEMA_OUTPUT_DIRECTORY, filename)
            with open(filepath, 'w') as f:
                json.dump(schema, f, indent=4)
            print(f"-> Schema saved to {filepath}")

        time.sleep(1)

    print("\nSchema generation complete!")


if __name__ == "__main__":
    main()