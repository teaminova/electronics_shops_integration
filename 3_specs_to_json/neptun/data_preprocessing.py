import pandas as pd
import os

INPUT_CSV = 'neptun_products_categorized.csv'
OUTPUT_CSV = 'products_preprocessed.csv'


category_mapping = {
    'Television': 'TV',
    'Headset': 'Headphones',
    'Bluetooth Speaker': 'Speaker',
    'Speakers': 'Speaker',
    'Mount': 'TV Mount',
    'Mounting Bracket': 'TV Mount',
    'Mounting Arm': 'TV Mount',
    'Separator': 'Distribution Frame',
    'SAS Cable': 'Distribution Frame',
    'Chair': 'Gaming Chair',
    'Mouse Pad': 'Mousepad',
    'CPU': 'Processor',
    'Power Supply (PSU)': 'Power Supply',
    'Sunglasses': 'Eyewear',
    'Flash Drive': 'USB Flash Drive',
    'Mobile Phone': 'Smartphone',
    'Inverter': 'Air Conditioner',
    'Water Heater': 'Boiler',
    'Desktop Graphics Card': 'Graphics Card',
}


def preprocess_categories():
    if not os.path.exists(INPUT_CSV):
        print(f"Error: The input file '{INPUT_CSV}' was not found.")
        print("Please make sure the file from the categorization step is in the same directory.")
        return

    print(f"Loading data from '{INPUT_CSV}'...")
    df = pd.read_csv(INPUT_CSV)

    print("Standardizing category names...")
    df['Category'] = df['Category'].replace(category_mapping)

    df.to_csv(OUTPUT_CSV, index=False)

    print(f"\nPreprocessing complete!")
    print(f"Cleaned data has been saved to '{OUTPUT_CSV}'.")
    print("You can now use this file for the next steps, like schema generation.")


if __name__ == "__main__":
    preprocess_categories()