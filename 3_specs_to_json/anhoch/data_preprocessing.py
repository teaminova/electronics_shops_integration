import pandas as pd
import os

# --- Configuration ---
# The file from your categorization step
INPUT_CSV = 'anhoch_products.csv'
# The new file where the cleaned data will be saved
OUTPUT_CSV = 'anhoch_products_preprocessed.csv'

# --- Category Mapping ---
# A dictionary where the key is the old category and the value is the new one
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
    'Sunglasses': 'Eyewear',
    'Flash Drive': 'USB Flash Drive',
    'Mobile Phone': 'Smartphone',
    'Inverter': 'Air Conditioner',
    'Water Heater': 'Boiler',
    'Desktop Graphics Card': 'Graphics Card',
    'E-Reader': 'E-Book Reader',
    'E-reader': 'E-Book Reader',
    'Video Game': 'Game',
    'SSD': 'SSD (Solid State Drive)',
    'SSD (Solid-State Drive)': 'SSD (Solid State Drive)',
    'HDD': 'HDD (Hard Disk Drive)',
    'UPS': 'UPS (Uninterruptible Power Supply)',
    'Power Supply (PSU)': 'PSU (Power Supply)',
    'Power Supply': 'PSU (Power Supply)',
    'CASE': 'Case',
    'CPU Air Cooler': 'CPU Cooler',
    'Cooler': 'CPU Cooler',
    '3D Printing Filament': '3D Printer Filament',
    'Audio/Video Accessory': 'Audio/Video Accessories',
    'Battery': 'Batteries',
    'Binocular': 'Binoculars',
    'Camera Accessory': 'Camera Accessories',
    'Carry Case': 'Carrying Case',
    'Controller Accessory': 'Controller Accessories',
    'Digital Game': 'Game',
    'Digital Photo Frame': 'Digital Picture Frame',
    'Gamepad': 'Game Pad',
    'Headphone': 'Headphones',
    'Headphone Stand': 'Headphones Stand',
    'Monitor Accessory': 'Monitor Accessories',
    'Peripheral': 'Peripherals',
    'Wearable': 'Wearable Device',
    'Webcam': 'Web Camera',
    'Wireless Access Point': 'Wireless Access Point (WAP)',
}


def preprocess_categories():
    """
    Loads the categorized data, standardizes the category names based on the mapping,
    and saves the result to a new file.
    """
    # Check if the input file exists
    if not os.path.exists(INPUT_CSV):
        print(f"Error: The input file '{INPUT_CSV}' was not found.")
        print("Please make sure the file from the categorization step is in the same directory.")
        return

    # Load the dataset
    print(f"Loading data from '{INPUT_CSV}'...")
    df = pd.read_csv(INPUT_CSV)

    # Use the .replace() method on the 'Category' column with our mapping
    print("Standardizing category names...")
    df['Category'] = df['Category'].replace(category_mapping)

    # Save the cleaned DataFrame to a new CSV file
    df.to_csv(OUTPUT_CSV, index=False)

    print(f"\nPreprocessing complete!")
    print(f"Cleaned data has been saved to '{OUTPUT_CSV}'.")
    print("You can now use this file for the next steps, like schema generation.")


if __name__ == "__main__":
    preprocess_categories()