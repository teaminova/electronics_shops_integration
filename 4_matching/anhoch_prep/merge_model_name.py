import pandas as pd

# Load both CSVs
products = pd.read_csv("anhoch_products.csv")
names = pd.read_csv("anhoch_products_model_names.csv")

# Add Model Name column directly (row alignment assumed)
products["Model Name"] = names["Model Name"]

# Save new CSV
products.to_csv("anhoch_products_merged.csv", index=False)

print("✅ Model Name column added (row-wise) and saved as 'anhoch_products_merged.csv'")
