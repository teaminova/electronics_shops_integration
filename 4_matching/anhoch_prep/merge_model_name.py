import pandas as pd

products = pd.read_csv("anhoch_products.csv")
names = pd.read_csv("anhoch_products_model_names.csv")

products["Model Name"] = names["Model Name"]

products.to_csv("anhoch_products_merged.csv", index=False)

print("Merged products with model names saved as 'anhoch_products_merged.csv'")
