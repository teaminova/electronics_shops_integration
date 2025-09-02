import pandas as pd

df = pd.read_csv("anhoch_products_categorized.csv")

df_cleaned = df.dropna(subset=['Specs'])

df_cleaned.to_csv("anhoch_products_model_names.csv", index=False)

print(f"✅ Cleaned dataset saved. {len(df) - len(df_cleaned)} rows with null 'Specs' were removed.")
