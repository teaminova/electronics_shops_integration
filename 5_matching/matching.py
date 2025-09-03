import pandas as pd
import re
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import json


def clean_text(text):
    if pd.isnull(text):
        return ""
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)
    return re.sub(r'\s+', ' ', text).strip()


def flatten_json_for_text(data):
    parts = []
    if isinstance(data, dict):
        for key in sorted(data.keys()):
            value = data[key]
            parts.append(str(key))
            parts.extend(flatten_json_for_text(value))
    elif isinstance(data, list):
        for item in data:
            parts.extend(flatten_json_for_text(item))
    elif pd.notnull(data) and str(data).strip() != "":
        parts.append(str(data))
    return parts


def json_to_representative_string(json_obj):
    if not isinstance(json_obj, dict):
        if pd.isnull(json_obj) or str(json_obj).strip() == "":
            return ""
        if isinstance(json_obj, list):
            flat_parts = flatten_json_for_text(json_obj)
            return " ".join(part for part in flat_parts if part)
        return str(json_obj)

    flat_parts = flatten_json_for_text(json_obj)
    return " ".join(part for part in flat_parts if part)


def preprocess_dataframe(df):
    df["Title_clean"] = df["Title"].astype(str).apply(clean_text)

    def parse_and_convert_specs(spec_str):
        if pd.isnull(spec_str) or not str(spec_str).strip():
            return ""
        try:
            json_obj = json.loads(str(spec_str))
            return json_to_representative_string(json_obj)
        except json.JSONDecodeError:
            return str(spec_str)

    df["Specs_for_embedding"] = df["extracted_specs"].apply(parse_and_convert_specs)
    df["Specs_clean"] = df["Specs_for_embedding"].apply(clean_text)
    return df


def have_no_shared_model_tokens(spec1_clean_str, spec2_clean_str):
    regex = r'\b[a-zA-Z0-9]*\d[a-zA-Z0-9]*\b'

    tokens1 = set(re.findall(regex, spec1_clean_str.lower()))
    tokens2 = set(re.findall(regex, spec2_clean_str.lower()))

    return len(tokens1 & tokens2) == 0


def match_products(file1, file2, output_file, top_k=3, title_threshold=0.5, spec_threshold=0.8):
    try:
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
    except FileNotFoundError as e:
        print(f"Error: Input file not found. {e}")
        return

    print(f"Processing {file1} (Store 1) with {len(df1)} products.")
    print(f"Processing {file2} (Store 2) with {len(df2)} products.")

    df1 = preprocess_dataframe(df1.copy())
    df2 = preprocess_dataframe(df2.copy())

    print("Loading sentence transformer model...")
    model = SentenceTransformer("all-MiniLM-L6-v2")

    print("Generating title embeddings for Store 1...")
    title_embeddings1 = model.encode(df1["Title_clean"].tolist(), show_progress_bar=True, batch_size=128)
    print("Generating title embeddings for Store 2...")
    title_embeddings2 = model.encode(df2["Title_clean"].tolist(), show_progress_bar=True, batch_size=128)

    print("Calculating title similarity matrix...")
    title_similarity_matrix = cosine_similarity(title_embeddings1, title_embeddings2)

    print("Generating spec embeddings for Store 1 (can take time)...")
    spec_embeddings1 = model.encode(df1["Specs_clean"].tolist(), show_progress_bar=True, batch_size=128)
    print("Generating spec embeddings for Store 2 (can take time)...")
    spec_embeddings2 = model.encode(df2["Specs_clean"].tolist(), show_progress_bar=True, batch_size=128)

    matched_store2_indices = set()
    merged_rows = []

    print(
        f"\nStarting product matching (top_k={top_k}, title_thresh={title_threshold}, spec_thresh={spec_threshold})...")
    for i in range(len(df1)):
        candidate_indices = np.argsort(title_similarity_matrix[i])[-top_k:][::-1]

        best_match_for_product_i = None
        highest_overall_score = -1

        for j in candidate_indices:
            if j in matched_store2_indices:
                continue

            current_title_score = title_similarity_matrix[i][j]

            if current_title_score < title_threshold:
                break

            spec_embedding1 = spec_embeddings1[i]
            spec_embedding2 = spec_embeddings2[j]

            current_spec_score = cosine_similarity([spec_embedding1], [spec_embedding2])[0][0]

            specs1_clean_str = df1.iloc[i]["Specs_clean"]
            specs2_clean_str = df2.iloc[j]["Specs_clean"]

            if current_spec_score < spec_threshold:
                continue

            if len(specs1_clean_str) > 10 and len(specs2_clean_str) > 10 and \
                    have_no_shared_model_tokens(specs1_clean_str, specs2_clean_str):
                continue

            combined_score = (current_title_score + current_spec_score) / 2.0

            if combined_score > highest_overall_score:
                highest_overall_score = combined_score
                best_match_for_product_i = {
                    'index_store2': j,
                    'title_sim': current_title_score,
                    'spec_sim': current_spec_score
                }

        if best_match_for_product_i is not None:
            idx_store2 = best_match_for_product_i['index_store2']
            matched_store2_indices.add(idx_store2)

            merged_rows.append({
                "Title_Store1": df1.iloc[i]["Title"],
                "Title_Store2": df2.iloc[idx_store2]["Title"],
                "Price_Store1": df1.iloc[i].get("Price"),
                "Price_Store2": df2.iloc[idx_store2].get("Price"),
                "HappyPrice_Store1": df1.iloc[i].get("HappyPrice"),
                "HappyPrice_Store2": df2.iloc[idx_store2].get("HappyPrice"),
                "Link_Store1": df1.iloc[i]["Link"],
                "Link_Store2": df2.iloc[idx_store2]["Link"],
                "Specs_JSON_Store1": df1.iloc[i]["extracted_specs"],
                "Specs_JSON_Store2": df2.iloc[idx_store2]["extracted_specs"],
                # "Specs_Cleaned_Store1": df1.iloc[i]["Specs_clean"], # For debugging
                # "Specs_Cleaned_Store2": df2.iloc[idx_store2]["Specs_clean"], # For debugging
                "Image_Store1": df1.iloc[i].get("Image", df1.iloc[i].get("Image Src")),
                "Image_Store2": df2.iloc[idx_store2].get("Image", df2.iloc[idx_store2].get("Image Src")),
                "Title_Similarity_Score": round(best_match_for_product_i['title_sim'], 4),
                "Specs_Similarity_Score": round(best_match_for_product_i['spec_sim'], 4)
            })
        if (i + 1) % 500 == 0:
            print(f"Processed {i + 1}/{len(df1)} products from Store 1. Found {len(merged_rows)} matches so far.")

    results_df = pd.DataFrame(merged_rows)
    results_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\nMatching complete. {len(results_df)} products matched.")
    print(f"Merged output saved to: {output_file}")


if __name__ == "__main__":
    file1_path = "products_anhoch.csv"
    file2_path = "products_neptun.csv"
    output_path = "matched_products.csv"

    match_products(
        file1_path,
        file2_path,
        output_path,
        top_k=5,
        title_threshold=0.5,
        spec_threshold=0.8
    )
    print("Script finished.")