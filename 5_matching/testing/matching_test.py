import csv
import json
import re
from collections import defaultdict


def normalize_text(text):
    if not isinstance(text, str):
        return ""
    return re.sub(r'[\W_]+', '', text).lower()


def load_products(filepath):
    products = []
    try:
        with open(filepath, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for i, row in enumerate(reader):
                try:
                    row['id'] = f"{filepath.split('.')[0]}-{i}"
                    row['extracted_specs'] = json.loads(row['extracted_specs'])
                    row['norm_model_name'] = normalize_text(row.get('Model Name', ''))
                    row['norm_category'] = normalize_text(row.get('Category', ''))
                    products.append(row)
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse JSON for row {i + 1} in {filepath}. Skipping.")
                except KeyError:
                    print(f"Warning: Missing expected column in row {i + 1} of {filepath}. Skipping.")
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
    return products


def calculate_spec_similarity(specs1, specs2):
    if not specs1 or not specs2:
        return 0.0

    all_keys = set(specs1.keys()) | set(specs2.keys())
    if not all_keys:
        return 1.0

    matching_values = 0
    for key in all_keys:
        val1 = specs1.get(key)
        val2 = specs2.get(key)

        if isinstance(val1, str):
            val1 = val1.lower().strip()
        if isinstance(val2, str):
            val2 = val2.lower().strip()

        if val1 == val2:
            matching_values += 1

    return matching_values / len(all_keys)


def match_products(products1, products2, spec_similarity_threshold=0.8):
    matches = []
    unmatched1 = list(products1)
    unmatched2 = list(products2)

    model_name_map = defaultdict(list)
    for product in unmatched2:
        model_name_map[product['norm_model_name']].append(product)

    still_unmatched1 = []

    for prod1 in unmatched1:
        matched_in_phase1 = False
        if prod1['norm_model_name'] in model_name_map:
            best_match_prod2 = None
            highest_score = -1

            potential_matches = model_name_map[prod1['norm_model_name']]

            for prod2 in potential_matches:
                if prod1['norm_category'] == prod2['norm_category']:
                    score = calculate_spec_similarity(prod1['extracted_specs'], prod2['extracted_specs'])
                    if score > highest_score:
                        highest_score = score
                        best_match_prod2 = prod2

            if best_match_prod2:
                matches.append({
                    "product1": prod1,
                    "product2": best_match_prod2,
                    "match_type": "Model Name & Category",
                    "score": highest_score
                })
                model_name_map[prod1['norm_model_name']].remove(best_match_prod2)
                matched_in_phase1 = True

        if not matched_in_phase1:
            still_unmatched1.append(prod1)

    still_unmatched2 = [prod for prods in model_name_map.values() for prod in prods]

    category_map = defaultdict(list)
    for product in still_unmatched2:
        category_map[product['norm_category']].append(product)

    matched_in_phase2_ids = set()

    for prod1 in still_unmatched1:
        potential_matches = category_map.get(prod1['norm_category'], [])

        best_match_prod2 = None
        highest_score = spec_similarity_threshold

        for prod2 in potential_matches:
            if prod2['id'] in matched_in_phase2_ids:
                continue

            score = calculate_spec_similarity(prod1['extracted_specs'], prod2['extracted_specs'])
            if score > highest_score:
                highest_score = score
                best_match_prod2 = prod2

        if best_match_prod2:
            matches.append({
                "product1": prod1,
                "product2": best_match_prod2,
                "match_type": "Category & Specs",
                "score": highest_score
            })
            matched_in_phase2_ids.add(best_match_prod2['id'])

    return matches


def write_matches_to_csv(matches, filepath):
    if not matches:
        print("No matches to write.")
        return

    fieldnames = [
        'product1_id', 'product1_model_name', 'product1_category',
        'product2_id', 'product2_model_name', 'product2_category',
        'match_type', 'score'
    ]

    try:
        with open(filepath, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            for match in matches:
                writer.writerow({
                    'product1_id': match['product1']['id'],
                    'product1_model_name': match['product1'].get('Model Name', ''),
                    'product1_category': match['product1'].get('Category', ''),
                    'product2_id': match['product2']['id'],
                    'product2_model_name': match['product2'].get('Model Name', ''),
                    'product2_category': match['product2'].get('Category', ''),
                    'match_type': match['match_type'],
                    'score': f"{match['score']:.2f}"
                })
        print(f"\nSuccessfully wrote {len(matches)} matches to {filepath}")
    except IOError as e:
        print(f"Error writing to CSV file: {e}")


if __name__ == "__main__":
    FILE1_PATH = 'products_anhoch.csv'
    FILE2_PATH = 'products_neptun.csv'
    OUTPUT_FILE_PATH = 'matches.csv'

    print("Loading products from files...")
    products_file1 = load_products(FILE1_PATH)
    products_file2 = load_products(FILE2_PATH)

    if products_file1 and products_file2:
        print(f"Loaded {len(products_file1)} products from {FILE1_PATH}")
        print(f"Loaded {len(products_file2)} products from {FILE2_PATH}")

        print("\nStarting product matching...")
        found_matches = match_products(products_file1, products_file2)
        print(f"Matching complete. Found {len(found_matches)} matches.")

        write_matches_to_csv(found_matches, OUTPUT_FILE_PATH)

