# 2026.07.08 9.00
import os
import dlt
import requests
import pandas as pd

AWIN_API_TOKEN = "YOUR_AWIN_BEARER_TOKEN_HERE"
PUBLISHER_ID = "YOUR_AWIN_PUBLISHER_ID_HERE"
POSTGRES_DSN = "postgresql://user:pass@localhost:5432/niche_products_db"

def fetch_and_filter_awin(target_subcategories: list):
    """
    Fetches the full Awin feed and uses Pandas to filter out everything 
    except the chosen subcategories before database ingestion.
    """
    print("Requesting full data block from Awin API...")
    url = f"https://awin.com{PUBLISHER_ID}/awinfeeds"
    headers = {"Authorization": f"Bearer {AWIN_API_TOKEN}", "Accept": "application/json"}
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"API Error: {response.text}")
        
    # Load everything into a temporary Pandas DataFrame
    raw_df = pd.DataFrame(response.json())
    
    # --- The Energy Saving Filter Step ---
    print(f"Filtering data for subcategories: {target_subcategories}")
    
    # We create a regex string from your list, e.g., "trench coat|dress|jacket"
    filter_regex = "|".join(target_subcategories)
    
    # Filter looking at the category column case-insensitively
    filtered_df = raw_df[
        raw_df['merchant_category'].str.contains(filter_regex, case=False, na=False) |
        raw_df['title'].str.contains(filter_regex, case=False, na=False)
    ]
    
    print(f"Filter Complete. Kept {len(filtered_df)} items out of {len(raw_df)} raw entries.")
    return filtered_df

def run_filtered_pipeline():
    # 1. Define exactly what your lifestyle influencer is promoting this week
    my_niche_subcategories = ["trench coat", "linen dress", "sunglasses", "blazer"]
    
    # Get the cleaned, highly targeted dataframe
    clean_dataframe = fetch_and_filter_awin(my_niche_subcategories)
    
    if clean_dataframe.empty:
        print("No matching products found for those subcategories today.")
        return

    # 2. Let dlt manage the pipeline transaction
    pipeline = dlt.pipeline(
        pipeline_name="filtered_awin_sync",
        destination=dlt.destinations.postgres(credentials=POSTGRES_DSN),
        dataset_name="public"
    )
    
    # dlt checks your database and only updates prices/new items seamlessly
    load_info = pipeline.run(
        clean_dataframe,
        table_name="products",
        write_disposition="merge",
        primary_key="id"
    )
    print(f"Success! Synced to PostgreSQL: {load_info}")

if __name__ == "__main__":
    run_filtered_pipeline()
