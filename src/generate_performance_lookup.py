import pandas as pd
import sqlite3
import openai
from fuzzywuzzy import process
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import logging
from typing import Set
from dotenv import load_dotenv
import requests
from requests.exceptions import RequestException

# Import functions from query_dmp.py
from query_dmp import get_auth_token

# Load environment variables
load_dotenv('/Users/adamhunter/miniconda3/envs/ragdev/ragdev.env')

# Set up OpenAI API key
openai.api_key = os.environ.get("OPENAI_API_KEY")
TTD_USERNAME = os.getenv('TTD_USERNAME')
TTD_PASS = os.getenv('TTD_PASS')
PARTNER_ID = os.getenv('PARTNER_ID')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_all_advertiser_names(token: str, partner_id: str) -> Set[str]:
    """Retrieve all advertiser names."""
    url = f"https://api.thetradedesk.com/v3/overview/partner/{partner_id}"
    headers = {"Content-Type": "application/json", "TTD-Auth": token}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        partner_overview = response.json()
        
        all_advertiser_ids = set()
        for advertiser in partner_overview.get("Advertisers", []):
            all_advertiser_ids.add(advertiser["AdvertiserName"])
        
        return all_advertiser_ids
    except RequestException as e:
        logging.error(f"Failed to get advertiser names: {e}")
        raise

def load_categorizations(file_path):
    """Load the CSV file with advertiser categorizations."""
    return pd.read_csv(file_path)

def get_top_matches(name, choices, n=10):
    return process.extract(name, choices, limit=n)

def llm_choose_match(advertiser_name, top_matches):
    matches_str = "\n".join([f"{match[0]} (Score: {match[1]})" for match in top_matches])
    prompt = f"""
    Given the advertiser name "{advertiser_name}", choose the best matching company from the following list:
    {matches_str}

    Respond with only the exact company name you've chosen, no explanation or anything else.
    If none of the options seem like a good match, respond with "No match".
    """


    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that matches advertisers to companies."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=50,
        n=1,
        stop=None,
        temperature=0,
    )
    chosen_match = response.choices[0].message.content.strip()
    return chosen_match if chosen_match != "No match" else None

def categorize_advertiser(advertiser_name, categories, df_lookup, columns_to_check):
    def fetch_top_matches(column):
        return get_top_matches(advertiser_name, df_lookup[column].unique())

    # First, try to find a match using the LLM in parallel
    with ThreadPoolExecutor() as executor:
        future_to_column = {executor.submit(fetch_top_matches, column): column for column in columns_to_check}
        all_top_matches = []
        for future in as_completed(future_to_column):
            all_top_matches.extend(future.result())

    chosen_match = llm_choose_match(advertiser_name, all_top_matches)

    if chosen_match:
        # Find the vertical for the chosen match
        for column in columns_to_check:
            if chosen_match in df_lookup[column].values:
                vertical = df_lookup.loc[df_lookup[column] == chosen_match, 'Client Industry Value'].iloc[0]
                return vertical, chosen_match, 'Matched'

    # If no match is found, use the original categorization method
    prompt = f"""
    Given the advertiser name "{advertiser_name}", choose the most appropriate category from the following list:
    {', '.join(categories)}
    
    Respond with only the category name precisely as written, no explanation or anything else, your response is being used to fill in a spreadsheet.
    """

    response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that categorizes advertisers."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50,
            n=1,
            stop=None,
            temperature=0,
        )

    category = response.choices[0].message.content.strip()
    if category not in categories:
        return "Uncategorized", None, 'AI Categorized'

    return category, None, 'AI Categorized'


def create_vertical_mapping(advertisers, df_lookup):
    columns_to_check = ['Company Name', 'Quickbooks Customer Name', 'Client Group']
    categories = df_lookup['Client Industry Value'].dropna().unique().tolist()
    
    vertical_mapping = []
    
    def process_advertiser(advertiser):
        vertical, matched_name, matched_column = categorize_advertiser(advertiser, categories, df_lookup, columns_to_check)
        return {
            'Advertiser': advertiser,
            'Matched_Company': matched_name if matched_name else 'NO MATCH',
            'Vertical': vertical,
            'Match_Score': None,  # We don't have a score for LLM matching
            'Categorization_Technique': matched_column
        }
    
    with ThreadPoolExecutor(max_workers=50) as executor:  # Adjust max_workers as needed
        future_to_advertiser = {executor.submit(process_advertiser, advertiser): advertiser for advertiser in advertisers}
        for future in as_completed(future_to_advertiser):
            advertiser = future_to_advertiser[future]
            try:
                result = future.result()
                vertical_mapping.append(result)
                logging.info(f"Processed advertiser: {advertiser}")
            except Exception as exc:
                logging.error(f"Advertiser {advertiser} generated an exception: {exc}")
    
    return pd.DataFrame(vertical_mapping)

def save_to_sqlite(df, db_path, table_name):
    """Save DataFrame to SQLite database."""
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

# Verify database contents
def print_sample_rows(db_path, table_name, num_rows=5):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {num_rows}")
    rows = cursor.fetchall()
    
    # Get column names
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    
    print(f"\nSample rows from {table_name}:")
    print(", ".join(columns))  # Print column names
    for row in rows:
        print(", ".join(str(value) for value in row))
    
    conn.close()

def load_vertical_mapping(csv_path):
    """Load vertical mapping from CSV file if it exists."""
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        logging.info(f"Loaded existing vertical mapping from {csv_path}")
        return df
    return None

if __name__ == "__main__":
    categorizations_file = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/categorizations.csv'
    output_db = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/sql/element_performance.db'
    table_name = 'advertiser_vertical_lookup'
    advertiser_file = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/advertiser_names.txt'
    output_csv = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/advertiser_vertical_lookup.csv'

    # Try to read advertiser names from file, fall back to API call if file not found
    if os.path.exists(advertiser_file):
        with open(advertiser_file, 'r') as f:
            advertiser_names = set(f.read().splitlines())
        logging.info(f"Read {len(advertiser_names)} advertiser names from file")
    else:
        # Get authentication token
        token = get_auth_token()

        # Get all advertiser names
        advertiser_names = get_all_advertiser_names(token, PARTNER_ID)
        logging.info(f"Retrieved {len(advertiser_names)} advertiser names from API")

        # Save advertiser names to file
        with open(advertiser_file, 'w') as f:
            f.write('\n'.join(advertiser_names))
        logging.info(f"Saved {len(advertiser_names)} advertiser names to file")

    # Load categorizations
    df_categorizations = load_categorizations(categorizations_file)

    # Try to load existing vertical mapping
    df_matched = load_vertical_mapping(output_csv)

    if df_matched is None:
        # If no existing mapping, create a new one
        logging.info("No existing vertical mapping found. Creating new mapping.")
        df_matched = create_vertical_mapping(advertiser_names, df_categorizations)
        
        # Save df_matched as CSV
        df_matched.to_csv(output_csv, index=False)
        logging.info(f"Saved new vertical mapping to CSV: {output_csv}")
    else:
        # Check for new advertisers not in the existing mapping
        existing_advertisers = set(df_matched['Advertiser'])
        new_advertisers = advertiser_names - existing_advertisers
        
        if new_advertisers:
            logging.info(f"Found {len(new_advertisers)} new advertisers. Updating vertical mapping.")
            new_mappings = create_vertical_mapping(new_advertisers, df_categorizations)
            df_matched = pd.concat([df_matched, new_mappings], ignore_index=True)
            
            # Save updated df_matched as CSV
            df_matched.to_csv(output_csv, index=False)
            logging.info(f"Saved updated vertical mapping to CSV: {output_csv}")
        else:
            logging.info("No new advertisers found. Using existing vertical mapping.")

    # Save to SQLite database
    try:
        save_to_sqlite(df_matched, output_db, table_name)
        logging.info(f"Advertiser-Vertical lookup table saved to SQLite database: {output_db}, table: {table_name}")
    except Exception as e:
        logging.error(f"Failed to save to SQLite database: {str(e)}")
        logging.info("Proceeding with CSV output only")

    # Verify database contents
    print_sample_rows(output_db, table_name)