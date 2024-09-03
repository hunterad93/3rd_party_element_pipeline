import pandas as pd
import requests
import json
import os
import time
import logging
from typing import Dict, Any, List, Set
from dotenv import load_dotenv
from requests.exceptions import RequestException
from datetime import datetime
import random
import sys

##############################################################################################
# This script takes any valid pathlabs advertiser id as input, downloads all available brands,     
# then uses the list of brand ids to download all available segments for each brand     
# the output is a jsonl file with a timestamp with lines for each segment               
##############################################################################################

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv('/Users/adamhunter/miniconda3/envs/ragdev/ragdev.env')

TTD_USERNAME = os.getenv('TTD_USERNAME')
TTD_PASS = os.getenv('TTD_PASS')
PARTNER_ID = os.getenv('PARTNER_ID')

def get_auth_token(max_retries=3, retry_delay=5) -> str:
    """Get authentication token from The Trade Desk API with retries."""
    url = "https://api.thetradedesk.com/v3/authentication"
    payload = {"Login": TTD_USERNAME, "Password": TTD_PASS}
    headers = {"Content-Type": "application/json"}
    
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json().get("Token")
        except RequestException as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to get auth token after {max_retries} attempts: {e}")
                raise
            logging.warning(f"Auth token request failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)


def get_all_advertiser_ids(token: str, partner_id: str) -> Set[str]:
    """Retrieve all advertiser IDs."""
    url = f"https://api.thetradedesk.com/v3/overview/partner/{partner_id}"
    headers = {"Content-Type": "application/json", "TTD-Auth": token}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        partner_overview = response.json()
        
        all_advertiser_ids = set()
        for advertiser in partner_overview.get("Advertisers", []):
            all_advertiser_ids.add(advertiser["AdvertiserId"])
        
        return all_advertiser_ids
    except RequestException as e:
        logging.error(f"Failed to get advertiser IDs: {e}")
        raise

def get_available_brands(advertiser_id: str, token: str, max_retries=3, retry_delay=5) -> List[str]:
    """Get available brand IDs for a given advertiser."""
    url = f"https://api.thetradedesk.com/v3/dmp/thirdparty/facets/{advertiser_id}"
    headers = {"Content-Type": "application/json", "TTD-Auth": token}
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            print(json.dumps(response.json(), indent=2))
            response.raise_for_status()
            brands = response.json().get("Brands", [])
            return [brand["BrandId"] for brand in brands]
        except RequestException as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to get available brands after {max_retries} attempts: {e}")
                raise
            logging.warning(f"Brand retrieval failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

def query_third_party_data(advertiser_id: str, token: str, brand_ids: List[str], page_start_index: int = 0, page_size: int = 100, max_retries=3, retry_delay=10) -> Dict[str, Any]:
    """Query third-party data from The Trade Desk API with retries."""
    url = "https://api.thetradedesk.com/v3/dmp/thirdparty/advertiser"
    headers = {"Content-Type": "application/json", "TTD-Auth": token}
    payload = {
        "AdvertiserId": advertiser_id,
        "PageStartIndex": page_start_index,
        "PageSize": page_size,
        "BrandIds": brand_ids,
        "UniqueCountMinimum": 0,
        "ExcludeTotalCounts": True
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=600)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to query third-party data after {max_retries} attempts: {e}")
                raise
            if response.status_code == 429:  # Too Many Requests
                retry_delay = int(response.headers.get('Retry-After', retry_delay))
            logging.warning(f"Query failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

def fetch_all_third_party_data(advertiser_id: str, token: str, brand_ids: List[str], output_file: str):
    """Fetch all third-party data for an advertiser and save to file."""
    page_size = 1000
    page_start_index = 0
    
    with open(output_file, 'a') as f:  # Changed to append mode
        while True:
            try:
                result = query_third_party_data(advertiser_id, token, brand_ids, page_start_index, page_size)
                print(len(result['Result']))
                if 'Result' not in result or not result['Result']:
                    break
                
                for item in result['Result']:
                    json.dump(item, f)
                    f.write('\n')
                
                page_start_index += len(result['Result'])
                logging.info(f"Fetched {page_start_index} results for AdvertiserId: {advertiser_id}")
                
                if len(result['Result']) < page_size:
                    break
                
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logging.error(f"Error fetching data for AdvertiserId {advertiser_id}: {e}")
                break

if __name__ == "__main__":
    # Set up output directory
    output_dir = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/jsonl'
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename with new timestamp format
    timestamp = datetime.now().strftime("%Y-%m-%d")
    output_file = os.path.join(output_dir, f'3rd_party_dmp_{timestamp}.jsonl')

    # Check if file already exists
    if os.path.exists(output_file):
        error_message = f"Error: File '{output_file}' already exists for today's date. Please remove or rename the existing file before running the script again."
        logging.error(error_message)
        print(error_message, file=sys.stderr)
        sys.exit(1)

    # Get authentication token
    token = get_auth_token()

    # Get all advertiser IDs
    try:
        all_advertiser_ids = get_all_advertiser_ids(token, PARTNER_ID)
        logging.info(f"Retrieved {len(all_advertiser_ids)} advertiser IDs")

        # Randomly select an advertiser ID, it doesnt seem to matter which
        if all_advertiser_ids:
            advertiser_id = random.choice(list(all_advertiser_ids))
            logging.info(f"Selected random AdvertiserId: {advertiser_id}")
        else:
            logging.error("No advertiser IDs found")
            exit(1)
    except Exception as e:
        logging.error(f"Failed to retrieve advertiser IDs: {e}")
        exit(1)
    available_brands = get_available_brands(advertiser_id, token)
    logging.info(f"Retrieved {len(available_brands)} available brands for AdvertiserId: {advertiser_id}")

    # Process brand IDs in batches of 10
    for i in range(0, len(available_brands), 10):
        brand_id_batch = available_brands[i:i+10]
        try:
            fetch_all_third_party_data(advertiser_id, token, brand_id_batch, output_file)
            logging.info(f"Completed batch {i//10 + 1} for AdvertiserId: {advertiser_id}")
        except Exception as e:
            logging.error(f"Error processing batch {i//10 + 1} for AdvertiserId {advertiser_id}: {e}")
        time.sleep(2)  # Add a delay between batches

    logging.info(f"Completed AdvertiserId: {advertiser_id}. Data saved to {output_file}")