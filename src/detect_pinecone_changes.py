import json
import os
import csv
from dotenv import load_dotenv
from pinecone import Pinecone
from tqdm import tqdm

# Load environment variables and initialize clients
load_dotenv('/Users/adamhunter/miniconda3/envs/ragdev/ragdev.env')
pc = Pinecone(api_key=os.environ.get('PINECONE_API_KEY'))
index = pc.Index("3rd-party-data-v2")

JSONL_FILE_PATH = "/Users/adamhunter/Documents/3rd_party_element_pipeline/data/jsonl/pinecone_data.jsonl"
OUTPUT_CSV_PATH = "/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/pinecone_changes_needed.csv"
BATCH_SIZE = 200  # Adjust this based on your memory constraints

def load_local_data(file_path):
    local_data = {}
    with open(file_path, 'r') as file:
        for line in file:
            data = json.loads(line)
            local_data[data['ThirdPartyDataId']] = data
    return local_data

def fetch_pinecone_data(id_list):
    return index.fetch(ids=id_list)

def compare_data(local_item, pinecone_item):
    if not pinecone_item:
        return "add"  # Item doesn't exist in Pinecone, needs to be added

    local_metadata = {k: str(v) for k, v in local_item.items() if k != 'ThirdPartyDataId'}
    pinecone_metadata = pinecone_item['metadata']

    # Compare 'raw_string'
    local_raw_string = f"Full Path: {local_item['FullPath']}, Description: {local_item['Description']}"
    if local_raw_string != pinecone_metadata.get('raw_string', ''):
        return "update"

    # Compare other metadata fields
    for key, value in local_metadata.items():
        if key not in pinecone_metadata or str(pinecone_metadata[key]) != value:
            return "update"

    return None  # No changes needed

def find_and_write_changes(local_data, csv_writer, batch_size=BATCH_SIZE):
    changes_count = 0
    for i in tqdm(range(0, len(local_data), batch_size), desc="Comparing data"):
        batch_ids = list(local_data.keys())[i:i+batch_size]
        pinecone_batch = fetch_pinecone_data(batch_ids)
        
        batch_changes = []
        for id in batch_ids:
            local_item = local_data[id]
            pinecone_item = pinecone_batch['vectors'].get(id)
            action = compare_data(local_item, pinecone_item)
            if action:
                batch_changes.append((id, action))
                changes_count += 1

        # Write batch changes to CSV
        csv_writer.writerows(batch_changes)

    return changes_count

def get_all_pinecone_ids():
    stats = index.describe_index_stats()
    total_vectors = stats['total_vector_count']
    
    all_ids = set()
    batch_size = 10000  # Adjust based on your Pinecone plan limits
    
    for i in tqdm(range(0, total_vectors, batch_size), desc="Fetching Pinecone IDs"):
        results = index.query(
            vector=[0] * 256,  # Dummy vector
            top_k=batch_size,
            include_metadata=False,
            include_values=False
        )
        all_ids.update(match['id'] for match in results['matches'])
    
    return all_ids

def main():
    # Clear existing CSV file
    with open(OUTPUT_CSV_PATH, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['ID', 'Action'])

    local_data = load_local_data(JSONL_FILE_PATH)
    print(f"Loaded {len(local_data)} items from local JSONL file")

    with open(OUTPUT_CSV_PATH, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        changes_count = find_and_write_changes(local_data, writer)

    print(f"Found and wrote {changes_count} items that need changes in Pinecone")

    pinecone_ids = get_all_pinecone_ids()
    print(f"Found {len(pinecone_ids)} items in Pinecone")

    # Add delete actions for items in Pinecone but not in local data
    delete_count = 0
    with open(OUTPUT_CSV_PATH, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for id in tqdm(pinecone_ids, desc="Checking for deletions"):
            if id not in local_data:
                writer.writerow([id, "delete"])
                delete_count += 1

    print(f"Added {delete_count} delete actions")
    print(f"Total changes to apply: {changes_count + delete_count}")
    print(f"All changes written to {OUTPUT_CSV_PATH}")

    # Print the first 10 items that need changes
    print("Sample of changes needed:")
    with open(OUTPUT_CSV_PATH, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        for i, row in enumerate(reader):
            if i >= 10:
                break
            print(f"ID: {row[0]}, Action: {row[1]}")

if __name__ == "__main__":
    main()