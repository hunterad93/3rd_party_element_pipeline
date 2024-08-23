import json
import os
import csv
from dotenv import load_dotenv
from openai import OpenAI
from pinecone import Pinecone
from tqdm import tqdm

# Load environment variables and initialize clients
load_dotenv('/Users/adamhunter/miniconda3/envs/ragdev/ragdev.env')
openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
pc = Pinecone(api_key=os.environ.get('PINECONE_API_KEY'))
index = pc.Index("3rd-party-data-v3")

EMBEDDING_MODEL = "text-embedding-3-large"
BATCH_SIZE = 200
JSONL_FILE_PATH = "/Users/adamhunter/Documents/3rd_party_element_pipeline/data/jsonl/pinecone_data.jsonl"
CSV_FILE_PATH = "/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/pinecone_changes_needed.csv"

def load_local_data(file_path):
    local_data = {}
    with open(file_path, 'r') as file:
        for line in file:
            data = json.loads(line)
            local_data[data['ThirdPartyDataId']] = data
    return local_data

def load_changes_from_csv(file_path):
    changes = {}
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        for row in reader:
            changes[row[0]] = row[1]
    return changes

def create_chunk(data):
    raw_string = f"Full Path: {data['FullPath']}, Description: {data['Description']}"
    metadata = {k: str(v) for k, v in data.items() if k != 'ThirdPartyDataId'}
    metadata['raw_string'] = raw_string
    return {
        "id": data['ThirdPartyDataId'],
        "metadata": metadata
    }

def generate_embeddings(batch):
    texts = [f"Full Path: {item['FullPath']}, Description: {item['Description']}" for item in batch]
    response = openai_client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
        encoding_format="float",
        dimensions=256
    )
    return [data.embedding for data in response.data]

def apply_changes(local_data, changes, batch_size, limit):
    upsert_batch = []
    delete_ids = []
    processed_count = 0

    for id, action in tqdm(changes.items(), desc="Preparing data"):
        if limit and processed_count >= limit:
            break
        if action in ["add", "update"] and id in local_data:
            upsert_batch.append(local_data[id])
        elif action == "delete":
            delete_ids.append(id)
        processed_count += 1

    print(f"Processing {len(upsert_batch)} upserts and {len(delete_ids)} deletions")

    # Process upserts
    for i in tqdm(range(0, len(upsert_batch), batch_size), desc="Applying upserts"):
        current_batch = upsert_batch[i:i+batch_size]
        chunks = [create_chunk(item) for item in current_batch]
        embeddings = generate_embeddings(current_batch)
        
        upserts = [
            {
                "id": chunk['id'],
                "values": embedding,
                "metadata": chunk['metadata']
            }
            for chunk, embedding in zip(chunks, embeddings)
        ]
        
        index.upsert(upserts)

    # Process deletions
    for i in tqdm(range(0, len(delete_ids), batch_size), desc="Applying deletions"):
        batch = delete_ids[i:i+batch_size]
        response = index.delete(ids=batch)
        print(response)

# In the main function, call apply_changes with a limit:
def main():
    local_data = load_local_data(JSONL_FILE_PATH)
    print(f"Loaded {len(local_data)} items from local JSONL file")

    changes = load_changes_from_csv(CSV_FILE_PATH)
    print(f"Loaded {len(changes)} changes from CSV file")

    # Set a limit for testing, e.g., 100 records
    apply_changes(local_data, changes, batch_size=BATCH_SIZE, limit=BATCH_SIZE)
    print("Changes applied to Pinecone database")

if __name__ == "__main__":
    main()