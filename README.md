# 3rd Party Element Pipeline

This data pipeline refreshes the Pinecone TradeDesk element database for vector search.

## Overview

This pipeline processes data from The Trade Desk (TTD) and other sources to update a Pinecone vector database. It consists of several scripts that run in sequence to fetch, process, and upload data.

## Pipeline Steps

1. **Retrieve TTD Report** (`src/retrieve_ttd_report.py`)
   - Fetches the latest performance data from TTD's API.
   - Output: CSV file in `data/csv/ai_element_performance/`

2. **Concatenate TTD Reports** (`src/concatenate_ttd_reports.py`)
   - Combines recent CSV reports into a SQLite database.
   - Output: Updates `data/sql/element_performance.db`

3. **Generate Performance Lookup** (`src/generate_performance_lookup.py`)
   - Creates a lookup table for advertiser verticals.
   - Output: Updates `data/sql/element_performance.db`

4. **Query DMP** (`src/query_dmp.py`)
   - Retrieves third-party data from TTD's Data Management Platform.
   - Output: JSONL file in `data/jsonl/`

5. **Flatten and Filter DMP Data** (`src/flatten_and_filter_dmp.py`)
   - Processes the DMP data and stores it in the database.
   - Output: Updates `data/sql/element_performance.db`

6. **Prepare Pinecone JSONL** (`src/prepare_pinecone_jsonl.py`)
   - Generates a JSONL file for Pinecone ingestion.
   - Output: `data/jsonl/pinecone_data.jsonl`

7. **Detect Pinecone Changes** (`src/detect_pinecone_changes.py`)
   - Identifies necessary updates to the Pinecone database.
   - Output: `data/csv/pinecone_changes_needed.csv`

8. **Pinecone Upsert** (`src/pinecone_upsert.py`)
   - Applies the identified changes to the Pinecone database.


The entire pipeline can be executed using the `run_pipeline.py` script in the project root. This script orchestrates the execution of all steps and performs basic checks.


## Making Changes

### Modifying Individual Steps

To change the behavior of a specific step, edit the corresponding script in the `src/` directory. Each script is designed to be self-contained and focuses on a specific task.

### Changing Data Sources

- TTD API credentials: Update the `.env` file (not tracked in git)
- Input/output paths: Modify the file paths in individual scripts or in `config/locations.py`

### Adjusting the Pipeline Flow

To change the order of operations or add/remove steps, edit the `run_pipeline.py` script in the project root.

### Pinecone Configuration

Pinecone settings, including the index name, can be found in `src/create_json_vdb.ipynb` and `src/pinecone_upsert.py`.

## Data Storage

- CSV files: `data/csv/`
- JSONL files: `data/jsonl/`
- SQLite database: `data/sql/element_performance.db`

## Important Notes

- Ensure all required Python packages are installed (requirements.txt recommended)
- Keep API credentials and sensitive information in the `.env` file (not tracked in git)
- Regularly backup the SQLite database to prevent data loss

## Troubleshooting

- Check the logs generated by `run_pipeline.py` for error messages
- Ensure all file paths are correct and accessible
- Verify API credentials and Pinecone settings