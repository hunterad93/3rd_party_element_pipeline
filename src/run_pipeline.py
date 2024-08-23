import subprocess
import logging
import os
import sqlite3
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_script(script_name):
    logging.info(f"Running {script_name}")
    result = subprocess.run(['python', f'src/{script_name}'], capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Error running {script_name}: {result.stderr}")
        raise Exception(f"Script {script_name} failed")
    logging.info(f"Completed {script_name}")
    return result.stdout

def check_file_exists(file_path):
    if not os.path.exists(file_path):
        raise Exception(f"File not found: {file_path}")

def check_db_table(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        conn.close()
        raise Exception(f"Table {table_name} not found in database")
    conn.close()

def run_pipeline():
    try:
        # Step 1: Retrieve TTD report
        run_script('retrieve_ttd_report.py')
        check_file_exists('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/ai_element_performance/ai_element_performance_latest.csv')

        # Step 2: Concatenate TTD reports
        run_script('concatenate_ttd_reports.py')
        check_db_table('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/sql/element_performance.db', 'report_stack')

        # Step 3: Generate performance lookup
        run_script('generate_performance_lookup.py')
        check_db_table('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/sql/element_performance.db', 'advertiser_vertical_lookup')

        # Step 4: Query DMP
        run_script('query_dmp.py')
        check_file_exists('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/jsonl/3rd_party_dmp_latest.jsonl')

        # Step 5: Flatten and filter DMP data
        run_script('flatten_and_filter_dmp.py')
        check_db_table('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/sql/element_performance.db', 'segments')

        # Step 6: Prepare Pinecone JSONL
        run_script('prepare_pinecone_jsonl.py')
        check_file_exists('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/jsonl/pinecone_data.jsonl')

        # Step 7: Detect Pinecone changes
        run_script('detect_pinecone_changes.py')
        check_file_exists('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/pinecone_changes_needed.csv')

        # Step 8: Pinecone upsert (if needed)
        # Uncomment the following lines when you're ready to perform the upsert
        # run_script('pinecone_upsert.py')
        # check_pinecone_updates()  # You'll need to implement this function

        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")

if __name__ == "__main__":
    run_pipeline()