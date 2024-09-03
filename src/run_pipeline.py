import subprocess
import logging
import os
import sqlite3
from datetime import datetime
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_script(script_name):
    logging.info(f"Running {script_name}")
    result = subprocess.run(['python', f'{script_name}'], capture_output=True, text=True)
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

def get_row_count(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    conn.close()
    return count

def check_row_count_change(db_path, table_name, before_count, tolerance_percent, operation_name):
    after_count = get_row_count(db_path, table_name)
    percent_change = ((after_count - before_count) / before_count) * 100
    
    logging.info(f"{operation_name}: {table_name} rows changed by {percent_change:.2f}%")
    
    if abs(percent_change) > tolerance_percent:
        raise Exception(f"{operation_name}: {table_name} row count change ({percent_change:.2f}%) exceeds tolerance of {tolerance_percent}%")

def count_jsonl_rows(file_path):
    if not os.path.exists(file_path):
        return 0
    with open(file_path, 'r') as f:
        return sum(1 for line in f if line.strip())

def check_jsonl_row_count_change(file_path, before_count, tolerance_percent, operation_name):
    after_count = count_jsonl_rows(file_path)
    if before_count == 0:
        percent_change = 100 if after_count > 0 else 0
    else:
        percent_change = ((after_count - before_count) / before_count) * 100
    
    logging.info(f"{operation_name}: rows changed by {percent_change:.2f}%")
    
    if abs(percent_change) > tolerance_percent:
        raise Exception(f"{operation_name}: row count change ({percent_change:.2f}%) exceeds tolerance of {tolerance_percent}%")

def run_pipeline():
    try:
        db_path = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/sql/element_performance.db'
        jsonl_path = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/jsonl/pinecone_data.jsonl'

        # Step 1: Retrieve TTD report
        folder_path = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/ai_element_performance/'
        files_before = len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
        run_script('retrieve_ttd_report.py')
        files_after = len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
        if files_after != files_before + 1:
            raise Exception(f"Expected 1 new file in {folder_path}, but found {files_after - files_before}")

        # Step 2: Concatenate TTD reports
        report_stack_before = get_row_count(db_path, 'report_stack')
        run_script('concatenate_ttd_reports.py')
        check_row_count_change(db_path, 'report_stack', report_stack_before, 30, 'Concatenate TTD reports')

        # Step 3: Generate performance lookup
        run_script('generate_performance_lookup.py')
        check_db_table(db_path, 'advertiser_vertical_lookup')

        # Step 4: Query DMP
        run_script('query_dmp.py')

        # Step 5: Flatten and filter DMP data
        segments_before = get_row_count(db_path, 'segments')
        run_script('flatten_and_filter_dmp.py')
        check_row_count_change(db_path, 'segments', segments_before, 10, 'Flatten and filter DMP data')

        # Step 6: Prepare Pinecone JSONL
        pinecone_data_before = count_jsonl_rows(jsonl_path)
        run_script('prepare_pinecone_jsonl.py')
        check_file_exists(jsonl_path)
        check_jsonl_row_count_change(jsonl_path, pinecone_data_before, 10, 'Prepare Pinecone JSONL')

        # Step 7: Detect Pinecone changes
        run_script('detect_pinecone_changes.py')
        check_file_exists('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/pinecone_changes_needed.csv')

        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")

if __name__ == "__main__":
    run_pipeline()