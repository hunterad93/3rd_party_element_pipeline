import sys
import os
from pathlib import Path
import glob
import csv

# Add the project root to the Python path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from config.locations import NON_US_LOCATIONS

import json
import re
import sqlite3
from typing import List, Dict

def flatten_json(data: Dict) -> Dict:
    flattened = {}
    for key, value in data.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flattened[f"{key}_{sub_key}"] = sub_value
        else:
            flattened[key] = value
    return flattened

def filter_non_us(segments: List[Dict]) -> List[Dict]:
    pattern = re.compile(r'\b(' + '|'.join(map(re.escape, NON_US_LOCATIONS)) + r')\b', re.IGNORECASE)
    
    def is_us_segment(segment):
        search_text = f"{segment['FullPath']} {segment['BrandName']}"
        return not pattern.search(search_text)
    
    filtered_segments = [segment for segment in segments if is_us_segment(segment)]
    print(f"Filtered out {len(segments) - len(filtered_segments)} non-US locations")
    return filtered_segments

def print_random_rows(db_path: str, num_rows: int = 5):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM segments ORDER BY RANDOM() LIMIT ?", (num_rows,))
    rows = cursor.fetchall()
    
    if rows:
        columns = [description[0] for description in cursor.description]
        print("\nRandom rows from the database:")
        for row in rows:
            print("\n" + "-" * 40)
            for col, value in zip(columns, row):
                print(f"{col}: {value}")
    else:
        print("No rows found in the database.")
    
    conn.close()

def get_column_types(segments: List[Dict]) -> Dict[str, str]:
    sample_segment = segments[0]
    column_types = {}
    
    for key, value in sample_segment.items():
        if isinstance(value, int):
            column_types[key] = "INTEGER"
        elif isinstance(value, float):
            column_types[key] = "REAL"
        else:
            column_types[key] = "TEXT"
    
    return column_types

def create_table(cursor, column_types: Dict[str, str]):
    columns = ', '.join([f'"{col}" {dtype}' for col, dtype in column_types.items()])
    cursor.execute(f'CREATE TABLE segments ({columns})')

def process_jsonl(input_file: str, output_db: str):
    segments = []
    
    with open(input_file, 'r') as f:
        for line in f:
            segment = json.loads(line)
            flattened_segment = flatten_json(segment)
            segments.append(flattened_segment)
    
    filtered_segments = filter_non_us(segments)
    
    if not filtered_segments:
        print("No segments to process.")
        return
    
    column_types = get_column_types(filtered_segments)
    
    conn = sqlite3.connect(output_db)
    cursor = conn.cursor()
    
    cursor.execute('DROP TABLE IF EXISTS segments')
    create_table(cursor, column_types)
    
    for segment in filtered_segments:
        placeholders = ', '.join(['?' for _ in segment])
        columns = ', '.join([f'"{k}"' for k in segment.keys()])
        cursor.execute(f'INSERT INTO segments ({columns}) VALUES ({placeholders})', list(segment.values()))
    
    conn.commit()
    conn.close()
    
    print(f"Processed {len(filtered_segments)} segments and stored them in {output_db}")
    print_random_rows(output_db)

def get_sorted_dmp_files(input_dir):
    pattern = os.path.join(input_dir, '3rd_party_dmp_*.jsonl')
    files = glob.glob(pattern)
    return sorted(files, key=os.path.getctime, reverse=True)

def cleanup_old_files(files_to_keep, all_files):
    files_to_delete = set(all_files) - set(files_to_keep)
    for file in files_to_delete:
        try:
            os.remove(file)
            print(f"Deleted old file: {file}")
        except Exception as e:
            print(f"Error deleting file {file}: {e}")

def export_to_csv(db_path: str, csv_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM segments")
    rows = cursor.fetchall()
    
    if rows:
        columns = [description[0] for description in cursor.description]
        
        with open(csv_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(columns)
            csvwriter.writerows(rows)
        
        print(f"Exported {len(rows)} rows to {csv_path}")
    else:
        print("No rows found in the database to export.")
    
    conn.close()

if __name__ == "__main__":
    input_dir = "/Users/adamhunter/Documents/3rd_party_element_pipeline/data/jsonl"
    output_db = "/Users/adamhunter/Documents/3rd_party_element_pipeline/data/sql/element_performance.db"
    output_csv = "/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/element_performance.csv"
    
    try:
        dmp_files = get_sorted_dmp_files(input_dir)
        if not dmp_files:
            raise FileNotFoundError(f"No 3rd_party_dmp files found in {input_dir}")
        
        input_file = dmp_files[0]
        print(f"Processing most recent file: {input_file}")
        process_jsonl(input_file, output_db)
        
        print("Exporting database to CSV...")
        export_to_csv(output_db, output_csv)
        
        print("Cleaning up old files...")
        cleanup_old_files(dmp_files[:2], dmp_files)
        
        print("Processing, export, and cleanup completed successfully")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)