import sys
import os
from pathlib import Path

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

if __name__ == "__main__":
    input_file = "/Users/adamhunter/Documents/3rd_party_element_pipeline/data/jsonl/3rd_party_dmp_20240822_103158.jsonl"
    output_db = "/Users/adamhunter/Documents/3rd_party_element_pipeline/data/sql/element_performance.db"
    process_jsonl(input_file, output_db)