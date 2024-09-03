import os
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import glob

def get_recent_csv_files(folder_path, months=6):
    """Get the most recent 6 months of CSV files."""
    today = datetime.now()
    six_months_ago = today - timedelta(days=30*months)
    
    csv_files = glob.glob(os.path.join(folder_path, 'ai_element_performance_*.csv'))
    recent_files = [
        file for file in csv_files
        if datetime.strptime(os.path.basename(file).split('_')[3].split('.')[0], '%Y-%m-%d') >= six_months_ago
    ]
    
    return sorted(recent_files, reverse=True)

def get_vertical_lookup(conn):
    """Retrieve the advertiser-vertical lookup as a dictionary."""
    df = pd.read_sql_query("SELECT Advertiser, Vertical FROM advertiser_vertical_lookup", conn)
    return dict(zip(df.Advertiser, df.Vertical))

def process_csv_files(file_list, db_path, table_name):
    """Process CSV files and refresh the report_stack table with the last 6 months of data."""
    conn = sqlite3.connect(db_path)
    vertical_lookup = get_vertical_lookup(conn)
    
    # Create or replace the table with the first file
    if file_list:
        df = pd.read_csv(file_list[0])
        df['Vertical'] = df['Advertiser'].map(vertical_lookup)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Replaced {table_name} with data from {file_list[0]}")
        
        # Process the rest of the files
        for file in file_list[1:]:
            df = pd.read_csv(file)
            df['Vertical'] = df['Advertiser'].map(vertical_lookup)
            df.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"Added data from {file} to {table_name}")
    
    conn.close()

def remove_old_csv_files(folder_path, months=12):
    """Remove CSV files older than 12 months."""
    today = datetime.now()
    cutoff_date = today - timedelta(days=30*months)
    
    csv_files = glob.glob(os.path.join(folder_path, 'ai_element_performance_*.csv'))
    removed_count = 0
    
    for file in csv_files:
        file_date = datetime.strptime(os.path.basename(file).split('_')[3].split('.')[0], '%Y-%m-%d')
        if file_date < cutoff_date:
            os.remove(file)
            removed_count += 1
            print(f"Removed old file: {file}")
    
    return removed_count

if __name__ == "__main__":
    input_folder = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/ai_element_performance/'
    output_db = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/sql/element_performance.db'
    table_name = 'report_stack'

    # Remove old CSV files
    removed_files = remove_old_csv_files(input_folder)
    print(f"Removed {removed_files} CSV files older than 12 months.")

    recent_files = get_recent_csv_files(input_folder)
    print(f"Found {len(recent_files)} CSV files from the last 6 months.")

    if recent_files:
        process_csv_files(recent_files, output_db, table_name)
        print(f"Report stack in {output_db} has been updated with the latest 6 months of data.")
    else:
        print("No recent CSV files found.")