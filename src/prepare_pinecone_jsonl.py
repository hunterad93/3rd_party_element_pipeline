import sqlite3
import json

def fetch_data(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def calculate_performance_keys(performance_data):
    keys = {}
    overall_clicks = 0
    overall_impressions = 0
    overall_cost = 0
    overall_conversions = 0
    
    for row in performance_data:
        vertical = row['Vertical']
        total_clicks = row['total_clicks']
        total_impressions = row['total_impressions']
        total_hypothetical_cost = row['total_hypothetical_cost']
        total_click_view_conversions = row['total_click_view_conversions']
        
        ctr = total_clicks / total_impressions if total_impressions else 0
        cpa = total_hypothetical_cost / total_click_view_conversions if total_click_view_conversions else 0
        cpc = total_hypothetical_cost / total_clicks if total_clicks else 0
        
        keys[f'{vertical}_ctr'] = ctr
        keys[f'{vertical}_cpa'] = cpa
        keys[f'{vertical}_cpc'] = cpc
        
        overall_clicks += total_clicks
        overall_impressions += total_impressions
        overall_cost += total_hypothetical_cost
        overall_conversions += total_click_view_conversions
    
    overall_ctr = overall_clicks / overall_impressions if overall_impressions else 0
    overall_cpa = overall_cost / overall_conversions if overall_conversions else 0
    overall_cpc = overall_cost / overall_clicks if overall_clicks else 0
    
    keys['overall_ctr'] = overall_ctr
    keys['overall_cpa'] = overall_cpa
    keys['overall_cpc'] = overall_cpc
    
    return keys

def replace_none_with_null(d):
    return {k: ("null" if v is None else v) for k, v in d.items()}

def main():
    try:
        conn = sqlite3.connect('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/sql/element_performance.db')
        conn.row_factory = sqlite3.Row
        
        segments_query = "SELECT * FROM segments"
        performance_query = "SELECT * FROM performance_summary"
        
        segments_data = fetch_data(segments_query, conn)
        performance_data = fetch_data(performance_query, conn)
        
        # Group performance data by ThirdPartyDataId
        performance_dict = {}
        for row in performance_data:
            third_party_id = row['ThirdPartyDataId']
            if third_party_id not in performance_dict:
                performance_dict[third_party_id] = []
            performance_dict[third_party_id].append(row)
        
        with open('/Users/adamhunter/Documents/3rd_party_element_pipeline/data/jsonl/pinecone_data.jsonl', 'w') as outfile:
            for segment in segments_data:
                third_party_id = segment['ThirdPartyDataId']
                segment_dict = dict(segment)  # Convert sqlite3.Row to dictionary
                if third_party_id in performance_dict:
                    performance_keys = calculate_performance_keys(performance_dict[third_party_id])
                    segment_dict.update(performance_keys)
                segment_dict = replace_none_with_null(segment_dict)  # Replace None with "null"
                json.dump(segment_dict, outfile)
                outfile.write('\n')
        
        conn.close()
    except sqlite3.OperationalError as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()