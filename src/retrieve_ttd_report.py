import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import time
from requests.exceptions import RequestException
from query_dmp import get_auth_token
import csv

# Load environment variables
load_dotenv('/Users/adamhunter/miniconda3/envs/ragdev/ragdev.env')

TTD_USERNAME = os.getenv('TTD_USERNAME')
TTD_PASS = os.getenv('TTD_PASS')
PARTNER_ID = os.getenv('PARTNER_ID')
DOWNLOAD_DIR = '/Users/adamhunter/Documents/3rd_party_element_pipeline/data/csv/ai_element_performance/'


def get_available_reports(token: str, partner_id: str, start_date: str, max_retries=3, retry_delay=5):
    """Retrieve available reports for a given partner ID."""
    url = "https://api.thetradedesk.com/v3/myreports/reportexecution/query/partners"
    headers = {"Content-Type": "application/json", "TTD-Auth": token}
    payload = {
        "PartnerIds": [partner_id],
        "ExecutionStates": ["Complete"],
        "ExecutionSpansStartDate": start_date,
        "PageStartIndex": 0,
        "PageSize": 1000  # Adjust as needed
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                print(f"Failed to get available reports after {max_retries} attempts: {e}")
                raise
            print(f"Report retrieval failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

def flatten_report(report):
    """Flatten a report object into a dictionary."""
    flat_report = {
        "ReportExecutionId": report['ReportExecutionId'],
        "ReportExecutionState": report['ReportExecutionState'],
        "LastStateChangeUTC": report['LastStateChangeUTC'],
        "DisabledReason": report['DisabledReason'],
        "Timezone": report['Timezone'],
        "ReportStartDateInclusive": report['ReportStartDateInclusive'],
        "ReportEndDateExclusive": report['ReportEndDateExclusive'],
        "ReportScheduleName": report['ReportScheduleName']
    }
    
    if report['ReportDeliveries']:
        delivery = report['ReportDeliveries'][0]  # Assume first delivery is most relevant
        flat_report.update({
            "ReportDestination": delivery['ReportDestination'],
            "DeliveredPath": delivery['DeliveredPath'],
            "DeliveredUTC": delivery['DeliveredUTC'],
            "DownloadURL": delivery['DownloadURL'],
            "DownloadURLExpirationUTC": delivery['DownloadURLExpirationUTC']
        })
    
    return flat_report

def download_report(url: str, filename: str, max_retries=3, retry_delay=5):
    """Download a report from the given URL."""
    headers = {"Content-Type": "application/json", "TTD-Auth": token}
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"Report downloaded successfully: {filename}")
            return True
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                print(f"Failed to download report after {max_retries} attempts: {e}")
                return False
            print(f"Download failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

if __name__ == "__main__":
    # Get authentication token
    token = get_auth_token()

    # Set the start date for report retrieval (e.g., 30 days ago)
    start_date = (datetime.now() - timedelta(days=30)).isoformat()

    # Get available reports
    reports = get_available_reports(token, PARTNER_ID, start_date)

    if reports and 'Result' in reports:
        # Filter reports with name containing 'element_performance'
        element_performance_reports = [report for report in reports['Result'] if 'ai_element_performance' in report['ReportScheduleName'].lower()]
        
        if element_performance_reports:
            # Sort by LastStateChangeUTC to get the most recent report
            most_recent_report = sorted(element_performance_reports, key=lambda x: x['LastStateChangeUTC'], reverse=True)[0]
            
            # Prepare filename for download
            report_name = most_recent_report['ReportScheduleName'].replace(' ', '_')
            report_date = most_recent_report['ReportEndDateExclusive'].split('T')[0]
            filename = os.path.join(DOWNLOAD_DIR, f"{report_name}_{report_date}.csv")
            
            # Ensure the download directory exists
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            
            # Download the report
            if most_recent_report['ReportDeliveries']:
                download_url = most_recent_report['ReportDeliveries'][0]['DownloadURL']
                if download_report(download_url, filename):
                    print(f"Report '{most_recent_report['ReportScheduleName']}' downloaded as '{filename}'")
                else:
                    print("Failed to download the report.")
            else:
                print("No download URL available for the report.")
        else:
            print("No 'ai_element_performance' reports found.")
    else:
        print("No reports found or error in retrieving reports.")