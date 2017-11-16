"""
Lambda function for logging report runs to a CSV file.

"""
import os
import requests
import json
import csv
from requests.auth import HTTPBasicAuth


# Read environment variables
token = os.environ['api_token']
password = os.environ['api_password']


def lambda_handler(event, context):
    """
    AWS Lambda entry point

    """
    body = json.loads(event.get('body','{}'))
    event_name = body.get('event','')
    run_url = body.get('report_run_url','')

    if event_name == 'report_run_completed':
        queries_info = get_queries_info(run_url)
        log_to_csv(queries_info)

    return {'body': 'success'}


def get_queries_info(run_url):
    """
    Retrieve metadata about query runs.

    """
    query_runs_url = run_url + '/query_runs'
    queries_req = requests.get(query_runs_url, auth=HTTPBasicAuth(token, password))
    queries_res = queries_req.json()
    columns_list = ["query_token", "state", "created_at", "completed_at", "raw_source", "parameters"]
    data = []

    for query in queries_res['_embedded']['query_runs']:
        row = [ query[col].encode("utf-8").replace('\n', ' ').replace('    ', '') if col == 'raw_source' else str(query[col]).encode("utf-8")
                for col in columns_list ]
        data.append(row)

    return data


def log_to_csv(queries_info):
    """
    Write to CSV.

    """
    with open('file_name.csv', 'a') as f:
        writer = csv.writer(f)

        for line in queries_info:
            writer.writerow(line)
