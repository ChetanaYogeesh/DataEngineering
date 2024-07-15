'''
get_authorization_header: Generates the Base64 encoded authorization header.
get_jobs: Fetches all jobs from Greenhouse.
update_requisition_ids: Updates the requisition IDs for each job by iterating through the list of jobs.
get_max_date: Extracts the maximum date from the job creation dates.
log_etl: Logs the ETL process in the MySQL database.
main: Main function that orchestrates the entire process.
Comments:
The authorization header is generated and reused to avoid repeated encoding.
The jobs are fetched once and reused in subsequent functions to optimize the workflow.
String formatting is used for constructing URLs and payloads.
A more Pythonic way to enumerate over the jobs and update their requisition IDs is employed.
Exception handling and additional logging could be added for a more robust implementation.
'''

import requests
import base64
import json
import re
from datetime import datetime
from helpers import MySQLRpts
from credentials import greenhouse


def get_authorization_header():
    """
    Generates the authorization header for Greenhouse API requests.
    """
    b64_val = base64.b64encode(f"{greenhouse['harvest_key']}:".encode('utf-8')).decode('utf-8')
    return {"Authorization": f"Basic {b64_val}"}


def get_jobs():
    """
    Fetches all jobs from Greenhouse.
    """
    response = requests.get('https://harvest.greenhouse.io/v1/jobs', headers=get_authorization_header())
    return response.json()


def update_requisition_ids(jobs):
    """
    Updates the requisition IDs for each job.
    """
    for idx, job in enumerate(jobs, start=1):
        response = requests.patch(
            f'https://harvest.greenhouse.io/v1/jobs/{job["id"]}',
            headers={
                "Authorization": get_authorization_header()["Authorization"],
                "Content-Type": "application/json",
                "On-Behalf-Of": greenhouse['user_id']
            },
            data=json.dumps({'requisition_id': str(idx)})
        )
        response.json()


def get_max_date(jobs):
    """
    Extracts the maximum date from job creation dates.
    """
    date_strings = [
        v for job in jobs for k, v in job.items()
        if isinstance(v, str) and re.findall(r'T.*:.*:.*Z', v)
    ]
    max_date_str = max(date_strings, key=lambda date: datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ'))
    return max_date_str


def log_etl(max_date):
    """
    Logs the ETL process with the max date to MySQL.
    """
    mysql_rpts = MySQLRpts()
    mysql_rpts.etl_log('Greenhouse Req IDs', 'Greenhouse', 'Greenhouse', max_date=mysql_rpts.convert_datetime(max_date))


def main():
    """
    Main function to execute the script.
    """
    jobs = get_jobs()
    update_requisition_ids(jobs)
    max_date = get_max_date(jobs)
    log_etl(max_date)


if __name__ == "__main__":
    main()
