import os
import re
import io
import sys
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.request import urlopen
from collections import OrderedDict
from fuzzywuzzy import fuzz, process
from pytz import timezone
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from salesforce_bulk.util import IteratorBytesIO

# Path configuration and import authentication credentials
AUTH_DIR = os.path.expanduser("~/docker")
os.chdir(AUTH_DIR)
sys.path.insert(0, AUTH_DIR)
from authentication.CYcreds import * 
from authentication.helpers import NetsuiteRpts, SalesforceRpts, MySQLRpts

# Define constants
NOISE_WORDS = [
    'research', 'inc.', 'electronics', 'engineering', 'services', 'inc', 'Centers',
    'System', 'Systems', 'International', 'Center', 'Scientific Studies and Research Center Employee',
    'University', 'Trading', 'Co', 'Technologies', 'Systems Pte Ltd', 'Pte', 'Ltd', 'Corp',
    'Corporation', 'Industries', 'Industry', 'COMPANY', 'INTERNACIONAL', 'Neighborhood',
    'scientific studies and research center', 'Professional'
]
NOISE_WORDS = [word.lower() for word in NOISE_WORDS]

TRADE_GOV_URL = blacklist['url']
NETSUITE_REPORT_URL = netsuite['rpt_blacklist_2']
DAYS = 30

# Download and preprocess the Trade.gov CSV file
def download_trade_gov_csv(url):
    response = requests.post(url)
    response.raise_for_status()
    data = response.content.decode('utf8')
    df = pd.read_csv(io.StringIO(data))
    df = df[['source', 'name', 'addresses', 'alt_names', 'title', 'source_list_url']]
    df.replace(r'\W+', ' ', regex=True, inplace=True)
    df.fillna('', inplace=True)
    df['name_address_title_altnames'] = (df['name'] + ' ' + df['alt_names'] + ' ' +
                                         df['addresses'] + ' ' + df['title']).str.lower()
    return df

# Read and preprocess the Netsuite recent orders report
def download_netsuite_report(url):
    raw_response = urlopen(url)
    raw_data = raw_response.read().decode('utf-8').replace("\r", " ").replace("\n", " ").encode('utf-8')
    bsobj = BeautifulSoup(raw_data, "html.parser")
    writer = [[elem.text.replace("=", "") for elem in tr.find_all('td')] for tr in bsobj.find_all('tr')]
    ns_raw_core_data = [dict(zip(writer[0], writer[i])) for i in range(1, len(writer))]
    return pd.DataFrame(ns_raw_core_data)

def preprocess_netsuite_data(df):
    df['Date'] = pd.to_datetime(df['Date'])
    cutoff_date = pd.to_datetime('today') - timedelta(days=DAYS)
    df = df[df['Date'] > cutoff_date]
    df['Billing Address'] = df['Address: Billing Address Line 1'] + ' ' + df['Address: Billing Address City'] + ' ' + df['Address: Billing Address State'] + ' ' + df['Address: Billing Address Zip Code']
    df['Shipping Address'] = df['Address: Shipping Address Line 1'] + ' ' + df['Address: Shipping Address City'] + ' ' + df['Address: Shipping Address State'] + ' ' + df['Address: Shipping Address Zip Code']
    df['Address: Billing Attention'].replace(r'\W+', ' ', regex=True, inplace=True)
    df['Address: Shipping Attention'].replace(r'\W+', ' ', regex=True, inplace=True)
    df['all'] = (df['Billing Address'] + ' ' + df['Shipping Address'] + ' ' + df['Address: Billing Attention'] + ' ' +
                 df['Address: Shipping Attention'] + ' ' + df['Name'] + ' ' + df['Company Name']).str.lower()
    df['all-noisewords'] = df['all'].apply(lambda x: re.sub(r'|'.join(NOISE_WORDS), ' ', x))
    df['all-noisewords'] = df['all-noisewords'].apply(lambda x: re.sub(r'\s+', ' ', x))
    return df

# Compare Netsuite recent orders with the Trade.gov consolidated screening list
def compare_orders_with_screening_list(ns_df, screening_df):
    ns_list = ns_df['all-noisewords'].astype(str).tolist()
    screening_list = screening_df['name_address_title_altnames'].astype(str).tolist()
    possibilities = {order: process.extract(order, screening_list, scorer=fuzz.token_sort_ratio) for order in ns_list}
    results = {order: match[0] for order in possibilities for match in possibilities[order] if match[1] > 65}
    return results

# Send matched results to Slack
def send_to_slack(ns_df, screening_df, results):
    payload = {
        "channel": "#my_test",
        "username": "ITAR",
        "icon_emoji": ":itar:",
        "attachments": [
            {
                "fallback": "Possible ITAR Violation",
                "color": "warning",
                "pretext": "The following order has a match on the "
                           "<https://api.trade.gov/static/consolidated_screening_list/consolidated.csv|consolidated screening list> "
                           "and has been flagged for ITAR check.\n"
                           "Please visit <http://2016.export.gov/ecr/eg_main_023148.asp|export.gov> for more information.",
                "fields": [
                    {
                        "title": "Possible ITAR Violator \n",
                        "short": False
                    }
                ]
            }
        ]
    }
    for purchase, itar in results.items():
        order_info = ns_df[ns_df['all-noisewords'] == purchase].iloc[0]
        itar_info = screening_df[screening_df['name_address_title_altnames'] == itar].iloc[0]
        payload["attachments"][0]["fields"][0]["value"] = (
            f"\n\n *COMPANY ORDER DETAILS [*"
            f"\nName : {order_info['Name']}"
            f"\nCompany : {order_info['Company Name']}"
            f"\nOrder Number : {order_info['Document Number']}"
            f"\nBilling Address : {order_info['Billing Address']}"
            f"\nShipping Address : {order_info['Shipping Address']}"
            f"\nOrder Date : {order_info['Date']}"
            f"\n\n \t \t *GOVERNMENT DETAILS [*"
            f"\n\t \t \tSource : {itar_info['source']}"
            f"\n\t \t \tName : {itar_info['name']}"
            f"\n\t \t \tAddress : {itar_info['addresses']}"
            f"\n\t \t \tCountry : {itar_info['alt_names']}"
            f"\n\t \t \tITAR Source List : {itar_info['source_list_url']}"
            f"\n\t \t*]*"
        )
        order_str = f'Order Number : {order_info["Document Number"]}'
        with open('/docker/ITAR/Blacklist_slack_post.txt', 'r+') as slack_file:
            file_content = slack_file.read()
            if order_str not in file_content:
                slack_file.write(f"{payload['attachments'][0]['fields'][0]['value']}\n")
                requests.post('https://hooks.slack.com/services/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', data=json.dumps(payload, sort_keys=True, indent=4))

# Post completion message to Slack
def post_completion_message():
    now_utc = datetime.now(timezone('UTC'))
    now_pacific = now_utc.astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S %Z%z")
    client = slack.WebClient(token=slack['SLACK_BOT_TOKEN'])
    client.chat_postMessage(
        channel='#bi_api_test',
        text=f"ITAR check completed on {now_pacific} for the following data \n"
    )

def main():
    consolidated_screening_list_df = download_trade_gov_csv(TRADE_GOV_URL)
    netsuite_df = download_netsuite_report(NETSUITE_REPORT_URL)
    processed_netsuite_df = preprocess_netsuite_data(netsuite_df)
    results = compare_orders_with_screening_list(processed_netsuite_df, consolidated_screening_list_df)
    if results:
        send_to_slack(processed_netsuite_df, consolidated_screening_list_df, results)
        post_completion_message()

if __name__ == "__main__":
    main()
