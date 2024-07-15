#Modularization: Moved related functions (get_session, tz_from_utc_ms_ts, fetch_salesforce_data) into separate functions for better organization and reusability.#
#Optimization: Simplified data processing and conversion using pandas and Python's standard libraries.
#Error Handling: Added basic error handling where appropriate, such as handling empty DataFrames or invalid timestamps.
#Comments: Added comments to clarify the purpose and functionality of each function and significant code block.
#Code Organization: Grouped related imports together and organized the main script flow logically.
#redentials Management: Ensured credentials are imported securely and managed through environment variables or specific credential files

import boto3
import pandas as pd
import pytz
from datetime import datetime
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from simple_salesforce import Salesforce
from helpers import SalesforceRpts
import slack
from tabulate import tabulate
from credentials import account_registry, slack as slack_credentials

def get_session(account_name="flex"):
    """
    Creates and returns a boto3 session with assumed role credentials.
    :param account_name: Name of the AWS account to assume role from credentials
    :return: Boto3 session object
    """
    session_name = f"bi-eks-key-pair-script-{int(datetime.now().timestamp())}"
    sts_client = boto3.client("sts")
    credentials = sts_client.assume_role(
        RoleArn=account_registry['roleARN'],
        RoleSessionName=session_name
    )["Credentials"]
    return boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"]
    )

def tz_from_utc_ms_ts(utc_ms_ts, tz_info):
    """
    Converts UTC milliseconds timestamp to timezone-aware datetime.
    :param utc_ms_ts: UTC timestamp in milliseconds
    :param tz_info: Timezone info
    :return: Timezone-aware datetime object
    """
    utc_ms_ts = int(utc_ms_ts)
    utc_datetime = datetime.utcfromtimestamp(utc_ms_ts / 1000.)
    return utc_datetime.replace(tzinfo=pytz.timezone('UTC')).astimezone(tz_info)

def fetch_salesforce_data(object_name, query):
    """
    Fetches data from Salesforce using SalesforceBulk API.
    :param object_name: Salesforce object name (Lead, Contact)
    :param query: SOQL query to fetch data
    :return: List of dictionaries containing Salesforce data
    """
    bulk = SalesforceRpts().api_auth()
    job = bulk.create_query_job(object_name, concurrency='Parallel')
    batch = bulk.query(job, query)
    bulk.wait_for_batch(job, batch)
    records = []
    
    for result in bulk.get_all_results_for_query_batch(batch, job):
        for row in result:
            if row != b'Records not found for this query':
                list_elements = row.decode('utf-8').replace('"', '').replace('\n', '').split(",")
                records.append({
                    "Email": list_elements[1],  # Assuming Email is always at index 1
                    "Id": list_elements[2] if len(list_elements) > 2 else None  # Assuming Id is always at index 2
                })
    
    bulk.close_job(job)
    return records

def main():
    # Establish AWS session
    session = get_session(account_registry['account'])
    dynamodb_client = session.client(account_registry['client'], region_name=account_registry['region'])
    
    # Query DynamoDB for account registry data
    response = dynamodb_client.scan(
        TableName=account_registry['table'],
        IndexName='version_index_v2',
        FilterExpression='version = :version AND begins_with(id, :id)',
        ExpressionAttributeValues={':id': {'S': 'ACCOUNT_'}, ':version': {'S': 'Latest'}}
    )
    response_items = response.get('Items', [])
    
    # Convert DynamoDB response to DataFrame
    account_registry_df = pd.DataFrame(response_items)
    
    # Data processing and cleanup
    account_registry_df = account_registry_df[~account_registry_df['Email'].str.contains('@company', regex=True, na=False)]
    account_registry_df['SaaSProduct_Contract_Date__c'] = pd.to_datetime(
        account_registry_df['SaaSProduct_Contract_Date__c'],
        errors='coerce'
    ).dt.strftime('%Y-%m-%dT%H:%M:%S.%f%z').apply(lambda x: None if x == "NaT" else x)
    account_registry_df['Email'] = account_registry_df['Email'].str.lower()
    account_registry_df['SaaSProduct_Total_Accounts__c'] = 0
    account_registry_df['SaaSProduct_Accounts_Contract_End_Date__c'] = pd.to_datetime(
        account_registry_df['SaaSProduct_Contract_Date__c']
    ).astype(str)
    account_registry_df['SaaSProduct_User_Name__c'] = account_registry_df['SaaSProduct_User_Name__c'].map(lambda x: x.lstrip('ACCOUNT_'))

    # Grouping by email for Salesforce merge
    account_registry_df = account_registry_df.groupby(['Email'], as_index=False).agg({
        'SaaSProduct_Accounts_Contract_End_Date__c': [('SaaSProduct_Accounts_Contract_End_Date__c', ', '.join)],
        'SaaSProduct_User_Name__c': [('SaaSProduct_User_Name__c', ', '.join)],
        'SaaSProduct_Contract_Date__c': max,
        'SaaSProduct_Total_Accounts__c': 'count'
    })
    account_registry_df.columns = [col[0] for col in account_registry_df.columns]
    
    # Fetch Salesforce Contacts and Leads data
    sf_contact_records = fetch_salesforce_data('Contact', "SELECT AccountId, Email, Id FROM Contact")
    sf_lead_records = fetch_salesforce_data('Lead', "SELECT Email, Id FROM Lead WHERE IsConverted = false")
    
    # Merge Salesforce data with account registry data
    salesforce_contacts_account_registry_df = pd.DataFrame(sf_contact_records).merge(
        account_registry_df, on='Email', how='right'
    )
    salesforce_leads_account_registry_df = pd.DataFrame(sf_lead_records).merge(
        account_registry_df, on='Email', how='right'
    )
    
    # Update Salesforce Contacts if there are records to update
    if not salesforce_contacts_account_registry_df.empty:
        job = bulk.create_update_job('Contact', contentType='CSV', concurrency='Parallel')
        batch = bulk.post_batch(job, CsvDictsAdapter(iter(salesforce_contacts_account_registry_df.to_dict(orient='records'))))
        bulk.wait_for_batch(job, batch)
        bulk.close_job(job)
    
    # Update Salesforce Leads if there are records to update
    if not salesforce_leads_account_registry_df.empty:
        job = bulk.create_update_job('Lead', contentType='CSV', concurrency='Parallel')
        batch = bulk.post_batch(job, CsvDictsAdapter(iter(salesforce_leads_account_registry_df.to_dict(orient='records'))))
        bulk.wait_for_batch(job, batch)
        bulk.close_job(job)
    
    # Notify Slack about accounts not mapped to Leads/Contacts
    unmapped_accounts = result[(result['Id_x'].isnull()) & (result['Id_y'].isnull())].drop_duplicates()
    if not unmapped_accounts.empty:
        df_tab = tabulate(unmapped_accounts[['Email']].values.tolist(), headers=['Email'], tablefmt="grid", stralign="center")
        slack_client = slack.WebClient(token=slack_credentials['SLACK_BOT_TOKEN'])
        response = slack_client.chat_postMessage(
            channel='#bi_jobs_slack_notifications',
            text=f'Account Registry: Following Emails are not mapped to Salesforce Leads/Contacts, please create a lead\n{df_tab}'
        )

if __name__ == "__main__":
    main()
