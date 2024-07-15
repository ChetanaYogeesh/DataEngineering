'''
Classes and Functions:

SalesforceRpts: Handles Salesforce authentication.
get_salesforce_data: Retrieves data from Salesforce using a given object name and query.
post_members_to_list: Posts member data to a Mailchimp list.
post_message_to_slack: Posts a message to a Slack channel.
main: Orchestrates the process, including querying Salesforce, combining data, posting to Mailchimp, and notifying via Slack.
Code Optimization:

Removed redundant imports and improved code readability by organizing imports and functions logically.
Used list comprehensions and dictionary comprehensions for concise data handling.
Comments:

Added comments to explain the purpose and functionality of each class, function, and critical code section.
'''

from datetime import date, datetime
import pandas as pd
import requests, json, urllib.parse
from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce
from credentials import *

# Salesforce Report Class for Authentication and API Access
class SalesforceRpts:
    def __init__(self):
        session = requests.Session()
        self.sf = Salesforce(
            username=salesforce['user'],
            password=salesforce['pwd'],
            security_token=salesforce['security_token'],
            organizationId=salesforce['organizationId'],
            session=session
        )
        self.headers = self.sf.headers
        self.sid = self.sf.session_id

    def api_auth(self):
        return SalesforceBulk(
            sessionId=self.sid, 
            host=urllib.parse.urlparse('https://companyname.my.salesforce.com').hostname, 
            API_version="40.0"
        )

# Function to retrieve Salesforce data based on object name and query
def get_salesforce_data(object_name, query):
    bulk = SalesforceRpts().api_auth()
    job = bulk.create_query_job(object_name, concurrency='Parallel')
    batch = bulk.query(job, query)
    bulk.wait_for_batch(job, batch)

    data = []
    keys = ("Id", "Email", "FirstName", "LastName", "Content__c", "MarketingRecycleLeadsStatus__c")
    for result in bulk.get_all_results_for_query_batch(batch, job):
        for row in result:
            if row != b'Records not found for this query':
                list_elements = row.decode('utf-8').replace('"', '').replace('\n', '').split(",")
                values = tuple(list_elements[i] for i in range(len(keys)))
                data.append(dict(zip(keys, values)))
    
    bulk.close_job(job)
    return data[1:]  # Skip header

# Function to post members to Mailchimp list
def post_members_to_list(**kwargs):
    payload = {
        'email_address': kwargs['Email'],
        'status': 'subscribed',
        'merge_fields': {
            'FNAME': kwargs['FirstName'],
            'LNAME': kwargs['LastName'],
            'SFID': kwargs['Id'],
            'type': kwargs['Id'],
            'content': kwargs['Content__c'],
            'MarketingRecycleEmailStatus': kwargs['MarketingRecycleLeadsStatus__c']
        }
    }
    print(payload)
    return requests.post(
        mailchimp['url'],
        auth=(mailchimp['user'], mailchimp['api_key']),
        data=json.dumps(payload)
    ).json()

# Function to post message to Slack channel
def post_message_to_slack(text, blocks=None):
    return requests.post(
        'https://slack.com/api/chat.postMessage',
        {
            'token': slack['SLACK_BOT_TOKEN'],
            'channel': 'bi_jobs_slack_notifications',
            'text': text,
            'icon_url': 'https://19321.apps.zdusercontent.com/19321/assets/1588752058-af0bf92adc0c96f42e55d30a354ce634/logo.png',
            'username': 'username@username.com',
            'blocks': json.dumps(blocks) if blocks else None
        }
    ).json()

def main():
    # Salesforce Lead Query
    lead_query = """
    SELECT Id, Email, FirstName, LastName, Content__c, MarketingRecycleLeadsStatus__c
    FROM Lead
    WHERE IsConverted = False
    AND (MarketingRecycleLeadsStatus__c LIKE '%Subscribed%' OR MarketingRecycleLeadsStatus__c = '')
    AND (Status = 'Marketing Recycle')
    AND Date_Added_To_Drip_Campaign__c = LAST_N_DAYS:3
    AND Content__c != 'Cross_Country_Drive_Test'
    """

    # Salesforce Contact Query
    contact_query = """
    SELECT Id, Email, FirstName, LastName, Content__c, MarketingRecycleLeadsStatus__c
    FROM Contact
    WHERE (Order_Number__c = '0' OR Order_Number__c IS NULL)
    AND (contact_total_orders__c = '0' OR contact_total_orders__c IS NULL)
    AND MC_Email_Status__c LIKE 'Subscribed%'
    AND (MarketingRecycleLeadsStatus__c = 'Subscribed' OR MarketingRecycleLeadsStatus__c = '')
    AND Is_Employee__c = FALSE
    AND Email IS NOT NULL
    AND AccountId IS NOT NULL
    AND CreatedDate = LAST_N_DAYS:3
    AND Content__c != 'Cross_Country_Drive_Test'
    """

    leads = get_salesforce_data('Lead', lead_query)
    contacts = get_salesforce_data('Contact', contact_query)
    all_records = leads + contacts

    df = pd.DataFrame(all_records)
    records_list = [val for key, val in df.T.to_dict().items()]

    for member in records_list:
        post_members_to_list(**member)

    today_date = date.today().strftime("%B %d, %Y")
    text = f"Marketing Recycle Drip Campaign Data Upload Completed on {today_date}"
    post_message_to_slack(text)

if __name__ == "__main__":
    main()
