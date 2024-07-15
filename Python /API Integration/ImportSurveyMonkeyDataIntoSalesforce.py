### Changes and Enhancements:
##1. **Comments**: Added comments for each function and significant blocks of code to explain their purpose and functionality.
#2. **Error Handling**: Improved error handling by checking the status code directly instead of comparing the response object to a string.
#3. **Code Organization**: Reorganized some of the repetitive logic into functions to improve readability and maintainability.
#4. **Variable Naming**: Used more descriptive variable names where necessary for better clarity.
#5. **Slack Notifications**: Added more specific Slack notifications to inform about successful connections and data loads.

import requests
import json
import urllib.parse
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from simple_salesforce import Salesforce
from datetime import date
import pandas as pd
from credentials import survey_monkey, salesforce, slack
from utils.helpers import SalesforceRpts

# Get today's date
today = date.today()
today_date = today.strftime("%B %d, %Y")

def post_message_to_slack(text, blocks=None):
    """
    Sends a message to Slack.
    :param text: Message text to send
    :param blocks: Optional blocks to send as part of the message
    """
    return requests.post('https://slack.com/api/chat.postMessage', {
        'token': slack['SLACK_BOT_TOKEN'],
        'channel': 'bi_jobs_slack_notifications',
        'text': text,
        'icon_emoji': ':survey_monkey:',
        'username': 'username@username.com',
        'blocks': json.dumps(blocks) if blocks else None
    }).json()

def get_survey_monkey_data():
    """
    Retrieves data from SurveyMonkey API.
    :return: List of recipient data with email statuses
    """
    s = requests.session()
    s.headers.update({
        "Authorization": f"Bearer {survey_monkey['token']}",
        "Content-Type": "application/json"
    })

    collector_messages_url = f"https://api.surveymonkey.com/v3/collectors/{survey_monkey['collector_id']}/messages/?page=1&per_page=1"
    try:
        collector_messages_response = s.get(collector_messages_url)
    except requests.ConnectionError:
        print("Failed to connect")
        post_message_to_slack("Survey Monkey Data Load: Unable to connect to Survey Monkey APIs")
        return []

    collector_messages_details = collector_messages_response.json()
    if 'error' in collector_messages_details:
        post_message_to_slack("Survey Monkey Data Load: Rate Limit Reached for Survey Monkey APIs")
        return []

    max_page_number = 7
    collectors_messages_recipients_list = []
    keys = ('recipient_email', 'message_id', 'recipient_id', 'last_email_status')

    for each_page in range(1, max_page_number + 1):
        collector_messages_url = f"https://api.surveymonkey.com/v3/collectors/{survey_monkey['collector_id']}/messages/?page={each_page}&per_page=50"
        try:
            collector_messages_response = s.get(collector_messages_url)
            post_message_to_slack(f"Survey Monkey Data Load: Connected to Survey Monkey APIs {collector_messages_url} on {today_date}")
        except requests.ConnectionError:
            post_message_to_slack(f"Survey Monkey Data Load: Unable to Connect to Survey Monkey APIs on {today_date}")
            continue

        if collector_messages_response.status_code == 429:
            break

        collector_messages_details = collector_messages_response.json()
        if 'data' in collector_messages_details:
            for email_message in collector_messages_details['data']:
                message_id = email_message['id']
                collector_messages_stats_url = f"https://api.surveymonkey.com/v3/collectors/{survey_monkey['collector_id']}/messages/{message_id}/stats/"
                try:
                    collector_messages_stats_response = s.get(collector_messages_stats_url)
                except requests.ConnectionError:
                    post_message_to_slack(f"Survey Monkey Data Load: Unable to Connect to Survey Monkey APIs on {today_date}")
                    continue

                collector_messages_stats_details = collector_messages_stats_response.json()

                last_survey_response_status = ""
                if 'survey_response_status' in collector_messages_stats_details:
                    survey_response_status = collector_messages_stats_details['survey_response_status']
                    response_status = {
                        'Responded to Survey': survey_response_status['completely_responded'],
                        'Not Responded to Survey': survey_response_status['not_responded'],
                        'Responded to Partial Survey': survey_response_status['partially_responded'],
                    }
                    last_survey_response_status = " ".join([key for key, value in response_status.items() if value == 1])

                last_email_status = ""
                if 'mail_status' in collector_messages_stats_details:
                    mail_status = collector_messages_stats_details['mail_status']
                    email_status = {
                        'Email Sent': mail_status['sent'],
                        'Email Opened': mail_status['opened'],
                        'Email not Sent': mail_status['not_sent'],
                        'Opt Out from Survey Emails': mail_status['opted_out'],
                        'Email Bounced': mail_status['bounced'],
                        'Survey Link Clicked': mail_status['link_clicked'],
                    }
                    last_email_status = " -> ".join([key for key, value in email_status.items() if value == 1])
                last_email_status += ' -> ' + last_survey_response_status

                message_recipient_href = email_message['href'] + '/recipients'
                try:
                    collector_messages_recipients_response = s.get(message_recipient_href)
                except requests.ConnectionError:
                    post_message_to_slack(f"Survey Monkey Data Load: Unable to Connect to Survey Monkey APIs on {today_date}")
                    continue

                collector_messages_recipients_details = collector_messages_recipients_response.json()

                if 'data' in collector_messages_recipients_details and len(collector_messages_recipients_details['data']):
                    recipient_id = collector_messages_recipients_details['data'][0]['id']
                    recipient_email = collector_messages_recipients_details['data'][0]['email']
                else:
                    recipient_id = ''
                    recipient_email = ''

                values = (recipient_email, message_id, recipient_id, last_email_status)
                collectors_messages_recipients_dict = dict(zip(keys, values))
                collectors_messages_recipients_list.append(collectors_messages_recipients_dict.copy())

    return collectors_messages_recipients_list

def get_salesforce_data(sf_object, sf_query):
    """
    Retrieves data from Salesforce using Bulk API.
    :param sf_object: Salesforce object name (Lead, Contact)
    :param sf_query: SOQL query to retrieve data
    :return: List of Salesforce data
    """
    bulk = SalesforceRpts().api_auth()
    job = bulk.create_query_job(sf_object, concurrency='Parallel')
    batch = bulk.query(job, sf_query)
    bulk.wait_for_batch(job, batch)
    sf_data = []
    keys = ("recipient_email", "recipient_id", "Id")

    for result in bulk.get_all_results_for_query_batch(batch, job):
        for row in result:
            if row != b'Records not found for this query':
                list_elements = row.decode('utf-8').replace('"', '').replace('\n', '').split(",")
                values = (list_elements[0], list_elements[1], list_elements[2])
                sf_data.append(dict(zip(keys, values)))

    bulk.close_job(job)
    return sf_data

def update_salesforce_data(sf_object, update_data):
    """
    Updates Salesforce data using Bulk API.
    :param sf_object: Salesforce object name (Lead, Contact)
    :param update_data: Data to be updated in Salesforce
    """
    bulk = SalesforceRpts().api_auth()
    job = bulk.create_update_job(sf_object, contentType='CSV', concurrency='Parallel')
    batch = bulk.post_batch(job, CsvDictsAdapter(iter(update_data)))
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)

def merge_and_prepare_data(survey_data, sf_data):
    """
    Merges SurveyMonkey data with Salesforce data and prepares it for update.
    :param survey_data: Data retrieved from SurveyMonkey
    :param sf_data: Data retrieved from Salesforce
    :return: Merged and prepared data for update
    """
    survey_df = pd.DataFrame(survey_data)
    salesforce_df = pd.DataFrame(sf_data)
    df_inner = pd.merge(salesforce_df, survey_df, on='recipient_email', how='inner')
    df_inner = df_inner[['Id', 'last_email_status', 'recipient_id_x']]
    df_inner = df_inner.rename(columns={'Id': 'Id', 'last_email_status': 'Survey_Monkey_Email_Click_Through_Rate__c', 'recipient_id_x': 'Survey_Recipient_ID__c'})
    return [val for key, val in df_inner.T.to_dict().items()]

def main():
    """
    Main function to orchestrate the data load process from SurveyMonkey to Salesforce.
    """
    # Retrieve data from SurveyMonkey
    survey_data = get_survey_monkey_data()
    if not survey_data:
        return

    # Define Salesforce queries
    sf_lead_query = "SELECT Email, Survey_Recipient_ID__c, Id, ConvertedContactId FROM Lead WHERE Survey_Recipient_ID__c != '' AND ConvertedContactId = ''"
    sf_contact_query = "SELECT Email, Survey_Recipient_ID__c, Id FROM Contact WHERE Survey_Recipient_ID__c != '' AND IsDeleted = false"

    # Retrieve Salesforce data for leads and contacts
    sf_lead_data = get_salesforce_data('Lead', sf_lead_query)
    sf_contact_data = get_salesforce_data('Contact', sf_contact_query)

    # Merge and prepare data for Salesforce update
    lead_update_data = merge_and_prepare_data(survey_data, sf_lead_data)
    contact_update_data = merge_and_prepare_data(survey_data, sf_contact_data)

    # Update Salesforce leads
    if lead_update_data:
        update_salesforce_data('Lead', lead_update_data)
    else:
        post_message_to_slack(f"Survey Monkey Data Load: No leads to update on {today_date}")

    # Update Salesforce contacts

    if contact_update_data:
        update_salesforce_data('Contact', contact_update_data)
    else:
        post_message_to_slack(f"Survey Monkey Data Load: No contacts to update on {today_date}")

    # Notify completion
    post_message_to_slack(f"Survey Monkey API Data Load Completed on {today_date}")

if __name__ == "__main__":
    main()
