from jira import JIRA
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from utils.helpers import SalesforceRpts
import os
import sys
import ast
import pandas as pd
from datetime import datetime

def fetch_salesforce_data(sf_object, sf_query, keys):
    """
    Fetches data from Salesforce using SalesforceBulk API.
    :param sf_object: Salesforce object name
    :param sf_query: SOQL query to fetch data
    :param keys: Tuple of keys for dictionary creation
    :return: List of dictionaries containing Salesforce data
    """
    bulk = SalesforceRpts.api_auth(SalesforceRpts())
    job = bulk.create_query_job(sf_object, concurrency='Parallel')
    batch = bulk.query(job, sf_query)
    bulk.wait_for_batch(job, batch)
    
    result_list = []
    for result in bulk.get_all_results_for_query_batch(batch, job, chunk_size=2048):
        for row in result:
            new_str = row.decode('utf-8')
            new_list = ast.literal_eval(new_str)
            result_dict = dict(zip(keys, new_list))
            result_list.append(result_dict.copy())
    
    bulk.close_job(job)
    return result_list

def main():
    # Salesforce queries and objects
    sf_site_object = 'sitetracker__Site__c'
    sf_site_query = 'SELECT Id, Name, companyname_Site_Code__c From sitetracker__Site__c'
    sf_user_object = 'User'
    sf_user_query = 'SELECT Email, Id, Name FROM User'
    
    # Fetch Salesforce data
    SitecompanynameCodesNames_dict = {}
    FieldTechProfilecompanynameUsers_dict = {}
    
    # Fetch Site data
    SitecompanynameCodesNames_dict = {row['companyname_Site_Code__c']: row['Name'] for row in fetch_salesforce_data(sf_site_object, sf_site_query, ('Id', 'Name', 'companyname_Site_Code__c'))}
    
    # Fetch User data
    FieldTechProfilecompanynameUsers_dict = {row['Email']: row['Id'] for row in fetch_salesforce_data(sf_user_object, sf_user_query, ('Id', 'Name', 'Email'))}
    
    # JIRA authentication and query
    jira_auth = JIRA(options=jiraNO['options'], basic_auth=(jiraNO['username'], jiraNO['api_token']))
    dispatch_jql = "project = \"NO\" AND (assignee=\"Michael McDonald\")"
    
    # List to store data for upload to Salesforce
    upload_jira_to_sitetracker_list = []
    
    block_size = 100
    block_num = 0
    
    # Iterate over JIRA issues
    while True:
        start_idx = block_num * block_size
        issues = jira_auth.search_issues(dispatch_jql, start_idx, block_size)
        
        for issue in issues:
            jira_link = f"https://companyname-nav.atlassian.net/browse/{issue.key}"
            jira_companyname_site_code = issue.fields.summary[:4]
            
            if issue.fields.components:
                jira_component = issue.fields.components[0]
            else:
                jira_component = 'Unknown'
            
            issue_root_cause = str(issue.fields.customfield_10041).strip('[]\'')
            
            if issue.fields.assignee:
                assignee_name = issue.fields.assignee.displayName
                user_assignee_id = FieldTechProfilecompanynameUsers_dict.get(assignee_name, 'Owner Name')
            else:
                assignee_name = 'Owner Name'
                user_assignee_id = 'Owner Name'
            
            if jira_companyname_site_code in SitecompanynameCodesNames_dict:
                site_name = SitecompanynameCodesNames_dict[jira_companyname_site_code]
                
                values = (
                    site_name,
                    jira_companyname_site_code,
                    issue.key,
                    jira_link,
                    issue.fields.summary,
                    issue.fields.status,
                    user_assignee_id,
                    issue.fields.created[:10],
                    issue.fields.updated[:10],
                    jira_component,
                    issue.fields.description,
                    'Maintenance',
                    issue_root_cause
                )
                
                sitetracker_companyname_site_codes_dict = dict(zip(keys, values))
                upload_jira_to_sitetracker_list.append(sitetracker_companyname_site_codes_dict.copy())
            else:
                print(f"{issue.key} {jira_companyname_site_code} site code does not exist")
        
        if len(issues) == 0:
            break
        block_num += 1
    
    # Salesforce bulk API for upsert
    sf_object = 'sitetracker_wm__Ticket__c'
    upsert_key = 'Ticket_Number__c'
    bulk = SalesforceRpts.api_auth(SalesforceRpts())
    job = bulk.create_upsert_job(sf_object, upsert_key, contentType='CSV', concurrency='Parallel')
    
    csv_iter = CsvDictsAdapter(iter(upload_jira_to_sitetracker_list))
    batch = bulk.post_batch(job, csv_iter)
    
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)

if __name__ == "__main__":
    main()
