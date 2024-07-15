# gets data from stripe stage tables in Dynamo DB (via stripe_events_dynamodb.py Lambda function)
# inserts data into Salesforce
# truncates stripe stage tables when complete
# customer should be already be created in Salesforce via Zapier if none already exists
# inserts into stripe schema in DW

from helpers import SalesforceRpts, MySQLRpts
from datetime import datetime
import credentials
import collections
import subprocess
import requests
import decimal
import hashlib
import stripe
import boto3
import json
import copy
import sys

is_test = False


class DecimalEncoder(json.JSONEncoder):
    """
    helper class to convert a DynamoDB item to json
    """
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def get_all_dynamo_data(table_name):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table(table_name)
    response = table.scan()
    page_1 = [json.loads(json.dumps(i, cls=DecimalEncoder)) for i in response['Items']]
    all_pages = []
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        for i in response['Items']:
            all_pages.append(json.loads(json.dumps(i, cls=DecimalEncoder)))
    return all_pages if all_pages else page_1

# get summary data
data = get_all_dynamo_data('stripe_stage')
if not data:
    MySQLRpts().etl_log('Stripe Logs', 'Dynamo', 'DW+SF')
    sys.exit()


def get_email(cust_id):
    api_key = credentials.stripe['api_key_secret_test'] if is_test else credentials.stripe['api_key_secret_live']
    return stripe.Customer.retrieve(cust_id, api_key=api_key)['email'] if cust_id else None

for dicts in data:
    if dicts['customer_id'] != 'null':
        try:
            dicts['email'] = get_email(dicts['customer_id'])
        except stripe.error.InvalidRequestError:
            dicts['email'] = None
        except KeyError:
            dicts['email'] = None
    else:
        dicts['email'] = None


class SalesforceRest:
    def __init__(self):
        self.payload = {
            'grant_type': 'password',
            'client_id': credentials.salesforce['consumer_key'],
            'client_secret': credentials.salesforce['consumer_secret'],
            'username': credentials.salesforce['user'],
            'password': credentials.salesforce['pwd']
        }
        self.token = requests.post('https://login.salesforce.com/services/oauth2/token',
                                   data=self.payload).json()['access_token']
        self.headers = {'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'}

    def get_sf_contact_id(self, email):
        try:
            return requests.get("https://na43.salesforce.com/services/data/v36.0/query?q="
                                "SELECT+Id,AccountId+FROM+Contact+WHERE+Email='%s'+ORDER+BY+CreatedDate+DESC" % email,
                                headers=self.headers).json()['records'][0]
        except IndexError:
            pass


# clean out unicode strings b/c of Dynamo to json tweak
def encode_dict(d, codec='utf8'):
    ks = d.keys()
    for keys in ks:
        val = d.pop(keys)
        if isinstance(val, unicode):
            val = val.encode(codec)
        elif isinstance(val, dict):
            val = encode_dict(val, codec)
        if isinstance(keys, unicode):
            keys = keys.encode(codec)
        d[keys] = val
    return d


def prep_data(list_of_dicts, destination='salesforce'):
    dt_insert = []
    for x in copy.deepcopy(list_of_dicts):
        encode_dict(x)
        for y in x:
            x[y] = None if x[y] == 'null' else x[y]
        if destination == 'mysql':
            x['event_datetime'] = datetime.fromtimestamp(x['created']).strftime('%Y-%m-%d %H:%M:%S')
            x['event_unixtimestamp'] = x.pop('created')
            x['event_type'] = x.pop('type')
            try:
                x['email'] = hashlib.md5(x.pop('email')).hexdigest()
            except TypeError:
                x['email'] = None
            dt_insert.append(x)
        elif x['customer_id']:
            contact_id = SalesforceRest().get_sf_contact_id(x.pop('email'))
            if not contact_id:
                pass
            else:
                x['Contact__c'] = contact_id['Id']
                x['Event_Date__c'] = datetime.fromtimestamp(x.pop('created')).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                x['Name'] = x['id']
                x['Event_ID__c'] = x.pop('id')
                x['Event__c'] = x.pop('type').replace('.', ' ').replace('_', ' ').title()
                x['Amount__c'] = float(x.pop('amount')) / 100
                x['Amount_Due__c'] = float(x.pop('amount_due')) / 100
                x['Amount_Refunded__c'] = float(x.pop('amount_refunded')) / 100
                x['Stripe_Customer_ID__c'] = x.pop('customer_id')
                x['Object__c'] = x.pop('object').title()
                x['Object_Event_ID__c'] = x.pop('object_event_id')
                try:
                    x['Account__c'] = contact_id['AccountId'] if contact_id['AccountId'] else ''
                except KeyError:
                    x['Account__c'] = ''
                dt_insert.append(x)
        else:
            pass
    return dt_insert

mysql_data = prep_data(data, destination='mysql')
ins_stmt = MySQLRpts().create_stmt('stripe.event_log', col_list=mysql_data[0].keys())
MySQLRpts().insert_data(ins_stmt, mysql_data)
max_date = MySQLRpts().get_select_data("select from_unixtime(%s)" % sorted([dico['created'] for dico in data],
                                                                           reverse=True)[0])[0].values()[0]
MySQLRpts().exec_simple('insert into fed.stripe_event_log_fed select * from stripe.event_log;')
sf_data = prep_data(data, destination='salesforce')

if sf_data:
    SalesforceRpts().api_upsert('Skylark_Subscription_Event__c', 'Event_ID__c', sf_data)

# get raw log object data, insert into DW, truncate Dynamo staging table
data_obj_raw = get_all_dynamo_data('stripe_object_stage')


def remove_unicode(nested_dict):
    if isinstance(nested_dict, basestring):
        return str(nested_dict.encode('utf8'))
    elif isinstance(nested_dict, collections.Mapping):
        return dict(map(remove_unicode, nested_dict.iteritems()))
    elif isinstance(nested_dict, collections.Iterable):
        return type(nested_dict)(map(remove_unicode, nested_dict))
    else:
        return nested_dict

data_obj = [remove_unicode(stg) for stg in data_obj_raw]
for xx in data_obj:
    xx['event_unixtimestamp'] = xx.pop('created')
    xx['event_datetime'] = datetime.fromtimestamp(xx['event_unixtimestamp']).strftime('%Y-%m-%d %H:%M:%S')
    for k, v in xx.items():
        xx[k] = str(xx.pop(k)) if k != 'data' else json.dumps(xx.pop(k))

ins_stmt_obj = MySQLRpts().create_stmt('stripe.event_object_raw', col_list=data_obj[0].keys())
MySQLRpts().insert_data(ins_stmt_obj, data_obj)

# call file to create object tables
min_event = min(set((int(x['event_unixtimestamp'])-(2*60*60)) for x in data_obj))
objects = set(x['object'] for x in data_obj)


def call_object_script(obj, mintimestamp):
    subprocess.call(
        'python -W"ignore" skylark/stripe_etl/stripe_dw_objects.py '
        '--object %s '
        '--mintimestamp %s' % (obj, mintimestamp),
        shell=True
    )

for objs in objects:
    call_object_script(objs, min_event)


# truncate dynamo stage tables
def delete_dynamo_item(table, primary_col, primary_key, data_type='S'):
    client = boto3.client('dynamodb', region_name='us-east-1')
    client.delete_item(Key={primary_col: {data_type: str(primary_key)}}, TableName=table)

for items in data_obj_raw:
    delete_dynamo_item('stripe_object_stage', 'id', items['id'], data_type='S')

for items in data:
    delete_dynamo_item('stripe_stage', 'id', items['id'], data_type='S')

MySQLRpts().etl_log('Stripe Logs', 'Dynamo', 'DW+SF', max_date=max_date)
