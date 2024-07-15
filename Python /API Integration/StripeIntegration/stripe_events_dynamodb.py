'''
Explanation
Classes and Inheritance:

Initialization: Handles initialization tasks including environment variable decryption and response formatting.
DataHandler: Validates and processes Stripe webhook payloads, extracting customer IDs and preparing data for insertion into DynamoDB.
DataInserter: Inherits from DataHandler to manage DynamoDB interactions, including inserting cleaned data and handling exceptions.
Methods and Properties:

Initialization: respond formats API Gateway responses. validate_payload verifies Stripe webhook signatures.
DataHandler: customer_id and payload_type properties extract customer IDs and determine payload type. add_customer_fields enhances data with additional fields.
DataInserter: clean_empty removes empty values recursively. insert_dynamo and insert_dynamo_summary insert data into DynamoDB tables based on payload type.
Lambda Function:

lambda_handler: Entry point for AWS Lambda function, initializes logging, processes webhook data, and handles exceptions while logging errors.
Enhancements
Modularity: Each class focuses on specific responsibilities (initialization, data handling, data insertion).
Error Handling: Proper exception handling ensures robustness, with detailed logging for debugging.
Optimization: Improved readability and performance through streamlined code and logical organization.
Comments: Added comments to explain functionality and provide clarity on each method's purpose.

'''


import logging
import json
import stripe
import boto3
import os
import random
from base64 import b64decode

class Initialization:
    def __init__(self):
        self.live = True  # Flag to switch between testing and live environments
        self.kms = boto3.client('kms')
        # Decrypt environment variables based on environment (live or test)
        self.es = self.kms.decrypt(CiphertextBlob=b64decode(os.environ['es']))['Plaintext'] if self.live else self.kms.decrypt(CiphertextBlob=b64decode(os.environ['es_test']))['Plaintext']  # signing secret

    @staticmethod
    def respond(err, res=None):
        # Utility function to generate API Gateway response format
        return {
            'statusCode': '400' if err else '200',
            'body': err.message if err else json.dumps(res),
            'headers': {'Content-Type': 'application/json'},
        }

    def validate_payload(self, **kwargs):
        # Validate Stripe webhook payload using Stripe SDK
        payload = kwargs['body']
        sig_header = kwargs['headers']['Stripe-Signature']
        return stripe.Webhook.construct_event(payload, sig_header, self.es)


class DataHandler(Initialization):
    def __init__(self, **kwargs):
        super().__init__()
        self.data = self.validate_payload(**kwargs)

    @property
    def customer_id(self):
        # Extract customer ID from webhook data
        stng = str(self.data)
        return ('cus_' + stng.partition('cus_')[2].partition("'")[0]).split('/')[0].split(',')[0].replace('"', '').replace('/', '') if 'cus_' in stng else None

    @property
    def payload_type(self):
        # Determine payload type ('customer' or 'system')
        return 'customer' if self.customer_id else 'system'

    def add_customer_fields(self):
        # Modify data to include additional fields for customer records
        if self.payload_type == 'customer':
            self.data['customer_id'] = self.customer_id if self.live else 'cus_BDiNd2KIUXyrg4'
            self.data['id'] = self.data['id'] if self.live else ''.join([random.choice('0123456789ABCDEF') for x in range(6)])
            self.data['created'] = self.data['created'] if self.live else int(''.join([random.choice('0123456789') for x in range(6)]))
        elif not self.live:
            self.data['id'] = ''.join([random.choice('0123456789ABCDEF') for x in range(6)])
            self.data['created'] = int(''.join([random.choice('0123456789') for x in range(6)]))
        else:
            pass
        return self.data

    def get_id(self, id_string):
        # Extract specific ID from webhook data
        stng = str(self.data)
        return (id_string + stng.partition(id_string)[2].partition('"')[0]).split('/')[0].split(',')[0].replace('"', '').replace('/', '') if id_string in stng else None

    @property
    def data_insert(self):
        # Prepare data for insertion into DynamoDB (including additional fields)
        return self.add_customer_fields()

    @property
    def data_summary_insert(self):
        # Prepare summary data for insertion into DynamoDB
        mysql_dict = {
            'id': self.data_insert['id'],
            'created': self.data_insert['created'],
            'type': self.data_insert['type'],
            'object': self.data_insert['data']['object']['object'],
        }
        try:
            mysql_dict['customer_id'] = self.data_insert['customer_id']
        except KeyError:
            mysql_dict['customer_id'] = 'null'
        try:
            mysql_dict['amount'] = self.data_insert['data']['object']['amount']
        except KeyError:
            mysql_dict['amount'] = 'null'
        try:
            mysql_dict['amount_due'] = self.data_insert['data']['object']['amount_due']
        except KeyError:
            mysql_dict['amount_due'] = 'null'
        try:
            mysql_dict['amount_refunded'] = self.data_insert['data']['object']['amount_refunded']
        except KeyError:
            mysql_dict['amount_refunded'] = 'null'
        try:
            mysql_dict['object_event_id'] = self.data_insert['data']['object']['id']
        except KeyError:
            mysql_dict['object_event_id'] = 'null'
        return mysql_dict

    @property
    def data_object_insert(self):
        # Prepare detailed object data for insertion into DynamoDB
        return {
            'id': self.data_insert['id'],
            'created': self.data_insert['created'],
            'object': self.data_insert['data']['object']['object'],
            'data': self.data_insert['data']['object'],
        }


class DataInserter(DataHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table_targets = {'customer': 'stripe_events_customer', 'system': 'stripe_events_system'}

    def clean_empty(self, d):
        # Remove empty values from dictionary or list recursively
        if not isinstance(d, (dict, list)):
            return d
        if isinstance(d, list):
            return [v for v in (self.clean_empty(v) for v in d) if v]
        return {k: v for k, v in ((k, self.clean_empty(v)) for k, v in d.items()) if v}

    def insert_dynamo(self):
        # Insert data into DynamoDB based on payload type (customer or system)
        for k, v in self.table_targets.items():
            if self.payload_type == k:
                table = self.dynamodb.Table(v)
                try:
                    table.put_item(Item=self.data_insert)
                except Exception:
                    table.put_item(Item=self.clean_empty(self.data_insert))

    def insert_dynamo_summary(self):
        # Insert summary and object data into respective DynamoDB tables
        table = self.dynamodb.Table('stripe_summary')
        table.put_item(Item=self.data_summary_insert)
        table_2 = self.dynamodb.Table('stripe_stage')
        table_2.put_item(Item=self.data_summary_insert)
        table_3 = self.dynamodb.Table('stripe_object')
        table_4 = self.dynamodb.Table('stripe_object_stage')
        try:
            table_3.put_item(Item=self.data_object_insert)
        except Exception:
            table_3.put_item(Item=self.clean_empty(self.data_object_insert))
        try:
            table_4.put_item(Item=self.data_object_insert)
        except Exception:
            table_4.put_item(Item=self.clean_empty(self.data_object_insert))


def lambda_handler(event, context):
    # Lambda function entry point
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    dt = DataInserter(**event)
    try:
        # Log relevant information and handle data insertion
        logger.info(dt.data)
        logger.info(dt.customer_id)
        logger.info(dt.payload_type)
        logger.info(dt.data_insert)
        dt.insert_dynamo()
        dt.insert_dynamo_summary()
        return dt.respond(None, 'success')
    except ValueError as e:
        logger.error('Invalid payload: ' + str(e))
        return dt.respond(e)
    except stripe.error.SignatureVerificationError as e:
        logger.error('Invalid signature: ' + str(e))
        return dt.respond(e)
