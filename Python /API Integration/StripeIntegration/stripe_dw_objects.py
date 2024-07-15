'''

1. **Classes and Inheritance**: 
   - **RawLogsPull**: Retrieves raw data from MySQL, removes Unicode characters, and consolidates Stripe event data.
   - **ChildObjects**: Extends RawLogsPull to extract specific details like subscription plans, items, and device-related data.
   - **DataInserter**: Inherits from ChildObjects to insert cleaned and structured data into MySQL tables based on object type.

2. **Methods**:
   - `remove_unicode`: Recursively removes Unicode from strings in dictionaries and lists.
   - `get_db_data`: Executes SQL statements to retrieve and parse data from MySQL.
   - `consolidate_data`: Transforms raw Stripe event data into a structured format.
   - Methods in **ChildObjects** and **DataInserter** extract specific details and build additional data structures as needed.

3. **Static Methods**:
   - `parse_vals`: Converts values to appropriate formats (e.g., JSON serialization) before insertion into MySQL.

4. **Command-Line Execution**: 
   - Parses command-line arguments (`--object` and `--mintimestamp`) to specify which object's data to process and from which timestamp.

'''

import collections
import datetime
import argparse
import hashlib
import json

from helpers import MySQLRpts  # Assuming MySQLRpts is a helper class or module

is_test = False  # Flag indicating whether this is a test run


class RawLogsPull:
    def __init__(self, data_object, min_timestamp):
        """
        Initialize RawLogsPull with data object and minimum timestamp.

        :param data_object: Stripe object type (e.g., 'customer', 'subscription')
        :param min_timestamp: Minimum Unix timestamp to filter events
        """
        self.data_object = data_object
        self.min_timestamp = min_timestamp
        self.get_stmt = (
            "SELECT * "
            "FROM stripe.event_object_raw "
            f"WHERE object = '{self.data_object}' AND event_unixtimestamp >= {self.min_timestamp} "
            "ORDER BY event_unixtimestamp "
        )
        self.raw_data = self.consolidate_data(self.get_db_data(self.get_stmt))

    def remove_unicode(self, nested_dict):
        """
        Recursively remove Unicode characters from nested dictionaries and lists.

        :param nested_dict: Dictionary or list containing Unicode strings
        :return: Cleaned dictionary or list
        """
        if isinstance(nested_dict, str):
            return str(nested_dict.encode('utf8'))
        elif isinstance(nested_dict, collections.Mapping):
            return dict(map(self.remove_unicode, nested_dict.items()))
        elif isinstance(nested_dict, collections.Iterable):
            return type(nested_dict)(map(self.remove_unicode, nested_dict))
        else:
            return nested_dict

    def get_db_data(self, stmt):
        """
        Retrieve data from MySQL database based on SQL statement.

        :param stmt: SQL statement to execute
        :return: Database query result as a list of dictionaries
        """
        dt = MySQLRpts().get_select_data(stmt)
        for items in dt:
            items['data'] = json.loads(items.pop('data'))
        return self.remove_unicode(dt)

    @staticmethod
    def consolidate_data(stripe_list_of_dicts):
        """
        Consolidate and transform raw Stripe event data.

        :param stripe_list_of_dicts: List of dictionaries representing Stripe events
        :return: Consolidated list of dictionaries with transformed data
        """
        new_events_data = []
        for events in stripe_list_of_dicts:
            append_data = {'updated_at': None}
            try:
                if events['data']['previous_attributes']:
                    append_data.update(events['data']['object'])
            except KeyError:
                append_data.update(events['data'])
            if events['object'] == 'customer':
                append_data['email_hash'] = hashlib.md5(append_data['email'].encode('utf-8')).hexdigest()
                append_data['name'] = append_data['description'].split(' of ')[0] \
                    if ' of ' in append_data['description'] else append_data['description']
                append_data['company'] = ' of '.join(append_data['description'].split(' of ')[1:]) \
                    if ' of ' in append_data['description'] else None
            elif events['object'] == 'subscription':
                append_data['plan_id'] = append_data['plan']['id']
                append_data['product'] = append_data['plan']['product']
            elif events['object'] == 'invoice':
                append_data['created'] = append_data['date']
                append_data['lines_invoice'] = append_data['lines']
                try:
                    if append_data['amount_paid'] and append_data['amount_remaining']:
                        pass
                except KeyError:
                    append_data['amount_paid'] = None
                    append_data['amount_remaining'] = None
            new_events_data.append(append_data)
        return new_events_data


class ChildObjects(RawLogsPull):
    """
    Extend RawLogsPull to extract child objects from main data object.
    """

    def __init__(self, data_object, min_timestamp):
        """
        Initialize ChildObjects with data object and minimum timestamp.

        :param data_object: Stripe object type (e.g., 'customer', 'subscription')
        :param min_timestamp: Minimum Unix timestamp to filter events
        """
        super().__init__(data_object, min_timestamp)

    def get_subscription_plan(self):
        """
        Extract subscription plan details from raw data.

        :return: List of dictionaries containing subscription plan details
        """
        if self.data_object != 'subscription':
            return []
        else:
            try:
                ss = [s['plan'] for s in self.raw_data if s['object'] == 'subscription']
                for sss in ss:
                    sss['updated_at'] = None
                    sss['plan_interval'] = sss.pop('interval')
                    sss['plan_interval_count'] = sss.pop('interval_count')
                return ss
            except KeyError:
                return []

    def get_subscription_items(self):
        """
        Extract subscription item details from raw data.

        :return: List of dictionaries containing subscription item details
        """
        if self.data_object != 'subscription':
            return []
        else:
            try:
                return [
                    {
                        'object': y['object'],
                        'id': y['id'],
                        'created': y['created'],
                        'metadata': y['metadata'],
                        'plan_id': y['plan']['id'],
                        'plan_amount': y['plan']['amount'],
                        'plan_interval': y['plan']['interval'],
                        'plan_interval_count': y['plan']['interval_count'],
                        'plan_nickname': y['plan']['nickname'],
                        'plan_trial_period_days': y['plan']['trial_period_days'],
                        'product': y['plan']['product'],
                        'quantity': y['quantity'],
                        'subscription': y['subscription'],
                        'updated_at': None,
                    } for si in self.raw_data for y in si['items']['data'] if si['object'] == 'subscription'
                ]
            except KeyError:
                return []

    def build_devices(self):
        """
        Build device-related data from raw data.

        :return: List of dictionaries containing device-related details
        """
        if self.data_object != 'subscription':
            return []
        else:
            try:
                device_stmt = (
                    "SELECT eor.id, el.event_type, el.object_event_id, eor.event_unixtimestamp, eor.data "
                    "FROM stripe.event_object_raw eor "
                    "JOIN stripe.event_log el ON eor.id = el.id "
                    f"WHERE eor.object = '{self.data_object}' "
                    f"AND eor.event_unixtimestamp >= {self.min_timestamp} "
                    "AND el.event_type not regexp 'trial' "
                    "ORDER BY eor.event_unixtimestamp "
                )
                device_raw_data = self.get_db_data(device_stmt)
                device_data = []
                for xxx in device_raw_data:
                    obj = {
                        'object': 'subscription_quantity',
                        'event_id': xxx['id'],
                        'subscription_id': xxx['object_event_id'],
                        'event_date': datetime.datetime.fromtimestamp(int(xxx['event_unixtimestamp']) - (8 * 60 * 60)).strftime('%Y-%m-%d 00:00:00'),
                        'event_unixtimestamp': xxx['event_unixtimestamp'],
                        'event_type': xxx['event_type']
                    }
                    if 'deleted' in xxx['event_type']:
                        obj.update({'quantity': 0})
                    else:
                        try:
                            obj.update({'quantity': xxx['data']['quantity']})
                        except KeyError:
                            obj.update({'quantity': xxx['data']['object']['quantity']})
                    device_data.append(obj)
                return device_data
            except KeyError:
                return []


class DataInserter(ChildObjects):
    """
    Insert extracted data into MySQL tables based on object type.
    """

    def __init__(self, data_object, min_timestamp):
        """
        Initialize DataInserter with data object and minimum timestamp.

        :param data_object: Stripe object type (e.g., 'customer', 'subscription')
        :param min_timestamp: Minimum Unix timestamp to filter events
        """
        super().__init__(data_object, min_timestamp)
        # Combine all extracted data into raw_data
        self.raw_data = self.raw_data \
                        + self.get_subscription_plan() \
                        + self.get_subscription_items() \
                        + self.build_devices()

        # Define columns for each object type to insert into MySQL tables
        self.insert_cols = {
            'subscription': ['id', 'billing', 'billing_cycle_anchor', 'cancel_at_period_end',
                             'canceled_at', 'created', 'current_period_end', 'current_period_start',
                             'customer', 'discount', 'ended_at', 'metadata', 'plan_id', 'product',
                             'quantity', 'start', 'status', 'tax_percent', 'trial_end', 'trial_start',
                             'updated_at'],
            'plan': ['id', 'amount', 'created', 'plan_interval', 'plan_interval_count',
                     'metadata', 'nickname', 'product', 'trial_period_days', 'updated_at'],
            'subscription_item': ['id', 'created', 'metadata', 'plan_id', 'plan_amount',
                                  'plan_interval', 'plan_interval_count', 'plan_nickname',
                                  'plan_trial_period_days', 'product', 'quantity',
                                  'subscription', 'updated_at'],
            'customer': ['id', 'account_balance', 'company', 'created', 'delinquent', 'description',
                         'email', 'email_hash', 'invoice_prefix', 'metadata', 'name', 'shipping',
                         'sources', 'updated_at'],
            'invoice': ['id',

 'amount_due', 'amount_paid', 'amount_remaining', 'attempt_count',
                        'billing', 'charge', 'closed', 'created', 'customer', 'description',
                        'discount', 'due_date', 'ending_balance', 'forgiven', 'lines_invoice', 'number',
                        'paid', 'period_end', 'period_start', 'receipt_number', 'starting_balance',
                        'statement_descriptor', 'subscription', 'subtotal', 'tax', 'tax_percent',
                        'total', 'updated_at'],
            'subscription_quantity': ['subscription_id', 'event_date', 'event_unixtimestamp', 'quantity']
        }

        # Insert data into corresponding MySQL tables
        for k in self.insert_cols:
            self.insert_data(k)

    @staticmethod
    def parse_vals(value_to_parse):
        """
        Parse values to appropriate format before inserting into MySQL.

        :param value_to_parse: Value to parse
        :return: Parsed value
        """
        if isinstance(value_to_parse, str):
            return value_to_parse
        elif not value_to_parse:
            return None
        elif isinstance(value_to_parse, collections.Mapping) or isinstance(value_to_parse, collections.Iterable):
            return json.dumps(value_to_parse)
        else:
            return value_to_parse

    def create_insert_data(self):
        """
        Create insert data in correct format for MySQL insertion.

        :return: List of dictionaries containing data to insert
        """
        return [
            {k: self.parse_vals(v) for k, v in records.items() if k in self.insert_cols[records['object']]}
            for records in self.raw_data
        ]

    def insert_data(self, table):
        """
        Insert data into MySQL table.

        :param table: Table name to insert data into
        """
        ins_data = [
            {
                k: self.parse_vals(v) for k, v in records.items()
                if k in self.insert_cols[table] and table == records['object']
            }
            for records in self.raw_data
        ]
        ins_data = [xy for xy in ins_data if xy]
        if ins_data:
            ins_stmt = MySQLRpts().create_stmt('stripe.' + table, 'REPLACE', self.insert_cols[table])
            if is_test:
                return ins_stmt, ins_data
            else:
                MySQLRpts().insert_data(ins_stmt, ins_data)

if __name__ == '__main__':
    # Command-line argument parsing
    parser = argparse.ArgumentParser(description="Create DW Stripe table")
    parser.add_argument("--object", help="Object for insertion")
    parser.add_argument("--mintimestamp", help="Earliest timestamp value to filter for")
    args = parser.parse_args()

    try:
        # Initialize DataInserter with command-line arguments
        DataInserter(args.object, args.mintimestamp)
    except KeyError:
        pass  # Handle KeyError if necessary
