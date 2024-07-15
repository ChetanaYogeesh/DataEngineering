# Import necessary libraries
import requests, urllib.parse, os, csv, urllib.request, gspread, httplib2, subprocess, datetime, pytz, zeep
import dateutil.parser
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from simple_salesforce import Salesforce
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery, http

class SalesforceRpts(object):
    def __init__(self):
        # Initialize Salesforce session
        session = requests.Session()
        sf = Salesforce(username=credentials.salesforce['user'], password=credentials.salesforce['pwd'],
                        security_token=credentials.salesforce['security_token'],
                        organizationId=credentials.salesforce['organizationId'], session=session)
        self.headers = sf.headers
        self.sid = sf.session_id

    @staticmethod
    def csv_to_dict(path_to_file, kill_file=None):
        # Convert CSV to dictionary
        with open(path_to_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = [line for line in reader]
        if kill_file:
            # Remove file after reading if specified
            try:
                os.remove(path_to_file)
            except OSError:
                pass
        return rows

    def download_csv(self, sf_rpt_id, path_to_save, create_dict=None, kill_file=None, footers=None, sf_rpt_name=None):
        # Download CSV report from Salesforce
        try:
            os.remove(path_to_save)
        except OSError:
            pass
        response = requests.get(f'https://swiftnav.my.salesforce.com/{sf_rpt_id}?view=d&snip&export=1&enc=UTF-8&xf=csv',
                                headers=self.headers, cookies={'sid': self.sid})
        csv_data = response.text
        if footers:
            csv_data = csv_data.split(f'\n\n\"{sf_rpt_name}', 1)
        with open(path_to_save, 'w') as text_file:
            text_file.write(csv_data.encode('ascii', 'ignore').decode('ascii'))
        if create_dict:
            if kill_file:
                dicts = self.csv_to_dict(path_to_save, kill_file)
                return dicts
            else:
                dicts = self.csv_to_dict(path_to_save)
                return dicts

    def api_auth(self):
        # Authenticate Salesforce Bulk API
        return SalesforceBulk(sessionId=self.sid, host=urllib.parse.urlparse('https://swiftnav.my.salesforce.com').hostname, API_version="40.0")

    def api_query(self, sf_object, sf_query):
        # Perform a bulk query using Salesforce Bulk API
        bulk = self.api_auth()
        job = bulk.create_query_job(sf_object, concurrency='Parallel')
        try:
            batch = bulk.query(job, sf_query)
            bulk.wait_for_batch(job, batch)
            result = bulk.get_batch_result_iter(job, batch, parse_csv=True)
            return list(result)
        except Exception as e:
            raise Exception(e)
        finally:
            bulk.close_job(job)

    def api_update(self, sf_object, list_of_dicts):
        # Perform a bulk update using Salesforce Bulk API
        bulk = self.api_auth()
        job = bulk.create_update_job(sf_object, contentType='CSV', concurrency='Parallel')
        try:
            batch = bulk.post_bulk_batch(job, CsvDictsAdapter(iter(list_of_dicts)))
            bulk.wait_for_batch(job, batch)
        except Exception as e:
            raise Exception(e)
        finally:
            bulk.close_job(job)

    def api_upsert(self, sf_object, upsert_key, list_of_dicts):
        # Perform a bulk upsert using Salesforce Bulk API
        bulk = self.api_auth()
        job = bulk.create_upsert_job(sf_object, upsert_key, contentType='CSV', concurrency='Parallel')
        try:
            batch = bulk.post_bulk_batch(job, CsvDictsAdapter(iter(list_of_dicts)))
            bulk.wait_for_batch(job, batch)
        except Exception as e:
            raise Exception(e)
        finally:
            bulk.close_job(job)
            
    def api_insert(self, sf_object, list_of_dicts):
        # Perform a bulk insert using Salesforce Bulk API
        bulk = self.api_auth()
        job = bulk.create_insert_job(sf_object, contentType='CSV', concurrency='Parallel')
        try:
            batch = bulk.post_bulk_batch(job, CsvDictsAdapter(iter(list_of_dicts)))
            bulk.wait_for_batch(job, batch)
        except Exception as e:
            raise Exception(e)
        finally:
            bulk.close_job(job)
     
class NetsuiteAPI(object):
    def __init__(self):
        # Initialize Netsuite API client
        self.WSDL_URL = 'https://webservices.na3.netsuite.com/wsdl/v2016_1_0/netsuite.wsdl'
        self.NS_EMAIL = credentials.netsuite_new['ceo_login']
        self.NS_PASSWORD = credentials.netsuite_new['ceo_pw']
        self.NS_ROLE = '25'
        self.NS_ACCOUNT = credentials.netsuite_new['org_id']
        self.NS_APPID = credentials.netsuite_new['app_id']

    def login_client(self):
        # Login to Netsuite API
        client = zeep.Client(self.WSDL_URL)
        p = client.get_type('ns0:Passport')
        passport = p(email=self.NS_EMAIL, password=self.NS_PASSWORD, account=self.NS_ACCOUNT)
        login = client.service.login(passport=passport, _soapheaders={'applicationInfo': self.NS_APPID})
        return client
    
    
class NetsuiteRpts(object):
    def __init__(self, rpt_link):
        # Initialize Netsuite report and parse HTML data
        self.rpt_link = rpt_link
        raw_response = urllib.request.urlopen(self.rpt_link)
        raw_data = raw_response.read().replace(b"\n", b"   ")
        bsobj = BeautifulSoup(raw_data, "html.parser")
        self.writer = []
        for tr in bsobj.find_all('tr')[0:]:
            tds = tr.find_all('td')
            row = [elem.text.replace("=", "") for elem in tds]
            self.writer.append(row)
        self.rows = [dict(list(zip(self.writer[0], self.writer[i]))) for i in range(len(self.writer)) if i > 0]

    def create_dict(self):
        # Convert parsed data to dictionary
        return self.rows


class GoogleRpts(object):
    def __init__(self):
        # Initialize Google API scopes
        self.scopes = {
            'drive': 'https://www.googleapis.com/auth/drive',
            'sheets': 'https://spreadsheets.google.com/feeds',
            'analytics': 'https://www.googleapis.com/auth/analytics.readonly',
            'non-api': {
                'url_login': 'https://accounts.google.com/ServiceLogin',
                'url_auth': 'https://accounts.google.com/ServiceLoginAuth'
            }
        }
        self.ses = requests.session()

    def non_api_auth(self, url):
        # Perform non-API Google authentication
        login_html = self.ses.get(self.scopes['non-api']['url_login'])
        soup_login = BeautifulSoup(login_html.content, 'html.parser').find('form').find_all('input')
        dico = {}
        for u in soup_login:
            if u.has_attr('value'):
                dico[u['name']] = u['value']
        # override the inputs without login and pwd:
        dico['Email'] = credentials.google['forum_login']
        dico['Passwd'] = credentials.google['forum_pwd']
        self.ses.post(self.scopes['non-api']['url_auth'], data=dico)
        return self.ses.get(url).text

    def api_auth(self, scope):
        # Perform API Google authentication
        creds = ServiceAccountCredentials._from_parsed_json_keyfile_name(credentials.google['service_acct_key'],
                                                                    self.scopes[scope])
        if scope in ('analytics', 'drive'):
            return creds.authorize(httplib2.Http())
        elif scope == 'sheets':
            return gspread.authorize(creds)


class MySQLRpts(object):
    def __init__(self):
        # Initialize MySQL connection
        self.connection = MySQLdb.connect(host='localhost', user=credentials.mysql['user'],
                                          passwd=credentials.mysql['pwd'], charset='utf8')
        self.cursor = self.connection.cursor()
        self.headers = []

    def __del__(self):
        # Close MySQL connection
        self.cursor.close()
        self.connection.close()

    @staticmethod
    def convert_datetime(dt, for_insert=False, convert_UTC=False):
        # Convert datetime to required format
        local = pytz.timezone("America/Los_Angeles")
        if isinstance(dt, datetime.datetime):
            parsed_date = dt
        else:
            parsed_date = dateutil.parser.parse(dt).replace(tzinfo=None)
        if not for_insert:
            if not convert_UTC:
                return datetime.datetime.strftime(parsed_date, '%Y-%m-%d %H:%M:%S')
            else:
                return datetime.datetime.strftime(local.fromutc(parsed_date).replace(tzinfo=None), '%Y-%m-%d %H:%M:%S')
        else:
            if not convert_UTC:
                return parsed_date
            else:
                return local.fromutc(parsed_date).replace(tzinfo=None)

    def etl_log(self, etl_process, source, target, max_date=None):
        # Log ETL process
        if max_date:
            insert_stmt = "INSERT INTO sn.etl_audit_log VALUES (null, %s,%s, %s, %s, null)"
            data = (etl_process, source, target, self.convert_datetime(max_date, for_insert=True))
        else:
            insert_stmt = "INSERT INTO sn.etl_audit_log VALUES (null, %s, %s, %s, null, null)"
            data = (etl_process, source, target)
        try:
            self.cursor.execute(insert_stmt, data)
            self.connection.commit()
        except Exception as e:
            pass
        finally:
            del self

    def exec_simple(self, stmt):
        # Execute simple SQL statement
        try:
            self.cursor.execute(stmt)
            self.connection.commit()
        except Exception as e:
            pass
        finally:
            del self

    def dictify(self, sql_output):
        # Convert SQL output to dictionary
        self.headers = [desc[0] for desc in self.cursor.description]
        lists = [dict(zip(self.headers, sql_output[x])) for x in range(len(sql_output))]
        for y in range(len(lists)):
            for z in lists[y]:
                if isinstance(lists[y][z], datetime.datetime):
                    lists[y][z] = self.convert_datetime(lists[y][z])
        return lists

    def get_query_headers(self):
        # Get headers of the last query
        return self.headers

    def get_select_data(self, stmt):
        # Execute select statement and return data
        try:
            self.cursor.execute(stmt)
            self.connection.commit()    # need?
            select_rows = self.cursor.fetchall()
            select_output = self.dictify(select_rows)
            return select_output
        except Exception as e:
            pass
        finally:
            del self

    def get_proc_data(self, proc_name, args=()):
        # Execute stored procedure and return data
        try:
            self.cursor.callproc(proc_name, args)
            proc_rows = self.cursor.fetchall()
            proc_output = self.dictify(proc_rows)
            return proc_output
        except Exception as e:
            pass
        finally:
            del self

    @staticmethod
    def create_stmt(table, insert_type='INSERT IGNORE', col_list=[]):
        # Create SQL insert statement
        cols_stringified = str(col_list).strip('[').strip(']').replace("'", '')
        cols_ins = str(['%(' + col_list[x] + ')s' for x in range(len(col_list))]).strip(']').strip('[').replace("'", '')
        return insert_type + ' INTO ' + table + ' (' + cols_stringified + ') VALUES (' + cols_ins + ')'
           
    def insert_data(self, stmt, data):
        # Insert data into table
        try:
            self.cursor.executemany(stmt, data)
            self.connection.commit()
        except Exception as e:
            pass
        finally:
            del self