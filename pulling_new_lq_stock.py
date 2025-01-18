import datetime
import io
import json
import logging
import os
import shutil
import time
import uuid
from urllib.parse import urljoin
from supabase import create_client

from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session

def check_lq_data():
    # Load credentials
    with open('ps-credential.txt', encoding="utf-8") as credential_file:
        CREDENTIALS = json.load(credential_file)

    # Initialize Supabase client
    url = "https://datalake.batinvestasi.com/"
    supabase = create_client(url, CREDENTIALS['supabase_key'])

    # Get Today & Yesterday's date (since the stock data is from previous day)
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    try:
        # Query to check data for yesterday's date
        response_today = supabase.table('LQ45_Weight') \
            .select('stock, timestamp') \
            .eq('timestamp', today) \
            .execute()
        
        response_yesterday = supabase.table('LQ45_Weight') \
            .select('stock, timestamp') \
            .eq('timestamp', yesterday) \
            .execute()
        
        records_today = response_today.data
        records_yesterday = response_yesterday.data
        
        stocks_today = set([record['stock'] for record in records_today])
        stocks_yesterday = set([record['stock'] for record in records_yesterday])
        
        # Get stocks added and removed
        stocks_added = stocks_today - stocks_yesterday
        stocks_removed = stocks_yesterday - stocks_today
        
        if stocks_added:
            LOG.info(f"Stocks added: {', '.join(stocks_added)}")
        if stocks_removed:
            LOG.info(f"Stocks removed: {', '.join(stocks_removed)}")
        
        return stocks_added, stocks_removed

    except Exception as e:
        LOG.error(f"Error checking stock data: {str(e)}")
        return [], []
    
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s',
)
LOG = logging.getLogger(__name__)

CREDENTIALS_PATH = os.path.join('ps-credential.txt')

with io.open(CREDENTIALS_PATH, encoding="utf-8") as credential_file:
    CREDENTIALS = json.load(credential_file)

EXPIRES_IN = datetime.datetime.fromtimestamp(CREDENTIALS['expiration_date'] / 1000) - datetime.datetime.utcnow()
if EXPIRES_IN.days < 0:
    LOG.warning("Credentials expired %s days ago", EXPIRES_IN.days)
elif EXPIRES_IN < datetime.timedelta(days=30):
    LOG.warning("Credentials expiring in %s", EXPIRES_IN)

class DLRestApiSession(OAuth2Session):
    """Custom session class for making requests to a DL REST API using OAuth2 authentication."""

    def __init__(self, *args, **kwargs):
        """
        Initialize a DLRestApiSession instance.
        """
        super().__init__(*args, **kwargs)

    def request_token(self):
        """
        Fetch an OAuth2 access token by making a request to the token endpoint.
        """
        oauth2_endpoint = 'https://bsso.blpprofessional.com/ext/api/as/token.oauth2'
        self.token = self.fetch_token(
            token_url=oauth2_endpoint,
            client_secret=CREDENTIALS['client_secret']
        )

    def request(self, *args, **kwargs):
        """
        Override the parent class method to handle TokenExpiredError by refreshing the token.
        :return: response object from the API request
        """
        try:
            response = super().request(*args, **kwargs)
        except TokenExpiredError:
            self.request_token()
            response = super().request(*args, **kwargs)

        return response

    def send(self, request, **kwargs):
        """
        Override the parent class method to log request and response information.
        :param request: prepared request object
        :return: response object from the API request
        """
        LOG.debug("Request being sent to HTTP server: %s, %s, %s", request.method, request.url, request.headers)

        response = super().send(request, **kwargs)

        LOG.debug("Response status: %s", response.status_code)
        LOG.debug("Response x-request-id: %s", response.headers.get("x-request-id"))

        if response.ok:
            if not kwargs.get("stream"):
                LOG.debug("Response content: %s", response.text)
        else:
            raise RuntimeError('\n\tUnexpected response status code: {c}\nDetails: {r}'.format(
                    c=str(response.status_code), r=response.json()))

        return response

CLIENT = BackendApplicationClient(client_id=CREDENTIALS['client_id'])

SESSION = DLRestApiSession(client=CLIENT)
# This is a required header for each call to DL Rest API.
SESSION.headers['api-version'] = '2'
SESSION.request_token()

HOST = 'https://api.bloomberg.com'

catalogs_url = urljoin(HOST, '/eap/catalogs/')
response = SESSION.get(catalogs_url)

# We got back a good response. Let's extract our account number.
catalogs = response.json()['contains']
for catalog in catalogs:
    if catalog['subscriptionType'] == 'scheduled':
        # Take the catalog having "scheduled" subscription type,
        # which corresponds to the Data License account number.
        catalog_id = catalog['identifier']
        break
else:
    # We exhausted the catalogs, but didn't find a non-'bbg' catalog.
    LOG.error('Scheduled catalog not in %r', response.json()['contains'])
    raise RuntimeError('Scheduled catalog not found')

account_url = urljoin(HOST, '/eap/catalogs/{c}/'.format(c=catalog_id))
LOG.info("Scheduled catalog URL: %s", account_url)

request_name = "Python309HistoryRequest" + str(uuid.uuid1())[:6]
stocks_added, _ = check_lq_data()

request_payload = {
    '@type': 'HistoryRequest',
    'name': request_name,
    'description': 'IDX stocks history request',
    'universe': {
        '@type': 'Universe',
        'contains': [
            *[{'@type': 'Identifier', 'identifierType': 'TICKER', 'identifierValue': stock + ' IJ Equity'} for stock in stocks_added]
        ]
    },
    'fieldList': {
        '@type': 'HistoryFieldList',
        'contains': [
            {'mnemonic': 'PX_OPEN'},
            {'mnemonic': 'PX_HIGH'},
            {'mnemonic': 'PX_LOW'},
            {'mnemonic': 'PX_LAST'},
            {'mnemonic': 'PX_VOLUME'},
            {'mnemonic': 'TURNOVER'},
            {'mnemonic': 'NUM_TRADES'},
            {'mnemonic': 'CHG_PCT_1D'},
        ],
    },
    'trigger': {
        '@type': 'SubmitTrigger'
    },
    'runtimeOptions': {
        '@type': 'HistoryRuntimeOptions',
        'dateRange': {
            '@type': 'IntervalDateRange',
            'startDate': (datetime.datetime.now() - datetime.timedelta(days=730)).strftime('%Y-%m-%d'),
            'endDate': (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        },
    },
    'formatting': {
        '@type': 'MediaType',
        'outputMediaType': 'application/json',
    },
}
LOG.info('Request component payload:\n%s', json.dumps(request_payload, indent=2))

# Create a new request resource.
requests_url = urljoin(account_url, 'requests/')
response = SESSION.post(requests_url, json=request_payload)

# Extract the URL of the created resource.
request_location = response.headers['Location']
request_url = urljoin(HOST, request_location)

# Extract the identifier of the created resource.
request_id = json.loads(response.text)['request']['identifier']

LOG.info('%s resource has been successfully created at %s',
         request_name,
         request_url)

SESSION.get(request_url)

responses_url = urljoin(HOST, '/eap/catalogs/{c}/content/responses/'.format(c=catalog_id))

# Filter the required output from the available content by passing the request_name as prefix 
# and request_id as unique requestIdentifier query parameters respectively.
params = {
    'prefix': request_name,
    'requestIdentifier': request_id,
}

# We recommend adjusting the polling frequency and timeout based on the amount of data or the time range requested.

reply_timeout_minutes = 45
reply_timeout = datetime.timedelta(minutes=reply_timeout_minutes)
expiration_timestamp = datetime.datetime.utcnow() + reply_timeout

while datetime.datetime.utcnow() < expiration_timestamp:
    content_responses = SESSION.get(responses_url, params=params)
    response_contains = json.loads(content_responses.text)['contains']
    if len(response_contains) > 0 :
        output = response_contains[0]
        LOG.info('Response listing:\n%s', json.dumps(output, indent=2))

        output_key = output['key']
        output_url = urljoin(
            HOST,
            '/eap/catalogs/{c}/content/responses/{key}'.format(c=catalog_id, key=output_key)
        )
        output_file_path = os.path.join(os.path.join('downloads'), output_key)
        break
    else:
        # We make a delay between polls to reduce network usage.
        time.sleep(60)
else:
    LOG.info('Response not received within %s minutes. Exiting.', reply_timeout_minutes)


with SESSION.get(output_url, stream=True) as response:
    output_filename = output_key
    if 'content-encoding' in response.headers:
        if response.headers['content-encoding'] == 'gzip':
            output_filename = output_filename + '.gz'
        elif response.headers['content-encoding'] == '':
            pass
        else:
            raise RuntimeError('Unsupported content encoding received in the response')

    downloads_path = os.path.join('downloads')
    output_file_path = os.path.join(downloads_path, output_filename)

    with open(output_file_path, 'wb') as output_file:
        LOG.info('Loading file from: %s (can take a while) ...', output_url)
        shutil.copyfileobj(response.raw, output_file)

LOG.info('File downloaded: %s', output_filename)
LOG.debug('File location: %s', output_file_path)

import pandas as pd

# Read the gzipped JSON file
with open(output_file_path, 'rb') as file:
    df = pd.read_json(file, compression='gzip')

# Create new dataframe with renamed and reordered columns
new_df = pd.DataFrame({
    'stock_id': df['IDENTIFIER'].str.replace(' IJ Equity', ''),  # Remove ' IJ Equity' suffix
    'report_date': df['DATE'],
    'open_price': df['PX_OPEN'],
    'high_price': df['PX_HIGH'],
    'low_price': df['PX_LOW'],
    'close_price': df['PX_LAST'],
    'volume': df['PX_VOLUME'],
    'value': df['TURNOVER'],
    'frequency': df['NUM_TRADES'],
    'change_percent': df['CHG_PCT_1D']
})

# Sort the dataframe by stock_id and report_date
new_df = new_df.sort_values(['stock_id', 'report_date'], ascending=[True, False])

# Fill NaN values with 0
new_df = new_df.fillna(0).astype({'volume': 'int64', 'value': 'int64', 'frequency': 'int64'})

# Initialize Supabase client
url = "https://datalake.batinvestasi.com/"
key = CREDENTIALS['supabase_key']
supabase = create_client(url, key)

# Import necessary libraries
from datetime import datetime

# First convert to datetime, then format as string
new_df['report_date'] = pd.to_datetime(new_df['report_date'])
new_df['report_date'] = new_df['report_date'].dt.strftime('%Y-%m-%d')

# Convert DataFrame to list of dictionaries
records = new_df.to_dict('records')

# Insert data into Supabase
try:
    result = supabase.table('bloomberg_stock_histories').upsert(records).execute()
    print(f"Successfully inserted {len(records)} records")
except Exception as e:
    print(f"Error inserting data: {str(e)}")

# Clean up downloads folder
try:
    for file in os.listdir('downloads'):
        if file == '.gitkeep':
            continue
        file_path = os.path.join('downloads', file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    LOG.info('Downloads folder cleaned successfully')
except Exception as e:
    LOG.error('Error cleaning downloads folder: %s', str(e))
