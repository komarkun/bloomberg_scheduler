import datetime
import io
import json
import logging
import os
import shutil
import time
import uuid
from urllib.parse import urljoin

from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session

import pandas as pd
from supabase import create_client

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

responses_url = urljoin(HOST, '/eap/catalogs/{c}/content/responses/'.format(c=catalog_id))

# Filter the required output from the available content by passing the request_name as prefix 
# and request_id as unique requestIdentifier query parameters respectively.
params = {
    'prefix': "IndexWeight",
    'requestIdentifier': 'IndexWeightDaily',
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
        output_file_path = os.path.join('downloads', output_key)
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

# Read the gzipped JSON file
with open(output_file_path, 'rb') as file:
    df = pd.read_json(file, compression='gzip')

# Create separate dataframes for each index
for _, row in df.iterrows():
    index_name = row['IDENTIFIER']
    # Convert the list of dictionaries to a dataframe
    weights_df = pd.DataFrame(row['INDX_MWEIGHT'])[['BC_INDX_MWEIGHT_TKR_EXCH', 'BC_INDX_MWEIGHT_PCT']]
    
    # Clean and sort the dataframe
    weights_df = (weights_df
        .assign(
            # Remove 'IJ' from stock names
            BC_INDX_MWEIGHT_TKR_EXCH=weights_df['BC_INDX_MWEIGHT_TKR_EXCH'].str.replace(' IJ', ''),
            # Round weights to 2 decimal places
            BC_INDX_MWEIGHT_PCT=weights_df['BC_INDX_MWEIGHT_PCT'].round(2)
        )
        # Sort by weight in descending order
        .sort_values('BC_INDX_MWEIGHT_PCT', ascending=False)
        # Rename columns
        .rename(columns={
            'BC_INDX_MWEIGHT_TKR_EXCH': 'stock',
            'BC_INDX_MWEIGHT_PCT': 'weight'
        })
        # Reset index after sorting
        .reset_index(drop=True)
    )
    
    # Store in separate variables
    if index_name == 'LQ45 Index':
        lq45_weights = weights_df
        
        # Add timestamp column
        lq45_weights['timestamp'] = pd.Timestamp.now()
        lq45_weights['timestamp'] = lq45_weights['timestamp'].dt.strftime('%Y-%m-%d')

        # Convert DataFrame to records for Supabase insertion
        records = lq45_weights.to_dict('records')

        supabase_url = "https://datalake.batinvestasi.com/"
        supabase_key = CREDENTIALS['supabase_key']
        supabase = create_client(supabase_url, supabase_key)
        
        # Insert data into Supabase
        try:
            result = supabase.table('LQ45_Weight').insert(records).execute()
            LOG.info('Successfully inserted %d records into LQ45_Weight table', len(records))
        except Exception as e:
            LOG.error('Failed to insert records into Supabase: %s', str(e))

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
