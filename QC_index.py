import datetime
from supabase import create_client
import json
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s',
)
LOG = logging.getLogger(__name__)

def check_stock_data():
    # Load credentials
    with open('ps-credential.txt', encoding="utf-8") as credential_file:
        CREDENTIALS = json.load(credential_file)

    # Initialize Supabase client
    url = "https://datalake.batinvestasi.com/"
    supabase = create_client(url, CREDENTIALS['supabase_key'])

    # Get yesterday's date (since the stock data is from previous day)
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')
    
    try:
        # Query to check data for yesterday's date
        response = supabase.table('bloomberg_stock_index_histories') \
            .select('index_code, report_date') \
            .eq('report_date', today) \
            .execute()
        
        records = response.data

        if not records:
            LOG.error(f"No data found for date: {today}")
            return False
        
        # Count unique stocks (should match your expected number of stocks)
        unique_stocks = len(set(record['index_code'] for record in records))
        expected_stocks = 2  # Based on your stock list in the original code
        
        if unique_stocks < expected_stocks:
            LOG.error(f"Incomplete data: Found {unique_stocks} stocks out of {expected_stocks} expected for {today}")
            return False
        
        LOG.info(f"Data validation successful: {unique_stocks} stocks found for {today}")
        return True

    except Exception as e:
        LOG.error(f"Error checking stock data: {str(e)}")
        return False

if __name__ == "__main__":
    is_data_valid = check_stock_data()
    
    if not is_data_valid:
        LOG.warning("Index data needs to be updated!")
    else:
        LOG.info("Index data is up to date!")