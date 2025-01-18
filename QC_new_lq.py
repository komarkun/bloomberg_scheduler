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

def check_stock_data(stocks_added):
    # Load credentials
    with open('ps-credential.txt', encoding="utf-8") as credential_file:
        CREDENTIALS = json.load(credential_file)

    # Initialize Supabase client
    url = "https://datalake.batinvestasi.com/"
    supabase = create_client(url, CREDENTIALS['supabase_key'])

    # Get yesterday's date (since the stock data is from previous day)
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    try:
        # Query to check data for yesterday's date
        response = supabase.table('bloomberg_stock_histories') \
            .select('stock_id, report_date') \
            .eq('report_date', yesterday) \
            .in_('stock_id', list(stocks_added)) \
            .execute()
        
        records = response.data

        if not records:
            LOG.error(f"No data found for added stocks on date: {yesterday}")
            return False
        
        # Count unique stocks (should match number of added stocks)
        unique_stocks = len(set(record['stock_id'] for record in records))
        expected_stocks = len(stocks_added)
        
        if unique_stocks < expected_stocks:
            LOG.error(f"Incomplete data: Found {unique_stocks} stocks out of {expected_stocks} added stocks expected for {yesterday}")
            return False
        
        LOG.info(f"Data validation successful: {unique_stocks} added stocks found for {yesterday}")
        return True

    except Exception as e:
        LOG.error(f"Error checking stock data: {str(e)}")
        return False

if __name__ == "__main__":
    stocks_added, _ = check_lq_data() 

    is_data_valid = check_stock_data(stocks_added)

    if not is_data_valid:
        LOG.warning("Stock data for added stocks needs to be updated!")
    else:
        LOG.info("Stock data for added stocks is up to date!")