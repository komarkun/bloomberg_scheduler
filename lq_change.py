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
        
        stocks_today = [record['stock'] for record in records_today]
        stocks_yesterday = [record['stock'] for record in records_yesterday]
        
        if sorted(stocks_today) == sorted(stocks_yesterday):
            return False
        else:
            return True

    except Exception as e:
        LOG.error(f"Error checking stock data: {str(e)}")
        return False

if __name__ == "__main__":
    is_lq_different = check_lq_data()
    
    if is_lq_different:
        LOG.warning("LQ45 data doesnt match!")
        LOG.info("LQ45 data need to be pulled from bloomberg!")
    else:
        LOG.info("LQ45 data match!")