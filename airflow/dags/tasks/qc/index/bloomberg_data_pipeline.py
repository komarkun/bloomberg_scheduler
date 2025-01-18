from common.logging import LOG
from supabase import create_client
import datetime
import os


def check_index_data():
    # Initialize Supabase client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase = create_client(url, key)

    # Get yesterday's date (since the stock data is from previous day)
    today = (datetime.datetime.now()).strftime("%Y-%m-%d")

    try:
        # Query to check data for yesterday's date
        response = (
            supabase.table("bloomberg_stock_index_histories")
            .select("index_code, report_date")
            .eq("report_date", today)
            .execute()
        )

        records = response.data

        if not records:
            LOG.error(f"No data found for date: {today}")
            return False

        # Count unique stocks (should match your expected number of stocks)
        unique_stocks = len(set(record["index_code"] for record in records))
        expected_stocks = 2  # Based on your stock list in the original code

        if unique_stocks < expected_stocks:
            LOG.error(
                f"Incomplete data: Found {unique_stocks} stocks out of {expected_stocks} expected for {today}"
            )
            return False

        LOG.info(
            f"Data validation successful: {unique_stocks} stocks found for {today}"
        )
        return True

    except Exception as e:
        # LOG.error(f"Error checking stock data: {str(e)}")
        raise RuntimeError(f"Error checking stock data: {str(e)}")
        # return False
