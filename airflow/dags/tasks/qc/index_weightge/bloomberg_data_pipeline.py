from common.logging import LOG
from supabase import create_client
import datetime
import os


def check_index_weightge_data():
    # Initialize Supabase client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase = create_client(url, key)

    # Get yesterday's date (since the stock data is from previous day)
    today = (datetime.datetime.now()).strftime("%Y-%m-%d")

    try:
        # Query to check data for yesterday's date
        response = (
            supabase.table("LQ45_Weight")
            .select("stock, timestamp")
            .eq("timestamp", today)
            .execute()
        )

        records = response.data

        if not records:
            LOG.error(f"No data found for date: {today}")
            return False

        # Updated number of stocks based on the provided weightage data
        expected_stocks = 45  # New total from the weightage data
        unique_stocks = len(set(record["stock"] for record in records))

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
        # return False
        raise RuntimeError(f"Error checking stock data: {str(e)}")
