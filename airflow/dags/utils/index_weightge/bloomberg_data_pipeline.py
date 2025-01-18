from common.settings import HOST
from urllib.parse import urljoin
from supabase import create_client
import datetime
from common.logging import LOG
import os


def get_index_weightge_responses_url(catalog_id):
    return urljoin(HOST, "/eap/catalogs/{c}/content/responses/".format(c=catalog_id))


def check_lq_data():
    # Initialize Supabase client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase = create_client(url, key)

    # Get Today & Yesterday's date (since the stock data is from previous day)
    today = (datetime.datetime.now()).strftime("%Y-%m-%d")
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    try:
        # Query to check data for yesterday's date
        response_today = (
            supabase.table("LQ45_Weight")
            .select("stock, timestamp")
            .eq("timestamp", today)
            .execute()
        )

        response_yesterday = (
            supabase.table("LQ45_Weight")
            .select("stock, timestamp")
            .eq("timestamp", yesterday)
            .execute()
        )

        records_today = response_today.data
        records_yesterday = response_yesterday.data

        stocks_today = [record["stock"] for record in records_today]
        stocks_yesterday = [record["stock"] for record in records_yesterday]

        if sorted(stocks_today) == sorted(stocks_yesterday):
            return False
        else:
            return True

    except Exception as e:
        LOG.error(f"Error checking stock data: {str(e)}")
        return False
