import datetime
from supabase import create_client
from common.logging import LOG
import os


def get_new_lq_stock_request_payload(request_name, stocks_added):
    return {
        "@type": "HistoryRequest",
        "name": request_name,
        "description": "IDX stocks history request",
        "universe": {
            "@type": "Universe",
            "contains": [
                *[
                    {
                        "@type": "Identifier",
                        "identifierType": "TICKER",
                        "identifierValue": stock + " IJ Equity",
                    }
                    for stock in stocks_added
                ]
            ],
        },
        "fieldList": {
            "@type": "HistoryFieldList",
            "contains": [
                {"mnemonic": "PX_OPEN"},
                {"mnemonic": "PX_HIGH"},
                {"mnemonic": "PX_LOW"},
                {"mnemonic": "PX_LAST"},
                {"mnemonic": "PX_VOLUME"},
                {"mnemonic": "TURNOVER"},
                {"mnemonic": "NUM_TRADES"},
                {"mnemonic": "CHG_PCT_1D"},
            ],
        },
        "trigger": {"@type": "SubmitTrigger"},
        "runtimeOptions": {
            "@type": "HistoryRuntimeOptions",
            "dateRange": {
                "@type": "IntervalDateRange",
                "startDate": (
                    datetime.datetime.now() - datetime.timedelta(days=730)
                ).strftime("%Y-%m-%d"),
                "endDate": (
                    datetime.datetime.now() - datetime.timedelta(days=1)
                ).strftime("%Y-%m-%d"),
            },
        },
        "formatting": {
            "@type": "MediaType",
            "outputMediaType": "application/json",
        },
    }


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

        stocks_today = set([record["stock"] for record in records_today])
        stocks_yesterday = set([record["stock"] for record in records_yesterday])

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

    # Initialize Supabase client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase = create_client(url, key)

    # Get yesterday's date (since the stock data is from previous day)
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    try:
        # Query to check data for yesterday's date
        response = (
            supabase.table("bloomberg_stock_histories")
            .select("stock_id, report_date")
            .eq("report_date", yesterday)
            .in_("stock_id", list(stocks_added))
            .execute()
        )

        records = response.data

        if not records:
            LOG.error(f"No data found for added stocks on date: {yesterday}")
            return False

        # Count unique stocks (should match number of added stocks)
        unique_stocks = len(set(record["stock_id"] for record in records))
        expected_stocks = len(stocks_added)

        if unique_stocks < expected_stocks:
            LOG.error(
                f"Incomplete data: Found {unique_stocks} stocks out of {expected_stocks} added stocks expected for {yesterday}"
            )
            return False

        LOG.info(
            f"Data validation successful: {unique_stocks} added stocks found for {yesterday}"
        )
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
