import datetime


from common.logging import LOG

import datetime
from supabase import create_client

import os


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

        records_today = response_today.data

        stocks_today = set([record["stock"] for record in records_today])

        return stocks_today
    except Exception as e:
        LOG.error(f"Error checking stock data: {str(e)}")
        return []


def get_stock_request_payload(request_name, stocks_today):
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
                    for stock in stocks_today
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
                "startDate": (datetime.datetime.now()).strftime("%Y-%m-%d"),
                "endDate": (datetime.datetime.now()).strftime("%Y-%m-%d"),
            },
        },
        "formatting": {
            "@type": "MediaType",
            "outputMediaType": "application/json",
        },
    }
