import datetime


def get_index_request_payload(request_name):
    return {
        "@type": "HistoryRequest",
        "name": request_name,
        "description": "IDX stocks history request",
        "universe": {
            "@type": "Universe",
            "contains": [
                {
                    "@type": "Identifier",
                    "identifierType": "TICKER",
                    "identifierValue": "LQ45 INDEX",
                },
                {
                    "@type": "Identifier",
                    "identifierType": "TICKER",
                    "identifierValue": "JCI INDEX",
                },
            ],
        },
        "fieldList": {
            "@type": "HistoryFieldList",
            "contains": [
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
                # Get data for yesterday and today
                "startDate": (
                    datetime.datetime.now() - datetime.timedelta(days=4)
                ).strftime("%Y-%m-%d"),
                "endDate": (datetime.datetime.now()).strftime("%Y-%m-%d"),
            },
        },
        "formatting": {
            "@type": "MediaType",
            "outputMediaType": "application/json",
        },
    }
