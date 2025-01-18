from utils.new_lq_stock.bloomberg_data_pipeline import (
    check_lq_data,
    get_new_lq_stock_request_payload,
)
from tasks.ingestion.common.bloomberg_data_pipeline import fetch_bloomberg_data


def fetch_bloomberg_stock_data(ti):
    stocks_added, _ = check_lq_data()

    request_payload_callable = lambda request_name: get_new_lq_stock_request_payload(
        request_name=request_name, stocks_added=stocks_added
    )
    return fetch_bloomberg_data(ti, request_payload_callable=request_payload_callable)
