from utils.scheduler import parse_group_id
from common.enums import TaskId
from tasks.ingestion.common.bloomberg_data_pipeline import fetch_bloomberg_data
from utils.stock.bloomberg_data_pipeline import get_stock_request_payload, check_lq_data

import pandas as pd


def fetch_bloomberg_stock_data(ti):
    stocks_today = check_lq_data()

    request_payload_callable = lambda request_name: get_stock_request_payload(
        request_name=request_name, stocks_today=stocks_today
    )
    return fetch_bloomberg_data(ti, request_payload_callable=request_payload_callable)


def refame_stock_dataframe(ti):

    group_id, task_name = parse_group_id(ti.task_id)

    if task_name != None:
        group_id += "."

    df = ti.xcom_pull(key="df", task_ids=group_id + TaskId.READ_DF.value)

    new_df = pd.DataFrame(
        {
            # Remove ' IJ Equity' suffix
            "stock_id": df["IDENTIFIER"].str.replace(" IJ Equity", ""),
            "report_date": df["DATE"],
            "open_price": df["PX_OPEN"],
            "high_price": df["PX_HIGH"],
            "low_price": df["PX_LOW"],
            "close_price": df["PX_LAST"],
            "volume": df["PX_VOLUME"],
            "value": df["TURNOVER"],
            "frequency": df["NUM_TRADES"],
            "change_percent": df["CHG_PCT_1D"],
        }
    )

    # Sort the dataframe by stock_id and report_date
    new_df = new_df.sort_values(["stock_id", "report_date"], ascending=[True, False])

    # Fill NaN values with 0
    new_df = new_df.fillna(0).astype(
        {"volume": "int64", "value": "int64", "frequency": "int64"}
    )

    # First convert to datetime, then format as string
    new_df["report_date"] = pd.to_datetime(new_df["report_date"])
    new_df["report_date"] = new_df["report_date"].dt.strftime("%Y-%m-%d")

    # Convert DataFrame to list of dictionaries
    records = new_df.to_dict("records")

    ti.xcom_push(key="records", value=records)
