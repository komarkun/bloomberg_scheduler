from utils.scheduler import parse_group_id
from common.enums import TaskId

import pandas as pd


def refame_index_dataframe(ti):

    group_id, task_name = parse_group_id(ti.task_id)

    if task_name != None:
        group_id += "."

    df = ti.xcom_pull(key="df", task_ids=group_id + TaskId.READ_DF.value)

    # Get today's date
    today = pd.Timestamp.now().date()

    # Sort by identifier and date
    df = df.sort_values(["IDENTIFIER", "DATE"])

    # Create a shifted column for previous day's close
    df["previous_price"] = df.groupby("IDENTIFIER")["PX_LAST"].shift(1)

    # Create new dataframe with renamed and reordered columns
    new_df = pd.DataFrame(
        {
            # Remove ' INDEX' suffix
            "index_code": df["IDENTIFIER"].str.replace(" INDEX", ""),
            "report_date": df["DATE"],
            "previous_price": df["previous_price"],
            "high_price": df["PX_HIGH"],
            "low_price": df["PX_LOW"],
            "close_price": df["PX_LAST"],
            "volume": df["PX_VOLUME"],
            "value": df["TURNOVER"],
            "frequency": df["NUM_TRADES"],
            "change_percent": df["CHG_PCT_1D"],
        }
    )

    # Replace 'JCI' with 'COMPOSITE' in index_code column
    new_df["index_code"] = new_df["index_code"].replace("JCI", "COMPOSITE")

    # Filter for today's date only
    new_df = new_df[new_df["report_date"].dt.date == today]

    # Fill NaN values with 0
    new_df = new_df.fillna(0).astype(
        {"volume": "int64", "value": "int64", "frequency": "int64"}
    )

    # Sort the dataframe by index_code
    new_df = new_df.sort_values("index_code")

    # First convert to datetime, then format as string
    new_df["report_date"] = pd.to_datetime(new_df["report_date"])
    new_df["report_date"] = new_df["report_date"].dt.strftime("%Y-%m-%d")

    # Convert DataFrame to list of dictionaries
    records = new_df.to_dict("records")

    ti.xcom_push(key="records", value=records)
