from utils.scheduler import parse_group_id
from common.enums import TaskId
from common.logging import LOG
from utils.index_weightge.bloomberg_data_pipeline import check_lq_data

import numpy as np
import pandas as pd


def refame_index_weightge_dataframe(ti):

    group_id, task_name = parse_group_id(ti.task_id)

    if task_name != None:
        group_id += "."

    df = ti.xcom_pull(key="df", task_ids=group_id + TaskId.READ_DF.value)

    for _, row in df.iterrows():
        index_name = row["IDENTIFIER"]

        weights_data = row["INDX_MWEIGHT"]
        if isinstance(weights_data, np.ndarray):  # If it's a NumPy array
            weights_data = weights_data.tolist()

        # Convert the list of dictionaries to a dataframe
        weights_df = pd.DataFrame(weights_data)[
            ["BC_INDX_MWEIGHT_TKR_EXCH", "BC_INDX_MWEIGHT_PCT"]
        ]

        # Clean and sort the dataframe
        weights_df = (
            weights_df.assign(
                # Remove 'IJ' from stock names
                BC_INDX_MWEIGHT_TKR_EXCH=weights_df[
                    "BC_INDX_MWEIGHT_TKR_EXCH"
                ].str.replace(" IJ", ""),
                # Round weights to 2 decimal places
                BC_INDX_MWEIGHT_PCT=weights_df["BC_INDX_MWEIGHT_PCT"].round(2),
            )
            # Sort by weight in descending order
            .sort_values("BC_INDX_MWEIGHT_PCT", ascending=False)
            # Rename columns
            .rename(
                columns={
                    "BC_INDX_MWEIGHT_TKR_EXCH": "stock",
                    "BC_INDX_MWEIGHT_PCT": "weight",
                }
            )
            # Reset index after sorting
            .reset_index(drop=True)
        )

        # Store in separate variables
        if index_name == "LQ45 Index":
            lq45_weights = weights_df

            # Add timestamp column
            lq45_weights["timestamp"] = pd.Timestamp.now()
            lq45_weights["timestamp"] = lq45_weights["timestamp"].dt.strftime(
                "%Y-%m-%d"
            )

            # Convert DataFrame to records for Supabase insertion
            records = lq45_weights.to_dict("records")

            ti.xcom_push(key="records", value=records)


def check_lq_data_change(ti, branch_true_name, branch_false_name):
    is_lq_different = check_lq_data()

    if is_lq_different:
        LOG.warning("LQ45 data doesnt match!")
        LOG.info("LQ45 data need to be pulled from bloomberg!")
        return branch_true_name
    else:
        LOG.info("LQ45 data match!")
        return branch_false_name
