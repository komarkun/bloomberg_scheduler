from common.enums import TaskId, TaskBranchId, TaskGroupId
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup

from task_group.stock import create_stock_task_group
from task_group.new_lq_stock import create_new_lq_stock_task_group

from tasks.ingestion.common.bloomberg_data_pipeline import (
    download_file_stream,
    read_data_frame,
    save_to_database,
)

from tasks.ingestion.index_weightge.bloomberg_data_pipeline import (
    refame_index_weightge_dataframe,
    check_lq_data_change,
)

from tasks.qc.index_weightge.bloomberg_data_pipeline import check_index_weightge_data

from utils.index_weightge.bloomberg_data_pipeline import (
    get_index_weightge_responses_url,
)


def create_index_weightge_task_group(group_id):
    with TaskGroup(group_id=group_id) as index_weightge_data_pipeline:

        download_file_task = PythonOperator(
            task_id=TaskId.DOWNLOAD_FILE_STREAM.value,
            python_callable=download_file_stream,
            op_kwargs={
                "external_responses_url_callable": get_index_weightge_responses_url,
                "external_param": {
                    "prefix": "IndexWeight",
                    "requestIdentifier": "IndexWeightDaily",
                },
            },
        )

        read_data_task = PythonOperator(
            task_id=TaskId.READ_DF.value,
            python_callable=read_data_frame,
        )

        refame_task = PythonOperator(
            task_id=TaskId.REFAME_DF.value,
            python_callable=refame_index_weightge_dataframe,
        )

        insert_task = PythonOperator(
            task_id=TaskId.INSERT.value,
            python_callable=save_to_database,
            op_kwargs={"tb_name": "LQ45_Weight", "upsert": False},
        )

        qc_task = PythonOperator(
            task_id=TaskId.QUALITY_CHECK.value,
            python_callable=check_index_weightge_data,
        )

        check_lq_data_change_task = BranchPythonOperator(
            task_id="check_lq_data_change_branch_task",
            python_callable=check_lq_data_change,
            trigger_rule="all_done",
            op_kwargs={
                "branch_true_name": f"{group_id}.{TaskBranchId.IS_LQ_DATA_NOT_UPDATED.value}",
                "branch_false_name": f"{group_id}.{TaskBranchId.IS_LQ_DATA_LATEST.value}",
            },
        )

        with TaskGroup(
            group_id=TaskBranchId.IS_LQ_DATA_NOT_UPDATED.value
        ) as is_lq_data_not_updated_branch:
            is_lq_data_not_updated_branch_stock = create_stock_task_group(
                group_id=TaskGroupId.STOCK.value
            )

            is_lq_data_not_updated_branch_new_lq_stock = create_new_lq_stock_task_group(
                group_id=TaskGroupId.NEW_LQ_STOCK.value
            )

            is_lq_data_not_updated_branch_stock
            is_lq_data_not_updated_branch_new_lq_stock

        is_lq_data_latest_branch = create_stock_task_group(
            group_id=TaskBranchId.IS_LQ_DATA_LATEST.value
        )

        (
            download_file_task
            >> read_data_task
            >> refame_task
            >> insert_task
            >> qc_task
            >> check_lq_data_change_task
            >> [is_lq_data_latest_branch, is_lq_data_not_updated_branch]
        )

        return index_weightge_data_pipeline
