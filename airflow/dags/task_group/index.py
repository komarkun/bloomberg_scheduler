from common.enums import TaskId
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.ingestion.common.bloomberg_data_pipeline import (
    fetch_bloomberg_data,
)
from tasks.ingestion.index.bloomberg_data_pipeline import refame_index_dataframe
from tasks.ingestion.common.bloomberg_data_pipeline import (
    download_file_stream,
    read_data_frame,
    save_to_database,
)

from tasks.qc.index.bloomberg_data_pipeline import check_index_data
from utils.index.bloomberg_data_pipeline import get_index_request_payload


def create_index_task_group(group_id):
    with TaskGroup(group_id=group_id) as index_pipeline:
        fetch_bloomberg_index_data_task = PythonOperator(
            task_id=TaskId.FETCH_BLOOMBERG_DATA.value,
            python_callable=fetch_bloomberg_data,
            op_kwargs={"request_payload_callable": get_index_request_payload},
        )

        download_file_task = PythonOperator(
            task_id=TaskId.DOWNLOAD_FILE_STREAM.value,
            python_callable=download_file_stream,
        )

        read_data_task = PythonOperator(
            task_id=TaskId.READ_DF.value,
            python_callable=read_data_frame,
        )

        refame_task = PythonOperator(
            task_id=TaskId.REFAME_DF.value,
            python_callable=refame_index_dataframe,
        )

        upsert_task = PythonOperator(
            task_id=TaskId.INSERT.value,
            python_callable=save_to_database,
            op_kwargs={"tb_name": "bloomberg_stock_index_histories", "upsert": True},
        )

        qc_task = PythonOperator(
            task_id=TaskId.QUALITY_CHECK.value,
            python_callable=check_index_data,
        )

        (
            fetch_bloomberg_index_data_task
            >> download_file_task
            >> read_data_task
            >> refame_task
            >> upsert_task
            >> qc_task
        )

        return index_pipeline
