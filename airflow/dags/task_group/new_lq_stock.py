from common.enums import TaskId
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.ingestion.new_lq_stock.bloomberg_data_pipeline import (
    fetch_bloomberg_stock_data,
)
from tasks.ingestion.stock.bloomberg_data_pipeline import refame_stock_dataframe
from tasks.qc.new_lq_stock.bloomberg_data_pipeline import check_stock_data

from tasks.ingestion.common.bloomberg_data_pipeline import (
    download_file_stream,
    read_data_frame,
    save_to_database,
)


def create_new_lq_stock_task_group(group_id):
    with TaskGroup(group_id=group_id) as stock_pipeline:

        fetch_bloomberg_stock_data_task = PythonOperator(
            task_id=TaskId.FETCH_BLOOMBERG_DATA.value,
            python_callable=fetch_bloomberg_stock_data,
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
            python_callable=refame_stock_dataframe,
        )

        upsert_task = PythonOperator(
            task_id=TaskId.INSERT.value,
            python_callable=save_to_database,
            op_kwargs={"tb_name": "bloomberg_stock_histories", "upsert": True},
        )

        qc_task = PythonOperator(
            task_id=TaskId.QUALITY_CHECK.value,
            python_callable=check_stock_data,
        )

        (
            fetch_bloomberg_stock_data_task
            >> download_file_task
            >> read_data_task
            >> refame_task
            >> upsert_task
            >> qc_task
        )

        return stock_pipeline
