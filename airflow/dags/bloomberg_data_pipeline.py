from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from utils.send_telegram_message import (
    send_telegram_failure_message,
    send_telegram_retry_message,
    send_telegram_success_message,
)
import datetime

from common.enums import TaskGroupId, TaskId
from tasks.ingestion.common.bloomberg_data_pipeline import (
    clean_downloads_folder,
    fetch_scheduled_catalog,
)


from task_group.index import create_index_task_group
from task_group.index_weightge import create_index_weightge_task_group


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_retry": True,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "on_failure_callback": send_telegram_failure_message,
    "on_retry_callback": send_telegram_retry_message,
    "on_success_callback": send_telegram_success_message,
}

with DAG(
    "bloomberg_data_pipeline",
    description="Bloomberg Data Pipeline",
    default_args=default_args,
    schedule_interval="30 17 * * *",
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["bloomberg_data_pipeline"],
) as dag:

    fetch_scheduled_catalog_task = PythonOperator(
        task_id=TaskId.FETCH_SCHEDULED_CATALOG.value,
        python_callable=fetch_scheduled_catalog,
    )

    # Group 1: Index Data Pipeline
    index_pipeline = create_index_task_group(group_id=TaskGroupId.INDEX.value)

    # Group 2: Index Weight Pipeline
    index_weightge_pipeline = create_index_weightge_task_group(
        group_id=TaskGroupId.INDEX_WEIGHT.value
    )

    # Cleanup task
    clean_downloads_folder_task = PythonOperator(
        task_id=TaskId.CLEAN_UP.value,
        python_callable=clean_downloads_folder,
        trigger_rule="all_done",
    )

    fetch_scheduled_catalog_task >> [
        index_pipeline,
        index_weightge_pipeline,
    ]

    chain(index_pipeline, clean_downloads_folder_task)
    chain(index_weightge_pipeline, clean_downloads_folder_task)
