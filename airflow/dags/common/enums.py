from enum import Enum


class TaskId(Enum):
    FETCH_BLOOMBERG_DATA = "fetch_bloomberg_data_task"
    FETCH_SCHEDULED_CATALOG = "fetch_scheduled_catalog_task"
    DOWNLOAD_FILE_STREAM = "download_file_stream_task"
    READ_DF = "read_df_task"
    REFAME_DF = "refame_df_task"
    INSERT = "insert_task"
    CLEAN_UP = "clean_up_task"
    QUALITY_CHECK = "quality_check_task"


class TaskGroupId(Enum):
    INDEX = "index_data_pipeline"
    STOCK = "stock_data_pipeline"
    INDEX_WEIGHT = "index_weightge_data_pipeline"
    NEW_LQ_STOCK = "new_lq_stock_data_pipeline"


class TaskBranchId(Enum):
    IS_LQ_DATA_NOT_UPDATED = "is_lq_data_not_updated"
    IS_LQ_DATA_LATEST = "is_lq_data_latest"
