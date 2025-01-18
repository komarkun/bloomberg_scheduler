from utils.new_lq_stock.bloomberg_data_pipeline import (
    check_lq_data,
    check_stock_data as _check_stock_data,
)

from common.logging import LOG


def check_stock_data(ti):
    stocks_added, _ = check_lq_data()

    is_data_valid = _check_stock_data(stocks_added)

    if not is_data_valid:
        raise RuntimeError("Stock data for added stocks needs to be updated!")
    else:
        LOG.info("Stock data for added stocks is up to date!")
