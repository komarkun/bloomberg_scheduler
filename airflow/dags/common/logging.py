import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s",
)
LOG = logging.getLogger(__name__)
