import sys
import logging
from .config import FluviiConfig


def _init_handler(loglevel=None):
    if not loglevel:
        loglevel = _get_loglevel()
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(loglevel)

    handler.setFormatter(
        logging.Formatter(
            "PID {process} - {asctime} - {name} - {levelname}: {message}",
            style="{"
        )
    )
    return handler


def _get_loglevel():
    return FluviiConfig().loglevel


def init_logger(name, loglevel=None):
    if not loglevel:
        loglevel = _get_loglevel()
    logger = logging.getLogger(name)
    logger.setLevel(loglevel)
    logger.addHandler(_init_handler(loglevel=loglevel))
    logger.propagate = False
    return logger
