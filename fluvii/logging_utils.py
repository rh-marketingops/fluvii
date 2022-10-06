import sys
import logging
from os import environ


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
    return environ.get('FLUVII_LOGLEVEL', 'INFO')


def init_logger(name, loglevel=None):
    if not loglevel:
        loglevel = _get_loglevel()
    logger = logging.getLogger(name)
    logger.setLevel(loglevel)
    logger.addHandler(_init_handler(loglevel=loglevel))
    logger.propagate = False
    return logger
