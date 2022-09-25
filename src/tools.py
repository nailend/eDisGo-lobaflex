""""""
import logging
import os
import time

from datetime import date
from pathlib import Path

import yaml

from loguru import logger


def get_config(path="./model_config.yaml"):
    """
    Returns the config.
    """
    with open(path, encoding="utf8") as f:
        return yaml.safe_load(f)


def setup_logfile(cfg):
    working_dir = Path(cfg["model"]["working-dir"])
    os.makedirs(working_dir, exist_ok=True)

    # logger.remove()
    logfile = working_dir / Path(f"{date.isoformat(date.today())}.log")
    logger.add(
        sink=logfile,
        format="{time} {level} {message}",
        level="TRACE",
        backtrace=True,
        diagnose=True,
    )

    logger.info("Start")


def show_logger():
    """Show all logger"""
    existing_loggers = [logging.getLogger()]  # get the root logger
    existing_loggers = existing_loggers + [
        logging.getLogger(name) for name in logging.root.manager.loggerDict
    ]

    for indiv_logger in existing_loggers:
        print(indiv_logger)
        print(indiv_logger.handlers)
        print("-" * 20)


def timeit(func):
    """
    Decorator for measuring function's running time.
    """

    def measure_time(*args, **kw):
        start_time = time.time()
        result = func(*args, **kw)
        print(
            "Processing time of %s(): %.2f seconds."
            % (func.__qualname__, time.time() - start_time)
        )
        return result

    return measure_time
