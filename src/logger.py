import os

from datetime import date
from pathlib import Path

from loguru import logger

from tools import get_dir

logs_dir = get_dir(key="logs")

os.makedirs(logs_dir, exist_ok=True)
# logger.remove()
logfile = logs_dir / f"{date.isoformat(date.today())}.log"
logger.add(
    sink=logfile,
    format="{time}|{level}|{file}:{line}:{function} - {message}",
    colorize=True,
    level="TRACE",
    backtrace=True,
    diagnose=True,
)
