from loguru import logger
import os
from pathlib import Path

from logs import __path__ as logs_dir
from datetime import date

logs_dir = Path(logs_dir[0])

os.makedirs(logs_dir, exist_ok=True)
# logger.remove()
logfile = logs_dir / f"{date.isoformat(date.today())}.log"
logger.add(
    sink=logfile,
    format="{time}|{level}|{file}:{line}:{function}{} - {message}",
    colorize=True,
    level="TRACE",
    backtrace=True,
    diagnose=True,
)
