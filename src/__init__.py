import os

from datetime import date
from pathlib import Path

from loguru import logger

logs_dir = Path("/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/logs")


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
