import logging

from edisgo.tools.logger import setup_logger

from lobaflex import config_dir
from lobaflex.tools.tools import get_config


def setup_logging(file_name):
    """
    Modify and setup edisgo logger

    Parameters
    ----------
    file_name :

    Returns
    -------

    """

    stream_formatter = logging.Formatter(
        "{levelname:<8s} | {name:>40s}: Line {lineno:<6d} - "
        "{funcName:>40s}(): {message:s}",
        style="{",
    )

    file_formatter = logging.Formatter(
        "{levelname:<8s} - {asctime} | {name:>30s}: Line {lineno:<6d}"
        " - {funcName:>40s}(): {message:s}",
        style="{",
    )

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    file_level = cfg_o.get("log_to_files", "warning")
    stream_level = cfg_o.get("log_to_stream", "info")

    loggers = [
        {"name": "edisgo", "file_level": file_level, "stream_level": stream_level},  # noqa: F401
        {"name": "lobaflex", "file_level": file_level, "stream_level": stream_level},  # noqa: F401
        {"name": "pyomo.core", "file_level": file_level, "stream_level": stream_level},  # noqa: F401
        {"name": "pypsa", "file_level": file_level, "stream_level": stream_level},  # noqa: F401
    ]

    setup_logger(
        loggers=loggers,
        file_formatter=file_formatter,
        stream_formatter=stream_formatter,
        reset_loggers=True,
        file_name=file_name,
    )
