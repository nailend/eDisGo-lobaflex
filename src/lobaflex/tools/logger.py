import logging

from edisgo.tools.logger import setup_logger


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

    setup_logger(
        loggers=[
            {"name": "edisgo", "file_level": "info", "stream_level": "info"},
            {"name": "lobaflex", "file_level": "info", "stream_level": "info"},
            {"name": "pyomo.core", "file_level": "info", "stream_level": "info"},  # noqa: F401
            {"name": "pypsa", "file_level": "info", "stream_level": "info"},
        ],
        file_formatter=file_formatter,
        stream_formatter=stream_formatter,
        reset_loggers=True,
        file_name=file_name,
    )
