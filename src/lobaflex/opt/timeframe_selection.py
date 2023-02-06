import logging
import warnings

from datetime import datetime

import pandas as pd

from edisgo.edisgo import EDisGo, import_edisgo_from_files
from edisgo.network.timeseries import TimeSeries

from lobaflex import config_dir, logs_dir, results_dir
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, log_errors

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
    logger.propagate = False
else:
    logger = logging.getLogger(__name__)
    logger.propagate = False


def extract_timeframe(
    edisgo_obj,
    timeframe=None,
    start_datetime=None,
    periods=None,
    freq="1h",
    ts=True,
    bev=True,
    hp=True,
):
    """Extracts a given time frame from the edisgo object for all time series
    which are defined in the edisgo object and flaged.

    Parameters
    ----------
    edisgo_obj : :class:`edisgo.EDisGo`
        Edisgo object
    timeframe : TimeIndex or None
        Timeframe to extract
    start_datetime : str or timeindex or None
        Start datetime of time series
    periods : int or none
        Number of periods of time series
    freq : str
        Frequency of time series, default "1h"
    ts : bool
        Extract time series, default True
    bev :
        Extract battery electric vehicle time series, default True
    hp :
        Extract heat pump time series, default True

    Returns
    -------
    :class:`edisgo.EDisGo`
        Edisgo object with extracted time series

    """
    # edisgo_obj = deepcopy(edisgo_obj)

    if timeframe is None:
        timeframe = pd.date_range(
            start=start_datetime, periods=periods, freq=freq
        )

    if not (timeframe.isin(edisgo_obj.timeseries.timeindex)).all():
        # logger.exception()
        raise ValueError(
            "Edisgo object does not contain all the given timeindex"
        )
    # adapt timeseries
    if ts:
        attributes = TimeSeries()._attributes
        edisgo_obj.timeseries.timeindex = timeframe
        for attr in attributes:
            if not getattr(edisgo_obj.timeseries, attr).empty:
                setattr(
                    edisgo_obj.timeseries,
                    attr,
                    getattr(edisgo_obj.timeseries, attr).loc[timeframe],
                )
        # logger.info("")
    # Battery electric vehicle timeseries
    if bev:
        for key, df in edisgo_obj.electromobility.flexibility_bands.items():
            if not df.empty:
                df = df.loc[timeframe]
                edisgo_obj.electromobility.flexibility_bands.update({key: df})
    # Heat pumps timeseries
    if hp:
        for attr in ["cop_df", "heat_demand_df"]:
            if not getattr(edisgo_obj.heat_pump, attr).empty:
                setattr(
                    edisgo_obj.heat_pump,
                    attr,
                    getattr(edisgo_obj.heat_pump, attr).loc[timeframe],
                )

    logger.info(
        f"Timeseries taken: {timeframe[0]} -> "
        f"{timeframe[-1]} including {periods} timesteps."
    )
    return edisgo_obj


@log_errors
def run_timeframe_selection(
    obj_or_path,
    grid_id,
    run_id=None,
    version_db=None,
):
    """Extract timeframe from edisgo object or dump. Currently using timeindex
    generate from config file input.

    Parameters
    ----------
    obj_or_path : :class:`edisgo.EDisGo` or PosixPath
        edisgo object or path to edisgo dump
    grid_id :
        grid id of MVGD
    run_id : str
        run id used for pydoit versioning
    version_db : dict
        Dictionary with version information for pydoit versioning


    Returns
    -------
    If run_id and version are not None, a dictionary with these values is
    given for the pydoit versioning.

    """
    # Log to pipeline log file
    logger.info(f"Run timeframe selection of {grid_id}")

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg_o = get_config(path=config_dir / ".opt.yaml")

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"opt_{run_id}_{grid_id}_{date}.log"
    setup_logging(file_name=logfile)

    logger.info(
        f"Run timeframe selection for grid: {grid_id} with run id: {run_id}"
    )
    export_path = results_dir / run_id / str(grid_id) / "reference_mvgd"

    if isinstance(obj_or_path, EDisGo):
        edisgo_obj = obj_or_path
    else:

        logger.info(f"Import Grid from file: {obj_or_path}")

        edisgo_obj = import_edisgo_from_files(
            obj_or_path,
            import_topology=True,
            import_timeseries=True,
            import_heat_pump=True,
            import_electromobility=True,
        )

    logger.info("Extract timeframe")
    edisgo_obj = extract_timeframe(
        edisgo_obj,
        start_datetime=cfg_o["start_datetime"],
        periods=cfg_o["total_timesteps"],
        freq="1h",
    )

    logger.info("Save reduced grid")
    edisgo_obj.save(
        export_path,
        save_topology=True,
        save_timeseries=True,
        save_heatpump=True,
        save_electromobility=True,
        save_results=True,
    )

    if version_db is not None:
        return version_db["db"]
