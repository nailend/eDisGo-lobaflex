import logging
import warnings

from copy import deepcopy
from datetime import datetime
from functools import partial

import pandas as pd

from edisgo.edisgo import EDisGo, import_edisgo_from_files

from lobaflex import config_dir, logs_dir, results_dir
from lobaflex.opt.timeframe_selection import extract_timeframe
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, get_files_in_subdirs, log_errors

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
else:
    logger = logging.getLogger(__name__)


def integrate_timeseries(
    edisgo_obj,
    import_path,
    parameters,
    start_datetime=None,
    periods=None,
    run_id=None,
):
    """Selected time series parameters are imported from a given directory
    and integrated into the edisgo object for the defined time frame.

    Parameters
    ----------
    edisgo_obj : :class:`edisgo.EDisGo`
        EDisGo object
    import_path : PosixPath
        Path to directory where results are stored
    parameters : list of str
        List of time series parameters to import
    start_datetime : str or timeindex or None
        Start datetime of time series
    periods : int or none
        Number of periods of time series, frequency is 1h
    run_id : str or None
        Run id for pydoit versioning

    Returns
    -------
    :class:`edisgo.EDisGo`
        EDisGo object with integrated time series
    """
    edisgo_obj = deepcopy(edisgo_obj)

    cfg_o = get_config(path=config_dir / ".opt.yaml")

    grid_id = edisgo_obj.topology.mv_grid.id

    logger.info(f"Start result integration of {grid_id} in {run_id}.")

    # results_path = results_dir / run_id / str(grid_id) / "mvgd"

    list_of_files = get_files_in_subdirs(import_path, pattern="*.csv")

    filenames = [
        file
        for file in list_of_files
        for parameter in parameters
        if parameter in file
    ]

    logger.info(f"Import results of {' & '.join(parameters)}.")
    df_loads_active_power = pd.concat(
        map(partial(pd.read_csv, index_col=0, parse_dates=True), filenames),
        axis=1,
    )

    # mark all optimised loads
    edisgo_obj.topology.loads_df.loc[:, "opt"] = False
    edisgo_obj.topology.loads_df.loc[
        df_loads_active_power.columns, "opt"
    ] = True
    # define timeframe to concat
    if start_datetime and periods is not None:
        timeframe = pd.date_range(
            start=cfg_o["start_datetime"],
            periods=cfg_o["total_timesteps"],
            freq="1h",
        )
        df_loads_active_power = df_loads_active_power.loc[timeframe]
    else:
        timeframe = df_loads_active_power.index

    logger.info("Reduce timeseries to selected timeframe.")
    edisgo_obj = extract_timeframe(edisgo_obj=edisgo_obj, timeframe=timeframe)

    logger.info("Drop former timeseries of flexible loads.")
    edisgo_obj.timeseries.loads_active_power = (
        edisgo_obj.timeseries.loads_active_power.drop(
            columns=df_loads_active_power.columns, errors="ignore"
        )
    )

    logger.info("Concatinate optimized loads.")
    edisgo_obj.timeseries.loads_active_power = pd.concat(
        [edisgo_obj.timeseries.loads_active_power, df_loads_active_power],
        axis=1,
    )

    logger.info("Set reactive power")
    edisgo_obj.set_time_series_reactive_power_control()

    return edisgo_obj


@log_errors
def integrate_dispatch(
    obj_or_path, import_path, grid_id=None, run_id=None, version_db=None
):
    """

    Parameters
    ----------
    obj_or_path : :class:`edisgo.EDisGo` or PosixPath
        Edisgo object or path to directory of edisgo dump
    import_path : PosixPath
        Path to directory where results are stored
    grid_id : int or None
        Grid id of MVGD
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
    logger.info(f"Run dispatch integration of {grid_id}")

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg_o = get_config(path=config_dir / ".opt.yaml")

    logger.info(f"Start integrate and reinforce of {grid_id} in {run_id}.")

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"dispatch_integration_{run_id}_{date}.log"
    setup_logging(file_name=logfile)

    export_path = str(import_path).split("_concat")[0] + "_mvgd"

    if isinstance(obj_or_path, EDisGo):
        edisgo_obj = obj_or_path
    else:

        logger.info(f"Import Grid from file: {obj_or_path}")
        edisgo_obj = import_edisgo_from_files(
            obj_or_path,
            import_topology=True,
            import_timeseries=True,
            import_electromobility=True,
            import_heat_pump=True,
        )

    # TODO define via config
    selected_timeseries = []
    if cfg_o["flexible_loads"]["hp"]:
        selected_timeseries += ["charging_hp_el"]
    if cfg_o["flexible_loads"]["bev"]:
        selected_timeseries += ["charging_ev"]

    logger.info(f"Selected time series: {'& '.join(selected_timeseries)}")

    edisgo_obj = integrate_timeseries(
        edisgo_obj,
        import_path,
        parameters=selected_timeseries,
        run_id=run_id,
    )

    logger.info("Save integrated grid")
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


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"result_integration_{date}_local.log"
    setup_logging(file_name=logfile)

    grid_id = 1111
    path = results_dir / "debug" / str(grid_id)
    integrate_dispatch(
        obj_or_path=path / "timesframe",
        import_path=path / "minimize_loading_concat",
        grid_id=grid_id,
        run_id=None,
        version=None,
    )
