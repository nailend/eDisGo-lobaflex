import logging
import os
import re
import warnings

from copy import deepcopy
from datetime import datetime
from functools import partial

import pandas as pd

from edisgo.edisgo import import_edisgo_from_files

from lobaflex import config_dir, data_dir, logs_dir, results_dir
from lobaflex.grids.feeder_extraction import get_flexible_loads
from lobaflex.opt.dispatch_optimization import extract_timeframe
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, get_files_in_subdirs, log_errors

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
else:
    logger = logging.getLogger(__name__)


def integrate_opt_results(
    edisgo_obj,
    parameters,
    start_datetime=None,
    periods=None,
    run_id=None,
    grid_id=None,
):
    """

    Parameters
    ----------
    edisgo_obj :
    parameters :
    run_id :
    grid_id :

    Returns
    -------

    """
    edisgo_obj = deepcopy(edisgo_obj)

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    if grid_id is None:
        grid_id = edisgo_obj.topology.mv_grid.id
    if run_id is None:
        run_id = cfg_o["run_id"]

    logger.info(f"Start result integration of {grid_id} in {run_id}.")

    results_path = results_dir / run_id / str(grid_id) / "mvgd"

    list_of_files = get_files_in_subdirs(results_path, pattern="*.csv")

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


@log_errors()
def integrate_and_reinforce(
    edisgo_obj=None, grid_id=None, doit=False, version=None
):
    """

    Parameters
    ----------
    edisgo_obj :
    grid_id :
    doit :
    version :

    Returns
    -------

    """
    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    cfg_g = get_config(path=config_dir / ".grids.yaml")
    run_id = cfg_o["run_id"]

    logger.info(f"Start integrate and reinforce of {grid_id} in {run_id}.")

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"opt_minimal_reinforcement_{run_id}_{date}.log"
    setup_logging(file_name=logfile)

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"reinforcement_{grid_id}_{date}.log"
    setup_logging(file_name=logfile)

    if edisgo_obj and grid_id is None:
        raise ValueError("Either edisgo_object or grid_id has to be given.")
    if edisgo_obj is None:
        import_dir = cfg_g["feeder_extraction"].get("import")
        import_path = data_dir / import_dir / str(grid_id)
        logger.info(f"Import Grid from file: {import_path}")

        edisgo_obj = import_edisgo_from_files(
            import_path,
            import_topology=True,
            import_timeseries=True,
            import_electromobility=True,
            import_heat_pump=True,
        )

    # TODO define via config
    selected_parameters = []
    if cfg_o["opt_hp"]:
        selected_parameters += ["charging_hp_el"]
    if cfg_o["opt_emob"]:
        selected_parameters += ["charging_ev"]

    logger.info(f"Selected parameters: {'& '.join(selected_parameters)}")
    edisgo_obj = integrate_opt_results(
        edisgo_obj, parameters=selected_parameters
    )

    logger.info("Start minimal reinforce")
    try:
        edisgo_obj.reinforce()
    except ValueError as e:
        exluded_timesteps = (
            re.findall(
                pattern=r"DatetimeIndex\(\[(.*)\], dtype", string=str(e)
            )[0]
            .replace("'", "")
            .split(",")
        )
        exluded_timesteps = pd.to_datetime(exluded_timesteps)

        logger.warning(
            "Powerflow didn't converge for time steps: "
            f"{exluded_timesteps}."
        )
        reduced_timesteps = edisgo_obj.timeseries.timeindex.drop(
            exluded_timesteps
        )
        logger.info("Start partial reinforce with reduced time steps.")
        edisgo_obj.reinforce(reduced_timesteps)
        logger.info("Continue partial reinforce with excluded time steps.")
        edisgo_obj.reinforce(exluded_timesteps)

    export_path = results_dir / run_id / str(grid_id) / "min_reinforce"
    os.makedirs(export_path, exist_ok=True)

    logger.info("Save reinforced grid")
    edisgo_obj.save(
        export_path,
        save_topology=True,
        save_timeseries=True,
        save_heatpump=True,
        save_electromobility=True,
        save_results=True,
    )

    if doit:
        return {"version": version, "run_id": run_id}


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"minimal_reinforcement_{date}_local.log"
    setup_logging(file_name=logfile)

    integrate_and_reinforce(
        # grid_id=1056, feeder_id=1, edisgo_obj=False, save=True, doit=False
        # grid_id=2534,
        # grid_id=1056,
        grid_id=1111,
        # feeder_id=8,
        doit=False,
    )

    # lopf.combine_results_for_grid
