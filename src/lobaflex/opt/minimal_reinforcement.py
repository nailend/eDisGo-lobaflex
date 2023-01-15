import logging
import os
import warnings

from datetime import datetime
from functools import partial

import pandas as pd

from edisgo.edisgo import import_edisgo_from_files

from lobaflex import config_dir, data_dir, logs_dir, results_dir
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, get_files_in_subdirs
from lobaflex.grids.feeder_extraction import get_flexible_loads
from lobaflex.opt.dispatch_optimization import extract_timeframe


if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
else:
    logger = logging.getLogger(__name__)


def integrate_opt_results(edisgo_obj, parameters, run_id=None, grid_id=None):
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

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    if grid_id is None:
        grid_id = edisgo_obj.topology.mv_grid.id
    if run_id is None:
        run_id = cfg_o["run_id"]

    logger.info(f"Start minimal reinforcement of {grid_id} in {run_id}.")

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

    edisgo_obj = extract_timeframe(
        edisgo_obj=edisgo_obj,
        timeframe=df_loads_active_power.index
    )

    logger.info("Identify flexible loads.")
    df_flexible_loads = get_flexible_loads(
        edisgo_obj,
        heat_pump=True,
        electromobility=True,
        electromobility_sectors=cfg_o["emob_sectors"],
    )

    logger.info("Drop former timeseries of flexible loads.")
    edisgo_obj.timeseries.loads_active_power = (
        edisgo_obj.timeseries.loads_active_power.drop(
            columns=df_flexible_loads.index, errors="ignore"
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


def integrate_and_reinforce(edisgo_obj=None, grid_id=None, doit=False,
                            version=None):
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

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"opt_minimal_reinforcement_{cfg_o['run_id']}" \
                         f"_{date}.log"
    setup_logging(file_name=logfile)

    # TODO define via config
    selected_parameters = []
    if cfg_o["opt_hp"]:
        selected_parameters += ["charging_hp_el"]
        logger.info("Integrate hp results.")
    if cfg_o["opt_emob"]:
        selected_parameters += ["charging_ev"]
        logger.info("Integrate emob results.")

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

    edisgo_obj = integrate_opt_results(
        edisgo_obj, parameters=selected_parameters
    )

    logger.info("Start minimal reinforce")
    edisgo_obj.reinforce()

    export_path = results_dir / str(grid_id) / "min_reinforce"
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
        return {"version": version, "run_id": cfg_o["run_id"]}


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
