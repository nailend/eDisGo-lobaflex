import logging
import os
import warnings

from datetime import datetime
from copy import deepcopy
from edisgo.edisgo import EDisGo, import_edisgo_from_files

from lobaflex import logs_dir, results_dir
from lobaflex.opt.grid_reinforcement import iterative_reinforce
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import log_errors

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
else:
    logger = logging.getLogger(__name__)


@log_errors
def run_expansion_scenario(
    obj_or_path, grid_id=None, percentage=None, run_id=None, version_db=None
):
    """

    Parameters
    ----------
    obj_or_path : :class:`edisgo.EDisGo` or PosixPath
        edisgo object or path to edisgo dump
    grid_id : int
        grid id of MVGD
    percentage : float
        Percentage of bev and hp p_pset used for expansion.
    run_id : str
        run id used for pydoit versioning
    version_db : dict
        Dictionary with version information for pydoit versioning

    Returns
    -------

    """
    # Log to pipeline log file
    logger.info(
        f"Run expansion pathway for {percentage:.0%} scenario of {grid_id} "
    )

    warnings.simplefilter(action="ignore", category=FutureWarning)

    date = datetime.now().date().isoformat()
    logfile = (
        logs_dir
        / f"opt_{int(percentage) * 100}_pct_expansion_{run_id}_{date}.log"
    )
    setup_logging(file_name=logfile)

    logger.info(
        f"Start expansion for {percentage:.0%} scenario of {grid_id} in {run_id}."
    )

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

    export_path = (
        results_dir
        / run_id
        / str(grid_id)
        / "scenarios"
        / f"{int(percentage*100)}_pct_reinforced"
        / "mvgd"
    )
    os.makedirs(export_path, exist_ok=True)

    for util in ["cp", "hp"]:
        keys = [
            key
            for key in edisgo_obj.config["worst_case_scale_factor"].keys()
            if ("load_case" in key and f"case_{util}" in key)
        ]
        edisgo_obj.config["worst_case_scale_factor"].update(
            dict.fromkeys(keys, percentage)
        )
    # Backup original timeseries
    ts_orig = deepcopy(edisgo_obj.timeseries)

    edisgo_obj.set_time_series_worst_case_analysis("load_case")

    edisgo_obj = iterative_reinforce(
        edisgo_obj,
        timesteps=[edisgo_obj.timeseries.timeindex[0]],
        mode="iterative",
        iterations=5,
        iteration_start=0.05,
    )

    # Restore original timeseries
    edisgo_obj.timeseries = ts_orig

    logger.info(f"Save reinforced grid to {export_path}")
    edisgo_obj.save(
        export_path,
        save_topology=True,
        save_timeseries=True,
        save_heatpump=True,
        save_electromobility=True,
        electromobility_attributes=[
            "integrated_charging_parks_df",
            "simbev_config_df",
            "flexibility_bands",
        ],
        save_results=True
    )

    if version_db is not None:
        return version_db["db"]


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"minimal_reinforcement_{date}_local.log"
    setup_logging(file_name=logfile)

    grid_id = 1111
    run_expansion_scenario(
        obj_or_path=results_dir
        / "debug"
        / str(grid_id)
        / "minimize_loading_reinforced",
        grid_id=grid_id,
        percentage=0.2,
        run_id="debug",
        version_db=None,
    )
