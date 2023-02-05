import logging
import warnings

from datetime import datetime

from edisgo.edisgo import EDisGo, import_edisgo_from_files

from lobaflex import config_dir, logs_dir, results_dir
from lobaflex.opt.dispatch_optimization import extract_timeframe
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, log_errors

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
    logger.propagate = False
else:
    logger = logging.getLogger(__name__)
    logger.propagate = False


@log_errors
def run_timeframe_selection(
    obj_or_path,
    grid_id,
    run_id=None,
    version=None,
):
    """Extract timeframe from edisgo object or dump. Currently using timeindex
    generate from config file input.

    Parameters
    ----------
    obj_or_path : :class:`edisgo.EDisGo` or PosixPath
        edisgo object or path to edisgo dump
    grid_id :
        grid id of MVGD
    run_id :
        run id used for pydoit versioning
    version :
        version number of run id used for pydoit versioning


    Returns
    -------

    """

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg_o = get_config(path=config_dir / ".opt.yaml")

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"opt_{run_id}_{grid_id}_{date}.log"
    setup_logging(file_name=logfile)

    logger.info(
        f"Run timeframe selection for grid: {grid_id} with run id: {run_id}"
    )
    export_path = results_dir / run_id / str(grid_id) / "timeframe"

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
        timesteps=cfg_o["total_timesteps"],
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

    if version is not None and run_id is not None:
        return {"version": version, "run_id": run_id}
