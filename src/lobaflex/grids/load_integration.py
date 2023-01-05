import logging
import os
import shutil
import warnings

from datetime import datetime

import pandas as pd

from edisgo.edisgo import EDisGo
from edisgo.tools.spatial_complexity_reduction import (
    remove_short_lines,
    remove_one_meter_lines,
)
from edisgo.io.ding0_import import remove_1m_end_lines

from lobaflex import config_dir, data_dir, logs_dir
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, timeit, write_metadata

logger = logging.getLogger("lobaflex.grids." + __name__)


@timeit
def run_load_integration(
    grid_id, edisgo_obj=False, save=False, doit=False, version=None
):

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg = get_config(path=config_dir / ".grids.yaml")

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"grids_{grid_id}_{date}.log"
    setup_logging(file_name=logfile)

    logger.info(f"Start load integration for {grid_id}.")

    if not edisgo_obj:

        import_dir = cfg["load_integration"].get("import")

        ding0_grid = data_dir / import_dir / str(grid_id)
        logger.info(f"Import Grid: {grid_id} from {ding0_grid}")
        edisgo_obj = EDisGo(ding0_grid=ding0_grid)

        logger.info("Remove 1m end lines.")
        edisgo_obj = remove_1m_end_lines(edisgo_obj)
        # end lines
        # edisgo_obj = remove_one_meter_lines(edisgo_obj)
        logger.info("Remove Lines < 1 Meter inside the graph.")
        # inside lines
        edisgo_obj = remove_short_lines(edisgo_obj, length=1)

    logger.info("Set worst-case analysis time series.")
    edisgo_obj.set_time_series_worst_case_analysis()
    # initial reinforce as ding0 grids not sufficient
    logger.info("Start initial reinforce with worst-case time series.")
    edisgo_obj.reinforce()

    logger.info("Reset time series.")
    edisgo_obj.timeseries.reset()

    scenario = cfg["load_integration"].get("generator_scenario")
    logger.info(f"Import generator scenario: {scenario}.")
    edisgo_obj.import_generators(generator_scenario=scenario)

    logger.info("Import timeseries for 2011.")
    timeindex = pd.date_range("1/1/2011", periods=8760, freq="H")

    edisgo_obj.set_timeindex(timeindex)
    edisgo_obj.set_time_series_active_power_predefined(
        fluctuating_generators_ts="oedb",
        dispatchable_generators_ts=pd.DataFrame(
            data=1, columns=["other"], index=timeindex
        ),
        conventional_loads_ts="demandlib",
    )
    logger.info("Set reactive power")
    edisgo_obj.set_time_series_reactive_power_control()

    if save:
        export_dir = cfg["load_integration"].get("export")
        export_path = data_dir / export_dir / str(grid_id)
        shutil.rmtree(export_path, ignore_errors=True)
        logger.info(f"Save grid to {export_path}")
        os.makedirs(export_path, exist_ok=True)
        edisgo_obj.save(
            export_path,
            save_topology=True,
            save_timeseries=True,
        )
        write_metadata(export_path, edisgo_obj)

    if doit:
        return {"version": version}
    else:
        return edisgo_obj


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig
    from datetime import datetime
    from lobaflex import logs_dir
    from lobaflex.tools.logger import setup_logging
    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"load_integration_{date}_local.log"
    setup_logging(file_name=logfile)

    edisgo_obj = run_load_integration(grid_id=2534)
