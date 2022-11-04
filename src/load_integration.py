import os

from pathlib import Path

import pandas as pd

from edisgo.edisgo import EDisGo

from logger import logger
from tools import get_config, get_dir, timeit

logs_dir = get_dir(key="logs")
data_dir = get_dir(key="data")
config_dir = get_dir(key="config")


@timeit
def run_load_integration(grid_id, edisgo_obj=False, targets=False,
                         doit=False):

    logger.info(f"Start load integration for {grid_id}.")
    cfg = get_config(path=config_dir / "model_config.yaml")

    if not edisgo_obj:

        import_dir = cfg["grid_generation"]["load_integration"].get("import")

        ding0_grid = data_dir / import_dir / str(grid_id)
        edisgo_obj = EDisGo(ding0_grid=ding0_grid)

    # set up time series
    timeindex = pd.date_range("1/1/2011", periods=8760, freq="H")
    edisgo_obj.set_timeindex(timeindex)
    edisgo_obj.set_time_series_active_power_predefined(
        fluctuating_generators_ts="oedb",
        dispatchable_generators_ts=pd.DataFrame(
            data=1, columns=["other"], index=timeindex
        ),
        conventional_loads_ts="demandlib",
    )
    edisgo_obj.set_time_series_reactive_power_control()

    if targets:
        if isinstance(targets, Path):
            logger.debug("Use export dir given as parameter.")
            export_path = targets
        elif isinstance(targets, str):
            logger.debug("Use export dir given as parameter.")
            export_path = Path(targets)
        else:
            logger.debug("Use export dir from config file.")
            export_dir = cfg["grid_generation"]["load_integration"].get("export")
            export_path = data_dir / export_dir / str(grid_id)

        os.makedirs(export_path, exist_ok=True)
        edisgo_obj.save(
            export_path,
            save_topology=True,
            save_timeseries=True,
        )
        logger.info(f"Saved grid to {export_path}")

    if doit:
        return True
    else:
        return edisgo_obj


if __name__ == "__main__":

    edisgo_obj = run_load_integration(grid_id=176)
