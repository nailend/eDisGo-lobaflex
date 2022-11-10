import os

import pandas as pd

from edisgo.edisgo import EDisGo
from edisgo.io.ding0_import import remove_1m_end_lines

from logger import logger
from tools import get_config, get_dir, timeit, write_metadata

logs_dir = get_dir(key="logs")
data_dir = get_dir(key="data")
config_dir = get_dir(key="config")


@timeit
def run_load_integration(grid_id, edisgo_obj=False, save=False, doit=False):

    logger.info(f"Start load integration for {grid_id}.")
    cfg = get_config(path=config_dir / ".grids.yaml")

    if not edisgo_obj:

        import_dir = cfg["load_integration"].get("import")

        ding0_grid = data_dir / import_dir / str(grid_id)
        logger.info(f"Import Grid: {grid_id} from {ding0_grid}")
        edisgo_obj = EDisGo(ding0_grid=ding0_grid)

        logger.info("Remove 1m end lines")
        edisgo_obj = remove_1m_end_lines(edisgo_obj)

    # set up time series
    logger.info("Import timeseries.")
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

        logger.info(f"Save grid to {export_path}")
        os.makedirs(export_path, exist_ok=True)
        edisgo_obj.save(
            export_path,
            save_topology=True,
            save_timeseries=True,
        )
        write_metadata(export_path, edisgo_obj)

    if doit:
        return True
    else:
        return edisgo_obj


if __name__ == "__main__":

    edisgo_obj = run_load_integration(grid_id=176)
