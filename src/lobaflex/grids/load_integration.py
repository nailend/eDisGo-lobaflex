import os
import shutil

import pandas as pd

from edisgo.edisgo import EDisGo
from edisgo.io.ding0_import import remove_1m_end_lines

from lobaflex import config_dir, data_dir
from lobaflex.tools.logger import logger
from lobaflex.tools.tools import get_config, timeit, write_metadata


@timeit
def run_load_integration(
    grid_id, edisgo_obj=False, save=False, doit=False, version=None
):

    logger.info(f"Start load integration for {grid_id}.")
    cfg = get_config(path=config_dir / ".grids.yaml")

    if not edisgo_obj:

        import_dir = cfg["load_integration"].get("import")

        ding0_grid = data_dir / import_dir / str(grid_id)
        logger.info(f"Import Grid: {grid_id} from {ding0_grid}")
        edisgo_obj = EDisGo(ding0_grid=ding0_grid)

        logger.info("Remove 1m end lines.")
        edisgo_obj = remove_1m_end_lines(edisgo_obj)

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

    from dodo import task_split_model_config_in_subconfig

    task_split_model_config_in_subconfig()

    edisgo_obj = run_load_integration(grid_id=1056)
