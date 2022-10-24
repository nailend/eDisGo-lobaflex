import os

from pathlib import Path

import pandas as pd

from edisgo.edisgo import EDisGo, import_edisgo_from_files
from loguru import logger


from tools import get_config, timeit

from data import __path__ as data_dir
from results import __path__ as results_dir
from logs import __path__ as logs_dir
from config import __path__ as config_dir
# from src.tools import setup_logger


data_dir = data_dir[0]
results_dir = results_dir[0]
config_dir = config_dir[0]
logs_dir = logs_dir[0]

def run_emob_integration():

    cfg = get_config(Path(f"{config_dir}/model_config.yaml"))
    cfg_m = cfg["model"]

    grid_id = cfg_m["grid-id"]

    ding0_grid = os.path.join(data_dir, f"elia_ding0_grids/{grid_id}")
    edisgo = EDisGo(ding0_grid=ding0_grid)
    logger.info("ding0 grid imported")

    # set up time series
    timeindex = pd.date_range("1/1/2011", periods=24 * 7, freq="H")
    edisgo.set_timeindex(timeindex)
    edisgo.set_time_series_active_power_predefined(
        fluctuating_generators_ts="oedb",
        dispatchable_generators_ts=pd.DataFrame(data=1, columns=["other"], index=timeindex),
        conventional_loads_ts="demandlib",
    )
    logger.info("oedb timeseries imported")
    edisgo.set_time_series_reactive_power_control()

    # resample time series to have a temporal resolution of 15 minutes, which is the same
    # as the electromobility time series
    edisgo.resample_timeseries()
    logger.info("timeseries resampled")

    edisgo.import_electromobility(
        simbev_directory=os.path.join(data_dir, f"simbev_results/{grid_id}"),
        tracbev_directory=os.path.join(data_dir, f"tracbev_results/{grid_id}"),
    )
    logger.info("emobility imported")

    flex_bands = edisgo.electromobility.get_flexibility_bands(edisgo, ["home", "work"])

    export_dir = os.path.join(results_dir, f"edisgo_objects_emob/{grid_id}")
    os.makedirs(export_dir, exist_ok=True)

    for name, df in flex_bands.items():
        df.to_csv(f"{export_dir}/{name}_flexibility_band.csv")

    logger.info(f"Flexibility bands exported to: {export_dir}")


    edisgo.save(
        export_dir,
        save_topology=True,
        save_results=False,
        save_timeseries=True,
        save_electromobility=True,
        save_heatpumps=False,
    )
    logger.info(f"Edisgo object exported to: {export_dir}")

# edisgo = import_edisgo_from_files(export_dir,
#                                       import_timeseries=True,
#                                       import_heat_pump=False,
#                                       import_topology=True,
#                                       import_electromobility=True)


if __name__ == "__main__":

    run_emob_integration()