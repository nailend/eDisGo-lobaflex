"""A script which combines the total workflow and is only configurated by the config file"""

from pathlib import Path

from emob_integration import run_emob_integration
from hp_integration import run_hp_integration
from load_integration import run_load_integration
from feeder_extraction import run_feeder_extraction
from logger import logger
from tools import get_config, get_dir

config_dir = get_dir(key="config")

cfg = get_config(Path(f"{config_dir}/model_config.yaml"))
multi_grid_ids = cfg["model"].get("multi_grid_ids")

for mvgd in multi_grid_ids:
    # load generation
    logger.info(f"Start load integration for {mvgd}.")
    edisgo_obj = run_load_integration(grid_id=mvgd, save=True)

    # emob integration
    logger.info(f"Start emob integration for {mvgd}.")
    edisgo_obj, flex_bands = run_emob_integration(
        edisgo_obj=edisgo_obj, grid_id=mvgd, save=True, to_freq="1h"
    )

    # # resample to 1h resolution
    # logger.info("Resample to 1h resolution")
    # edisgo_obj.resample_timeseries(method="ffill", freq="1h")

    # hp integration
    logger.info(f"Start heat pump integration for {mvgd}.")
    edisgo_obj = run_hp_integration(edisgo_obj=edisgo_obj, grid_id=mvgd, save=True)

    # bess integration
    # residential mit pv_rooftop

    # feeder extraction
    run_feeder_extraction()
    # model optimization
    logger.info(f"MVGD: {mvgd} done.")
