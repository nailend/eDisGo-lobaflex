"""A script which combines the total workflow and is only configurated by the config file"""

from pathlib import Path

from dnm_generation import run_dnm_generation
from emob_integration import run_emob_integration
from feeder_extraction import run_feeder_extraction
from hp_integration import run_hp_integration
from load_integration import run_load_integration
from logger import logger
from tools import get_config, get_dir

config_dir = get_dir(key="config")

cfg = get_config(path=config_dir / "model_config.yaml")
multi_grid_ids = cfg["grid_generation"].get("grids")

for mvgd in multi_grid_ids:

    # load generation
    edisgo_obj = run_load_integration(grid_id=mvgd, targets=True)

    # emob integration
    edisgo_obj, flex_bands = run_emob_integration(
        edisgo_obj=edisgo_obj, grid_id=mvgd, targets=True, to_freq="1h"
    )

    # # resample to 1h resolution
    # logger.info("Resample to 1h resolution")
    # edisgo_obj.resample_timeseries(method="ffill", freq="1h")

    # hp integration
    edisgo_obj = run_hp_integration(edisgo_obj=edisgo_obj, grid_id=mvgd, targets=True)

    # bess integration
    # residential mit pv_rooftop

    # feeder extraction
    feeders, buses_with_feeders = run_feeder_extraction(
        edisgo_obj=edisgo_obj, grid_id=mvgd, targets=True
    )

    # generate downstream nodes matrix
    run_dnm_generation(grid_id=mvgd, targets=True)

    # model optimization
    logger.info(f"MVGD: {mvgd} done.")
