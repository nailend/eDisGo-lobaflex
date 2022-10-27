"""A script which combines the total workflow and is only configurated by the config file"""

from emob_integration import run_emob_integration
from hp_integration import run_hp_integration
from load_integration import run_load_integration
from logger import logger

# load generation
logger.info("Start load integration")
edisgo_obj = run_load_integration(save=True)

# emob integration
logger.info("Start emob integration")
edisgo_obj, flex_bands = run_emob_integration(edisgo_obj=edisgo_obj, save=True)

# resample to 1h resolution
logger.info("Resample to 1h resolution")
edisgo_obj.resample_timeseries(method="ffill", freq="1h")

# hp integration
logger.info("Start heat pump integration")
edisgo_obj = run_hp_integration(edisgo_obj=edisgo_obj, save=True)

# bess integration

# feeder extraction

# model optimization
