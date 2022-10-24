"""A script which combines the total workflow and is only configurated by the config file"""

from emob_integration import run_emob_integration
from hp_integration import run_hp_integration
from load_integration import run_load_integration

# load generation
edisgo_obj = run_load_integration(save=True)

# emob integration
edisgo_obj, flex_bands = run_emob_integration(edisgo_obj=edisgo_obj, save=True)

# hp integration
edisgo_obj = run_hp_integration(edisgo_obj=edisgo_obj, save=True)

# bess integration

# feeder extraction

# model optimization
