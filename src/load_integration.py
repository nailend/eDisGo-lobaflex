import os

from pathlib import Path

import pandas as pd

from edisgo.edisgo import EDisGo, import_edisgo_from_files
from edisgo.network.electromobility import get_energy_bands_for_optimization
from edisgo.tools.logger import setup_logger

import_path = Path("./")
grid_id = 2534


edisgo_obj = import_edisgo_from_files(import_path, import_timeseries=False)

# set up time series
timeindex = pd.date_range("1/1/2011", periods=24 * 7, freq="H")
edisgo_obj.set_timeindex(timeindex)
edisgo_obj.set_time_series_active_power_predefined(
    fluctuating_generators_ts="oedb",
    dispatchable_generators_ts=pd.DataFrame(data=1, columns=["other"], index=timeindex),
    conventional_loads_ts="demandlib",
)
edisgo_obj.set_time_series_reactive_power_control()

# resample time series to have a temporal resolution of 15 minutes, which is the same
# as the electromobility time series
edisgo_obj.resample_timeseries()

# TODO get simbev + tracbev from kilian

data_dir = os.path.join(os.path.dirname(os.getcwd()), "tests", "data")
edisgo_obj.import_electromobility(
    simbev_directory=os.path.join(data_dir, "simbev_example_scenario_2"),
    tracbev_directory=os.path.join(data_dir, "tracbev_example_scenario_2"),
)

for use_case in ["home", "work"]:
    power, lower, upper = get_energy_bands_for_optimization(edisgo_obj, use_case)

    power.to_csv(data_dir + r"\{}\upper_power_{}.csv".format(grid_id, use_case))
    lower.to_csv(data_dir + r"\{}\lower_energy_{}.csv".format(grid_id, use_case))
    upper.to_csv(data_dir + r"\{}\upper_energy_{}.csv".format(grid_id, use_case))
    print("Successfully created bands for {}-{}".format(grid_id, use_case))
