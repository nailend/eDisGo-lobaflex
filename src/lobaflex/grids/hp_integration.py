""""""
import logging
import os
import shutil

import numpy as np
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files

from lobaflex import config_dir, data_dir
from lobaflex.grids.db_data import (
    calc_residential_heat_profiles_per_mvgd,
    determine_minimum_hp_capacity_per_building,
    get_cop,
    identify_similar_mvgd,
)
from lobaflex.tools.tools import get_config, timeit, write_metadata

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.grids." + __name__)
else:
    logger = logging.getLogger(__name__)


def get_hp_penetration():
    """Derive percentage of households with hp from NEP2035"""
    # TODO anderes überlegen
    hp_cap_2035 = 50
    number_of_residential_buildings = 540000

    return hp_cap_2035 / number_of_residential_buildings


def create_heatpumps_from_db(edisgo_obj, penetration=None):
    """"""

    def check_nans(df):
        """Checks for any NaN values"""
        if any(df.isna().any(axis=0)):
            nan_building_ids = df.columns[df.isna().any(axis=0).values]
            raise ValueError(
                f"There are NaN-Values in the following buildings: "
                f"{nan_building_ids.values}"
            )

    # HP-disaggregation
    # TODO HP-disaggregation from egon-data insert here

    # Workaround: assign to half of all residentials
    # Get all residentials
    residential_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df.sector == "residential"
    ]

    # TODO get heat_time_series for all buildings in MVGD
    #  and remove district heating buildings
    # ############### get residential heat demand profiles ###############
    mvgd = identify_similar_mvgd(residential_loads.shape[0])

    logger.info("Get heat demand time series from db.")
    heat_demand_df = calc_residential_heat_profiles_per_mvgd(
        mvgd=mvgd, scenario="eGon100RE"
    )
    #
    # pivot to allow aggregation with CTS profiles
    heat_demand_df = heat_demand_df.pivot(
        index=["day_of_year", "hour"],
        columns="building_id",
        values="demand_ts",
    )
    heat_demand_df = heat_demand_df.sort_index().reset_index(drop=True)

    if penetration is not None:
        number_of_hps = int(residential_loads.shape[0] * penetration)

    else:
        # eGon100RE all buildings with decentral heating
        number_of_hps = residential_loads.shape[0]

    percentage = number_of_hps / residential_loads.shape[0] * 100
    logger.info(f"{number_of_hps} heat pumps identified ({percentage:.2f} %).")

    # Select random residential loads
    residential_loads = residential_loads.sample(
        number_of_hps, random_state=42
    )

    # Select random heat profiles
    heat_demand_df = heat_demand_df.sample(number_of_hps, random_state=42)

    # Create HP names and map to db building id by order
    hp_names = [f"HP_{i}" for i in residential_loads.index]
    map_hp_to_loads = dict(zip(heat_demand_df.columns, hp_names))
    heat_demand_df = heat_demand_df.rename(columns=map_hp_to_loads)

    # Get cop for selected buildings
    logger.info("Get COP ts")
    cop_df = get_cop(heat_demand_df.columns)
    check_nans(cop_df)
    cop_df.rename(columns=map_hp_to_loads, inplace=True)

    # Adapt timeindex to timeseries
    year = edisgo_obj.timeseries.timeindex.year.unique()[0]
    timeindex_db = pd.date_range(
        start=f"{year}-01-01 00:00:00", end=f"{year}-12-31 23:45:00", freq="h"
    )
    heat_demand_df.index = timeindex_db
    cop_df.index = timeindex_db

    # Resample COP and demand if frequencies are not equal
    freq_load = pd.Series(edisgo_obj.timeseries.timeindex).diff().min()
    if not freq_load == timeindex_db.freq:
        heat_demand_df = heat_demand_df.resample(freq_load).ffill()
        cop_df = cop_df.resample(freq_load).ffill()
        logger.info(
            f"Heat demand ts and cop ts resampled from "
            f"{timeindex_db.freq} to {freq_load}"
        )

    # Reduce to timeindex of grid
    heat_demand_df = heat_demand_df.loc[edisgo_obj.timeseries.timeindex]
    cop_df = cop_df.loc[edisgo_obj.timeseries.timeindex]
    logger.info(f"Heat pump time series cut adapted to year {year}")

    # Minimum hp capacity is determined by peak load
    logger.info("Determine minimum hp capacity by peak load")
    hp_p_set = determine_minimum_hp_capacity_per_building(heat_demand_df.max())
    # hp_p_set = heat_demand_df.div(cop_df).max() * 0.8

    buses = residential_loads.bus
    loads_df = pd.DataFrame(
        index=hp_names,
        columns=["bus", "p_set", "type"],
        data={"bus": buses.values, "p_set": hp_p_set, "type": "heat_pump"},
    )

    cfg = get_config(path=config_dir / ".grids.yaml")
    tes_size = cfg["hp_integration"].get("tes_size", 4)
    tes_capacity = heat_demand_df.rolling(window=tes_size).sum().max()

    # ceil to next value base
    # TODO check
    def custom_round(x, base=0.001):
        return int(base * np.ceil(float(x) / base))

    tes_capacity = tes_capacity.apply(lambda x: custom_round(x, base=0.001))
    thermal_storage_units_df = pd.DataFrame(
        data={
            # "capacity": [0.05],
            "capacity": tes_capacity,
            "efficiency": [1.0],
            "state_of_charge_initial": [0.5],
        },
        index=hp_names,
    )

    edisgo_obj.heat_pump.building_ids_df = pd.concat(
        [
            pd.Series(residential_loads.index),
            pd.Series(map_hp_to_loads.keys()),
            pd.Series(map_hp_to_loads.values()),
            pd.Series(range(residential_loads.shape[0])),
        ],
        keys=[
            "residential_building_id",
            "db_building_id",
            "hp_building_id",
            "building_ids",
        ],
        axis=1,
    )

    # Add to edisgo obj
    edisgo_obj.heat_pump.heat_demand_df = heat_demand_df
    edisgo_obj.heat_pump.cop_df = cop_df
    edisgo_obj.heat_pump.thermal_storage_units_df = thermal_storage_units_df

    edisgo_obj.topology.loads_df = pd.concat(
        [edisgo_obj.topology.loads_df, loads_df]
    )
    logger.info(
        f"{sum(loads_df.p_set):.2f} MW of heat pumps for individual "
        f"heating integrated."
    )

    return edisgo_obj


@timeit
def run_hp_integration(
    grid_id, edisgo_obj=False, save=False, doit=False, version=None
):

    logger.info(f"Start heat pump integration for {grid_id}.")
    cfg = get_config(path=config_dir / ".grids.yaml")

    if not edisgo_obj:
        import_dir = cfg["hp_integration"].get("import")
        import_path = data_dir / import_dir / str(grid_id)
        logger.info(f"Import Grid from file: {import_path}")

        edisgo_obj = import_edisgo_from_files(
            import_path,
            import_topology=True,
            import_timeseries=True,
            import_electromobility=True,
        )

    # Add heatpumps fron egon-data-db
    # TODO add durchdringung
    # hp_penetration = get_hp_penetration()
    logger.info("Add heat pumps to eDisGo")
    hp_penetration = cfg["hp_integration"].get("hp_penetration")
    edisgo_obj = create_heatpumps_from_db(
        edisgo_obj, penetration=hp_penetration
    )

    logger.info("Apply heat pump operating strategy")
    edisgo_obj.apply_heat_pump_operating_strategy()

    if save:
        export_dir = cfg["hp_integration"].get("export")
        export_path = data_dir / export_dir / str(grid_id)
        shutil.rmtree(export_path, ignore_errors=True)
        os.makedirs(export_path, exist_ok=True)
        edisgo_obj.save(
            export_path,
            save_topology=True,
            save_timeseries=True,
            save_electromobility=True,
            save_heatpump=True,
            electromobility_attributes=[
                "integrated_charging_parks_df",
                "simbev_config_df",
                "flexibility_bands",
            ],
        )
        write_metadata(export_path, edisgo_obj)
        logger.info(f"Saved grid to {export_path}")

        # TODO write metadata
        # write_metadata(export_path, edisgo_obj)

    if doit:
        return {"version": version}
    else:
        return edisgo_obj


if __name__ == "__main__":

    from datetime import datetime

    from lobaflex import logs_dir
    from lobaflex.tools.logger import setup_logging
    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"hp_integration_{date}_local.log"
    setup_logging(file_name=logfile)

    edisgo_obj = run_hp_integration(grid_id=1056)
