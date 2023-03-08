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


def get_hps_mvgd(penetration, residentials_mvgd):
    """Disaggregate number of hp per grid

    Parameters
    ----------
    penetration : float/string
    residentials_mvgd : int

    Returns
    -------
    number of hps per grid

    """

    if penetration is not None:
        if isinstance(penetration, float):
            number_of_hps_mvgd = int(residentials_mvgd * penetration)
        elif penetration == "NEP2035":

            number_of_hps_germany = 7 * 1e6
            # TODO Gebäudebestand 2021 20Mio
            # number_of_residential_buildings_germany_2035 = 42 * 1e6
            number_of_residential_buildings_germany_2021 = 19375911
            penetration = (
                number_of_hps_germany
                / number_of_residential_buildings_germany_2021
            )
            number_of_hps_mvgd = int(residentials_mvgd * penetration)

        elif penetration == 1:
            number_of_hps_mvgd = residentials_mvgd
        else:
            raise ValueError

    else:
        # eGon100RE all buildings with decentral heating
        number_of_hps_mvgd = residentials_mvgd

    return number_of_hps_mvgd


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

    def custom_round(x, base=0.001):
        return base * np.ceil(float(x) / base)

    cfg_g = get_config(path=config_dir / ".grids.yaml")

    # Get all residentials
    residential_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df.sector == "residential"
    ]

    # TODO get heat_time_series for all buildings in MVGD
    #  and remove district heating buildings
    # ############### get residential heat demand profiles ###############
    # Overhead of profiles is needed as only profiles in range 8-20MWh are
    # selected.
    if penetration == "NEP2035":
        overhead_factor = 2
    elif penetration < 0.3:
        overhead_factor = 1
    elif penetration < 0.7:
        overhead_factor = 2
    else:
        overhead_factor = 3

    mvgd = identify_similar_mvgd(
        residential_loads.shape[0], overhead_factor=overhead_factor
    )

    logger.info("Get heat demand time series from db.")
    heat_demand_df = calc_residential_heat_profiles_per_mvgd(
        mvgd=mvgd, scenario=cfg_g["hp_integration"]["scenario"]
    )
    #
    heat_demand_df = heat_demand_df.pivot(
        index=["day_of_year", "hour"],
        columns="building_id",
        values="demand_ts",
    )
    heat_demand_df = heat_demand_df.sort_index().reset_index(drop=True)

    # define number of hp in the grid
    number_of_hps_mvgd = get_hps_mvgd(
        penetration=penetration, residentials_mvgd=residential_loads.shape[0]
    )

    percentage = number_of_hps_mvgd / residential_loads.shape[0] * 100
    logger.info(
        f"{number_of_hps_mvgd} heat pumps identified ({percentage:.2f} %)."
    )

    # Select random residential loads
    residential_loads = residential_loads.sample(
        number_of_hps_mvgd, axis="index", random_state=42
    )

    # Select n smallest heat profiles
    # id_profiles = heat_demand_df.sum(axis="rows").nsmallest(
    #     number_of_hps_mvgd).index
    # heat_demand_df = heat_demand_df.loc[:, id_profiles]

    logger.info("Select heat demand profiles in range 8 - 20 MWh/a.")
    heat_demand_df_sum = heat_demand_df.sum(axis="rows")
    selected_ids = heat_demand_df_sum.loc[
        (heat_demand_df_sum > 8) & (heat_demand_df_sum < 20)
    ].index
    logger.info(f"{len(selected_ids)} heat demand profiles selected.")
    heat_demand_df = heat_demand_df.loc[:, selected_ids]

    # Select n  random heat profiles
    heat_demand_df = heat_demand_df.sample(
        number_of_hps_mvgd, axis="columns", random_state=42
    )

    # Create HP names and map to db building id by order
    hp_names = [f"HP_{i}" for i in residential_loads.index]
    map_hp_to_loads = dict(zip(heat_demand_df.columns, hp_names))
    heat_demand_df = heat_demand_df.rename(columns=map_hp_to_loads)

    # Get cop for selected buildings
    logger.info("Get COP ts")
    cop_df = get_cop(map_hp_to_loads.keys())
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
    # round to next kW
    hp_stepsize = 0.001
    logger.info(
        "HP capacities are rounded to the next higher value of "
        f"base {hp_stepsize*1e3} kW."
    )
    hp_p_set = hp_p_set.apply(lambda x: custom_round(x, base=hp_stepsize))
    # hp_p_set = heat_demand_df.div(cop_df).max() * 0.8

    buses = residential_loads.bus
    loads_df = pd.DataFrame(
        index=hp_names,
        columns=["bus", "p_set", "type"],
        data={"bus": buses.values, "p_set": hp_p_set, "type": "heat_pump"},
    )

    logger.info("Determine thermal energy storage.")
    tes_size = cfg_g["hp_integration"].get("tes_size", 4)
    logger.info(
        f"Storage size will cover the heat demand of {tes_size} "
        f"highest consecutive days."
    )
    tes_capacity = heat_demand_df.rolling(window=tes_size).sum().max()

    # round to next 5 kWh
    tes_stepsize = 0.005
    logger.info(
        "TES capacities are rounded to the next higher value of "
        f"base {tes_stepsize*1e3} kWh"
    )
    tes_capacity = tes_capacity.apply(
        lambda x: custom_round(x, base=tes_stepsize)
    )
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
    cfg_g = get_config(path=config_dir / ".grids.yaml")

    if not edisgo_obj:
        import_dir = cfg_g["hp_integration"].get("import")
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
    hp_penetration = cfg_g["hp_integration"].get("hp_penetration")
    edisgo_obj = create_heatpumps_from_db(
        edisgo_obj, penetration=hp_penetration
    )

    logger.info("Apply heat pump operating strategy")
    edisgo_obj.apply_heat_pump_operating_strategy()

    if save:
        export_dir = cfg_g["hp_integration"].get("export")
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
    logfile = logs_dir / f"hp_integration_{date}_local.log"
    setup_logging(file_name=logfile)

    edisgo_obj = run_hp_integration(grid_id=1056)
