""""""
import os

# from datetime import datetime
from pathlib import Path

# import edisgo.opf.lopf as lopf
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files

import egon_db as db

from db_data import (  # create_timeseries_for_building,; get_random_residential_buildings,
    calc_residential_heat_profiles_per_mvgd,
    determine_minimum_hp_capacity_per_building,
    get_cop,
    identify_similar_mvgd,
)

# from edisgo.opf.lopf import BANDS, import_flexibility_bands
# from edisgo.tools.tools import convert_impedances_to_mv
# from loguru import logger
from logger import logger

# from src import __path__ as source_dir
from tools import get_config, timeit

# from model_solving import get_downstream_matrix
# from results import __path__ as results_dir
# from logs import __path__ as logs_dir
# from config import __path__ as config_dir
# from data import __path__ as data_dir


# data_dir = Path(data_dir[0])
# results_dir = Path(results_dir[0])
# config_dir = Path(config_dir[0])
# logs_dir = Path(logs_dir[0])

data_dir = Path("/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/data")
logs_dir = Path("/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/logs")
config_dir = Path(
    "/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/config"
)


engine = db.engine()


def get_hp_penetration():
    """Derive percentage of households with hp from NEP2035"""
    # TODO anderes überlegen
    hp_cap_2035 = 50
    number_of_residential_buildings = 540000

    return hp_cap_2035 / number_of_residential_buildings


def create_heatpumps_from_db(edisgo_obj):
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
    # number_of_hps = int(residential_loads.shape[0] / 2)
    # eGon100RE all buildings with decentral heating
    # number_of_hps = residential_loads.shape[0]

    # # Get random residential buildings from DB
    # db_building_ids = get_random_residential_buildings(
    #     scenario="eGon100RE", limit=number_of_hps
    # )["building_id"].tolist()

    # # TODO blöder workaround
    # db_building_ids = [i for i in db_building_ids if i not in [6778, 6780]]
    # Select random residential loads
    # residential_loads = residential_loads.sample(number_of_hps)

    # TODO get heat_time_series for all buildings in MVGD
    #  and remove district heating buildings
    # ############### get residential heat demand profiles ###############
    mvgd = identify_similar_mvgd(residential_loads.shape[0])

    df_heat_ts = calc_residential_heat_profiles_per_mvgd(
        mvgd=mvgd, scenario="eGon100RE"
    )
    #
    # pivot to allow aggregation with CTS profiles
    df_heat_ts = df_heat_ts.pivot(
        index=["day_of_year", "hour"],
        columns="building_id",
        values="demand_ts",
    )
    df_heat_ts = df_heat_ts.sort_index().reset_index(drop=True)
    df_heat_ts = df_heat_ts.iloc[:, : residential_loads.shape[0]]
    db_building_ids = df_heat_ts.columns

    # Create HP names and map to db building id randomly
    hp_names = [f"HP_{i}" for i in residential_loads.index]
    map_hp_to_loads = dict(zip(db_building_ids, hp_names))

    heat_demand_df = df_heat_ts.rename(columns=map_hp_to_loads)

    # Get cop for selected buildings
    cop_df = get_cop(db_building_ids)
    check_nans(cop_df)
    cop_df = cop_df.rename(columns=map_hp_to_loads)

    # # Get heat timeseries for selected buildings
    # heat_demand_df = pd.concat(
    #     [
    #         create_timeseries_for_building(building_id, scenario="eGon2035")
    #         for building_id in db_building_ids
    #     ],
    #     axis=1,
    # )
    # check_nans(heat_demand_df)
    # heat_demand_df = heat_demand_df.rename(columns=map_hp_to_loads)

    # heat_demand_df.max()

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
            f"Heat demand ts and cop ts resampled to from "
            f"{timeindex_db.freq} to {freq_load}"
        )

    # Only keep loads timesteps
    heat_demand_df = heat_demand_df.loc[edisgo_obj.timeseries.timeindex]
    cop_df = cop_df.loc[edisgo_obj.timeseries.timeindex]
    logger.info(f"Heat pump time series cut adapted to year {year}")

    # TODO set p_set? minimal capacity?
    peak_load = heat_demand_df.max()
    hp_p_set = determine_minimum_hp_capacity_per_building(peak_load)
    # hp_p_set = heat_demand_df.div(cop_df).max() * 0.8

    buses = residential_loads.bus
    loads_df = pd.DataFrame(
        index=hp_names,
        columns=["bus", "p_set", "type"],
        data={"bus": buses.values, "p_set": hp_p_set, "type": "heat_pump"},
    )

    thermal_storage_units_df = pd.DataFrame(
        data={
            "capacity": [0.05],
            "efficiency": [1.0],
            "state_of_charge_initial": [0.5],
        },
        index=hp_names,
    )

    edisgo_obj.heat_pump.building_ids_df = pd.concat(
        [
            pd.Series(residential_loads.index),
            pd.DataFrame.from_dict(map_hp_to_loads.items()),
        ],
        keys=["residential_buildilng_id", "db_building_id", "hp_building_id"],
        axis=1,
    ).droplevel(1, axis=1)

    # Add to edisgo obj
    edisgo_obj.heat_pump.heat_demand_df = heat_demand_df
    edisgo_obj.heat_pump.cop_df = cop_df
    edisgo_obj.heat_pump.thermal_storage_units_df = thermal_storage_units_df

    edisgo_obj.topology.loads_df = pd.concat([edisgo_obj.topology.loads_df, loads_df])
    logger.info(
        f"{sum(loads_df.p_set):.2f} MW of heat pumps for individual "
        f"heating integrated."
    )

    return edisgo_obj


def write_metadata(path, edisgo_obj):
    """"""

    # TODO generate metadat from edisgo_obj
    # copy from  check_integrity
    # edisgo_obj
    metadata = ["This is Delhi \n", "This is Paris \n", "This is London \n"]
    grid_id = edisgo_obj.topology.mv_grid
    # Writing to file
    with open(Path(f"{path}/metadata.md"), "w") as file:
        # Writing data to a file
        file.write(f"METADATA for Grid {grid_id} \n {'*'*20} \n \n")
        file.writelines(metadata)


@timeit
def run_hp_integration(edisgo_obj=False, save=False):

    # TODO use SH1 59776

    cfg = get_config(Path(f"{config_dir}/model_config.yaml"))
    if not edisgo_obj:

        grid_id = cfg["model"].get("grid-id")
        import_dir = cfg["directories"]["hp_integration"].get("import")
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
    edisgo_obj = create_heatpumps_from_db(edisgo_obj)
    logger.info("Added heat pumps to eDisGo")

    if save:
        export_dir = cfg["directories"]["hp_integration"].get("export")
        export_path = data_dir / export_dir / str(grid_id)
        os.makedirs(export_path, exist_ok=True)
        edisgo_obj.save(
            export_path,
            save_topology=True,
            save_timeseries=True,
            save_electromobility=True,
            save_heatpump=True,
        )
        logger.info(f"Saved grid to {export_path}")

        # TODO write metadata
        write_metadata(export_path, edisgo_obj)

    return edisgo_obj


if __name__ == "__main__":

    edisgo_obj = run_hp_integration()
