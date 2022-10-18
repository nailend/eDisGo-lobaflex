""""""
import os

from pathlib import Path
from datetime import datetime
import edisgo.opf.lopf as lopf
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files
from edisgo.opf.lopf import BANDS, import_flexibility_bands
from edisgo.tools.tools import convert_impedances_to_mv
from loguru import logger

import egon_db as db

from db_data import (
    create_timeseries_for_building,
    get_cop,
    get_random_residential_buildings,
)
from model_solving import get_downstream_matrix
from tools import get_config, setup_logfile, create_dir_or_variant

from data import __path__ as data_dir
from results import __path__ as results_dir
from src import __path__ as source_dir
from logs import __path__ as logs_dir
from config import __path__ as config_dir

data_dir = data_dir[0]
results_dir = results_dir[0]
source_dir = source_dir[0]
logs_dir = logs_dir[0]
config_dir = config_dir[0]

engine = db.engine()


def determine_minimum_hp_capacity_per_building(
    peak_heat_demand, flexibility_factor=24 / 18, cop=1.7
):
    """
    Determines minimum required heat pump capacity.

    Parameters
    ----------
    peak_heat_demand : pd.Series
        Series with peak heat demand per building in MW. Index contains the
        building ID.
    flexibility_factor : float
        Factor to overdimension the heat pump to allow for some flexible
        dispatch in times of high heat demand. Per default, a factor of 24/18
        is used, to take into account

    Returns
    -------
    pd.Series
        Pandas series with minimum required heat pump capacity per building in
        MW.

    """
    return peak_heat_demand * flexibility_factor / cop


def create_heatpumps_from_db(edisgo_obj):
    """"""

    def check_nans(df):
        """Checks for any NaN values"""
        if any(df.isna().any(axis=0)):
            nan_building_ids = df.columns[df.isna().any(axis=0).values]
            raise ValueError(
                f"There are NaN-Values in the following buildings: "
                f"{nan_building_ids}"
            )

    # HP-disaggregation
    # TODO HP-disaggregation insert here

    # Workaround: assign to half of all residentials
    # Get all residentials
    residential_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df.sector == "residential"
    ]
    number_of_hps = int(residential_loads.shape[0] / 2)

    # Get random residential buildings from DB
    db_building_ids = get_random_residential_buildings(
        scenario="eGon2035", limit=number_of_hps)["building_id"].tolist()

    # Select random residential loads
    residential_loads = residential_loads.sample(number_of_hps)
    buses = residential_loads.bus

    # Create HP names and map to db building id randomly
    hp_names = [f"HP_{i}" for i in residential_loads.index]
    map_hp_to_loads = dict(zip(db_building_ids, hp_names))

    # Get cop for selected buildings
    cop_df = get_cop(db_building_ids)
    check_nans(cop_df)
    cop_df = cop_df.rename(columns=map_hp_to_loads)

    # TODO get heat_time_series for all buildings in MVGD
    #  and remove district heating buildings
    # Get heat timeseries for selected buildings
    heat_demand_df = pd.concat(
        [
            create_timeseries_for_building(building_id, scenario="eGon2035")
            for building_id in db_building_ids
        ],
        axis=1,
    )
    check_nans(heat_demand_df)
    heat_demand_df = heat_demand_df.rename(columns=map_hp_to_loads)

    heat_demand_df.max()

    # Adapt timeindex to timeseries
    year = edisgo_obj.timeseries.timeindex.year.unique()[0]
    timeindex_db = pd.date_range(
        start=f"{year}-01-01 00:00:00",
        end=f"{year}-12-31 23:45:00",
        freq="h"
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
    hp_p_set = heat_demand_df.div(cop_df).max() * 0.8

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

    edisgo_obj.topology.loads_df = pd.concat(
        [edisgo_obj.topology.loads_df, loads_df]
    )
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
    metadata = [f"This is Delhi \n", "This is Paris \n", "This is London \n"]
    grid_id = edisgo_obj.topology.mv_grid
    # Writing to file
    with open(Path(f"{path}/metadata.md"), "w") as file:
        # Writing data to a file
        file.write(f"METADATA for Grid {grid_id} \n {'*'*20} \n \n")
        file.writelines(metadata)


if __name__ == "__main__":

    cfg = get_config(Path(f"{config_dir}/model_config.yaml"))
    setup_logfile(path=logs_dir)

    cfg_m = cfg["model"]

    feeder = cfg_m["feeder"]
    grid_id = cfg_m["grid-id"]
    feeder_id = cfg_m["feeder-id"]
    # setup_directory(cfg_m)

    logger.info("Build model")
    logger.info(f"Model settings:{cfg_m}")

    # import Grid
    if feeder:
        import_dir = os.path.join(results_dir,
                                  f"{grid_id}/{feeder_id}")
    else:
        import_dir = os.path.join(results_dir,
                                  f"edisgo_objects_emob/{grid_id}")

    edisgo_obj = import_edisgo_from_files(import_dir,
                                          import_topology=True,
                                          import_timeseries=True,
                                          import_electromobility=True,
                                          )

    logger.info(f"eDisGo object imported: {grid_id}/{feeder_id}")

    # Add heatpumps fron egon-data-db
    edisgo_obj = create_heatpumps_from_db(edisgo_obj)
    logger.info("Added heat pumps to eDisGo")

    if feeder:
        export_path = os.path.join(results_dir,
                                   f"edisgo_objects_emob_hp"
                                   f"/{grid_id}"
                                   f"/{feeder_id}")
    else:
        export_path = os.path.join(results_dir,
                                   f"edisgo_objects_emob_hp"
                                   f"/{grid_id}")

    create_dir_or_variant(export_path)

    edisgo_obj.save(
        directory=export_path,
        save_topology=True,
        save_heatpump=True,
        save_results=True,
        save_timeseries=True,
        save_electromobility=True,
    )
    logger.info(f"Model saved to: {export_path}")

    # TODO write metadata
    write_metadata(export_path, edisgo_obj)

