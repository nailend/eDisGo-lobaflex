""""""
import os

from pathlib import Path

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
from tools import get_config, setup_logfile

engine = db.engine()


def create_heatpumps_from_db(edisgo_obj):

    cfg_m = get_config()["model"]

    # HP-disaggregation
    # TODO insert here

    # Workaround: assign to half of all residentials
    # Get all residentials
    residential_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df.sector == "residential"
    ]
    number_of_hps = int(residential_loads.shape[0] / 2)

    # Get random residential buildings from DB
    db_building_ids = get_random_residential_buildings(
        scenario="eGon2035", limit=number_of_hps
    )["building_id"].tolist()

    # Select random residential loads
    residential_loads = residential_loads.sample(number_of_hps)
    # Create HP names for selected residential loads
    hp_names = [f"HP_{i}" for i in residential_loads.index]
    buses = residential_loads.bus

    # Map residential loads to db building id randomly
    map_hp_to_loads = dict(zip(db_building_ids, hp_names))

    # Get cop for selected buildings
    cop_df = get_cop(db_building_ids)
    # if any nan value in ts raise error
    if any(cop_df.isna().any(axis=0)):
        nan_building_ids = cop_df.columns[cop_df.isna().any(axis=0).values]
        raise ValueError(
            f"There are NaN-Values in the following cop_df of buildings: {nan_building_ids}"
        )

    # Rename ts for residential buildings
    cop_df = cop_df.rename(columns=map_hp_to_loads)

    # Get heat timeseries for selected buildings
    # TODO get heat_time_series for all buildings in MVGD
    #  and remove district heating buildings
    heat_demand_df = pd.concat(
        [
            create_timeseries_for_building(building_id, scenario="eGon2035")
            for building_id in db_building_ids
        ],
        axis=1,
    )

    # if any nan value in ts raise error
    if any(heat_demand_df.isna().any(axis=0)):
        nan_building_ids = heat_demand_df.columns[
            heat_demand_df.isna().any(axis=0).values
        ]
        raise ValueError(
            f"There are NaN-Values in the following heat_demand_df of buildings: {nan_building_ids}"
        )

    # Rename ts for residential buildings
    heat_demand_df = heat_demand_df.rename(columns=map_hp_to_loads)

    # Workaround: generate random heat time series
    # heat_demand_df = pd.DataFrame(np.random.rand(8760, number_of_hps) /1e3, columns=hp_names)

    # TODO adapt timeindex
    year = edisgo_obj.timeseries.timeindex.year.unique()[0]

    timeindex_db = pd.date_range(
        start=f"{year}-01-01 00:00:00", end=f"{year}-12-31 23:45:00", freq="h"
    )
    heat_demand_df.index = timeindex_db
    cop_df.index = timeindex_db

    # Resample COP and demand
    freq_load = pd.Series(edisgo_obj.timeseries.timeindex).diff().min()
    if not freq_load == timeindex_db.freq:
        heat_demand_df = heat_demand_df.resample(freq_load).ffill()
        cop_df = cop_df.resample(freq_load).ffill()
        logger.info(
            f"Heat demand ts and cop ts resampled to from {timeindex_db.freq} to {freq_load}"
        )

    # Select keep loads timesteps
    heat_demand_df = heat_demand_df.loc[edisgo_obj.timeseries.timeindex]
    cop_df = cop_df.loc[edisgo_obj.timeseries.timeindex]
    logger.info(f"Heat pump time series adapted to year {year}")

    hp_p_set = heat_demand_df.div(cop_df).max() * 0.8

    loads_df = pd.DataFrame(
        index=hp_names,
        columns=["bus", "p_set", "type"],
        data={"bus": buses.values, "p_set": hp_p_set, "type": "heat_pump"},
    )
    # TODO p_set - peak heat demand
    #   script von birgit? minimum_hp_capacity

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
            pd.DataFrame.from_dict(map_hp_to_loads.items())
        ],
        keys=["residential_buildilng_id", "db_building_id", "hp_building_id"],
        axis=1).droplevel(1, axis=1)
    edisgo_obj.heat_pump.heat_demand_df = heat_demand_df
    edisgo_obj.heat_pump.cop_df = cop_df
    edisgo_obj.heat_pump.thermal_storage_units_df = thermal_storage_units_df

    edisgo_obj.topology.loads_df = pd.concat([edisgo_obj.topology.loads_df, loads_df])
    logger.info(
        f"{sum(loads_df.p_set):.2f} MW of heat pumps for individual heating integrated."
    )

    return edisgo_obj


# @logger.catch
def build_model(cfg):
    """
    Builds the model.
    """
    logger.info("Build model")
    cfg_m = cfg["model"]
    logger.info(f"Model settings:{cfg_m}")

    # import Grid
    import_dir = Path(cfg_m["feeder-dir"]) / Path(cfg_m["feeder-id"])
    edisgo_obj = import_edisgo_from_files(import_dir, import_timeseries=True)
    logger.info(f"eDisGo object imported: {cfg_m['grid-id']}/{cfg_m['feeder-id']}")

    # Resampling not possible as no coherent timeseries
    # edisgo_obj.timeseries.resample("h")
    # logger.info("Resampled Time series to h")

    # Add heatpumps fron egon-data-db
    edisgo_obj = create_heatpumps_from_db(edisgo_obj)
    logger.info("Added heat pumps to eDisGo")

    return edisgo_obj


def write_metadata(path, edisgo_obj):
    """"""

    # TODO generate metadat from edisgo_obj
    # edisgo_obj
    metadata = [f"This is Delhi \n", "This is Paris \n", "This is London \n"]

    # Writing to file
    with open(Path(f"{path}/metadata.md"), "w") as file:
        # Writing data to a file
        file.write(f"METADATA for Grid {cfg_m['grid_id']} \n {'*'*20} \n \n")
        file.writelines(metadata)


if __name__ == "__main__":

    cfg = get_config()
    setup_logfile(cfg)

    cfg_m = cfg["model"]
    # setup_directory(cfg_m)
    edisgo_obj = build_model(cfg)
    logger.info("Model is build")

    export_path = Path(cfg_m["working-dir"]) / Path(f"build/{cfg_m['grid_id']}")
    os.makedirs(export_path, exist_ok=True)

    edisgo_obj.save(
        directory=export_path,
        save_topology=True,
        save_heatpump=True,
        save_results=True,
        save_timeseries=True,
        save_electromobility=True,
    )

    write_metadata(export_path, edisgo_obj)
    # TODO write metadata

    # # Due to different voltage levels, impedances need to adapted
    # # TODO alternatively p.u.
    # edisgo_obj = convert_impedances_to_mv(edisgo_obj)
    # logger.info("Converted impedances to mv")
    #
    # import_dir = Path(cfg_m["feeder-dir"]) / Path(cfg_m["feeder-id"])
    # downstream_nodes_matrix = get_downstream_matrix(import_dir, edisgo_obj)
    # logger.info("Downstream node matrix imported")
    #
    # # Create dict with time invariant parameters
    # parameters = lopf.prepare_time_invariant_parameters(
    #     edisgo_obj,
    #     downstream_nodes_matrix,
    #     pu=False,
    #     optimize_storage=False,
    #     optimize_ev_charging=False,
    #     optimize_hp=True,
    # )
    # logger.info("Time-invariant parameters extracted")
    #
    # # cfg_o = get_config()["opt"]
    #
    # timesteps = list(range(24))
    # timesteps = edisgo_obj.timeseries.timeindex[timesteps]
    # model = lopf.setup_model(
    #     parameters,
    #     timesteps=timesteps,
    #     objective="residual_load",
    # )
    # logger.info("Model is setup")
    #
    # logger.info("Optimize model")
    # results = lopf.optimize(model, "gurobi")
    #
    # # existing_loggers = [logging.getLogger()]  # get the root logger
    # # existing_loggers = existing_loggers + [
    # #     logging.getLogger(name) for name in logging.root.manager.loggerDict
    # # ]
    # #
    # # for logger in existing_loggers:
    # #     print(logger)
    # #     print(logger.handlers)
    # #     print("-"*20)
    # logger.info("Model solved")
