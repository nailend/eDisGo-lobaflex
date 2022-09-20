import logging
import multiprocessing as mp
import os
import sys

from datetime import date
from pathlib import Path
from time import perf_counter

import edisgo.opf.lopf as lopf
import numpy as np
import pandas as pd
import saio
import yaml

from edisgo.edisgo import import_edisgo_from_files
from edisgo.opf.lopf import (
    BANDS,
    import_flexibility_bands,
    optimize,
    prepare_time_invariant_parameters,
    setup_model,
    update_model,
)
from edisgo.tools.tools import convert_impedances_to_mv
from loguru import logger

import egon_db as db

from db_data import (
    create_timeseries_for_building,
    get_cop,
    get_hp_thermal_loads,
    get_random_residential_buildings,
)

engine = db.engine()
saio.register_schema("demand", engine=engine)

import contextlib
import sys

from contextlib import redirect_stderr, redirect_stdout

# sys.stdout = Logger()

# class Tee(object):
#     def __init__(self, name, mode):
#         # super.__init__(
#         self.file = open(name, mode)
#         self.stdout = sys.stdout
#         sys.stdout = self
#     def __del__(self):
#         sys.stdout = self.stdout
#         self.file.close()
#     def write(self, data):
#         self.file.write(data)
#         self.stdout.write(data)
#     def flush(self):
#         self.file.flush()


class Tee(object):
    def __init__(self, logfile):
        # super.__init__(
        # self.remove = logger.remove()
        self.start = logger.add(
            sink=logfile,
            format="{time} {level} {message}",
            level="TRACE",
            backtrace=True,
            diagnose=True,
        )
        self.file = logger
        self.stdout = sys.stdout
        sys.stdout = self
        self.stderr = sys.stderr
        sys.stderr = self

    def __del__(self):
        sys.stdout = self.stdout
        # self.file.close()

    def write(self, data):
        self.file.info(data)
        self.stdout.write(data)
        self.stderr.write(data)
        stdout.write(data)

    def flush(self):
        self.file.info("Its over")


def get_config(path="./model_config.yaml"):
    """
    Returns the config.
    """
    with open(path, encoding="utf8") as f:
        return yaml.safe_load(f)


def setup_logfile(cfg):

    working_dir = Path(cfg["model"]["working-dir"])
    os.makedirs(working_dir, exist_ok=True)

    # logger.remove()
    logfile = working_dir / Path(f"{date.isoformat(date.today())}.log")
    logger.add(
        sink=logfile,
        format="{time} {level} {message}",
        level="TRACE",
        backtrace=True,
        diagnose=True,
    )
    # print = logger.info
    # sys.stdout = redirect_stdout(logger.info)
    # redirect_stderr(logger.warning)
    # detour = Tee(logfile)
    # sys.stderr = Tee
    # sys.stdout = detour
    # sys.stdout.write = logger.info

    # import subprocess, os, sys
    #
    # tee = subprocess.Popen(["tee", "log.txt"], stdin=subprocess.PIPE)
    # # Cause tee's stdin to get a copy of our stdin/stdout (as well as that
    # # of any child processes we spawn)
    # os.dup2(tee.stdin.fileno(), sys.stdout.fileno())
    # os.dup2(tee.stdin.fileno(), sys.stderr.fileno())
    #
    # # The flush flag is needed to guarantee these lines are written before
    # # the two spawned /bin/ls processes emit any output
    # print("\nstdout", flush=True)
    # print("stderr", file=sys.stderr, flush=True)
    #
    # # These child processes' stdin/stdout are
    # os.spawnve("P_WAIT", "/bin/ls", ["/bin/ls"], {})
    # os.execve("/bin/ls", ["/bin/ls"], os.environ)

    logger.info("Start")


def get_downstream_matrix(import_path, edisgo_obj):
    """Matrix that describes the influence of the nodes on each other"""
    cfg_m = get_config()["model"]
    downstream_nodes_matrix = pd.read_csv(
        os.path.join(
            import_path,
            f"downstream_node_matrix_{cfg_m['grid-id']}_{cfg_m['feeder-id']}.csv",
        ),
        index_col=0,
    )
    # TODO could be sparse?
    downstream_nodes_matrix = downstream_nodes_matrix.astype(np.uint8)
    downstream_nodes_matrix = downstream_nodes_matrix.loc[
        edisgo_obj.topology.buses_df.index, edisgo_obj.topology.buses_df.index
    ]
    return downstream_nodes_matrix


def create_heatpumps_from_db(edisgo_obj):

    cfg_m = get_config()["model"]
    # path = Path(cfg_m["feeder-dir"]) / Path(str(cfg_m["feeder-id"]))
    # TODO get from egon-data
    # TODO timeindex from where?

    # cop_df = (
    #     pd.read_csv(path / Path("COP_2011.csv")).set_index(timeindex)
    #     .resample("15min")
    #     .ffill()
    # ).rename(columns={"COP 2011": hp_name})
    #
    # df_heat_time_series = (
    #     pd.read_csv(path / Path("hp_heat_2011.csv"),
    #                 index_col=0).set_index(timeindex)
    # ).rename(columns={"0": hp_name})

    # HP-disaggregation
    # TODO insert here

    # Workaround: assign to half of all residentials
    # Get all residentials
    residential_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df.sector == "residential"
    ]
    number_of_hps = int(residential_loads.shape[0] / 2)

    residential_loads = residential_loads.sample(number_of_hps)
    hp_names = [f"HP_{i}" for i in residential_loads.index]
    buses = residential_loads.bus

    # Get random residential buildings
    building_ids = get_random_residential_buildings(
        scenario="eGon2035", limit=number_of_hps
    )["building_id"].tolist()

    # Get cop for selected buildings
    cop_df = get_cop(building_ids)
    map_cop_hp_names = dict(zip(cop_df.columns, hp_names))
    cop_df = cop_df.rename(columns=map_cop_hp_names)
    # TODO COP 1 ausprobieren worst case

    # Get heat timeseries fro selected buildings
    # TODO wait for SH run, tables dont exist yet
    # df_heat_time_series = pd.concat([create_timeseries_for_building(
    #     building_id,
    #     scenario="eGon2035") for building_id in building_ids],
    #     axis=1, keys=building_ids)

    # Workaround: generate random heat time series
    heat_demand_df = pd.DataFrame(np.random.rand(8760, number_of_hps), columns=hp_names)

    # TODO adapt timeindex
    year = edisgo_obj.timeseries.timeindex.year.unique()[0]
    freq = pd.Series(edisgo_obj.timeseries.timeindex).diff().min()
    timeindex = pd.date_range(
        start=f"{year}-01-01 00:00:00", end=f"{year}-12-31 23:45:00", freq="h"
    )

    heat_demand_df.index = timeindex
    cop_df.index = timeindex
    if not freq == timeindex.freq:
        heat_demand_df = heat_demand_df.resample(freq).ffill()
        cop_df = cop_df.resample(freq).ffill()

    heat_demand_df = heat_demand_df.loc[edisgo_obj.timeseries.timeindex]
    cop_df = cop_df.loc[edisgo_obj.timeseries.timeindex]

    logger.info(f"Heat pump time series adapted to year {year}")

    loads_df = pd.DataFrame(
        index=hp_names,
        columns=["bus", "p_set", "type"],
        data={"bus": buses, "p_set": 0.003, "type": "heat_pump"},
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

    # Not whole timeseries available
    # timesteps = edisgo_obj.timeseries.timeindex
    # year = timesteps.year.unique()[0]
    # pd.date_range(start=f"{year}-01-01 00:00:00", end=f"{year}-12-31 23:45:00", freq='h')
    edisgo_obj.heat_pump.heat_demand_df = heat_demand_df

    edisgo_obj.heat_pump.cop_df = cop_df
    edisgo_obj.heat_pump.thermal_storage_units_df = thermal_storage_units_df

    edisgo_obj.topology.loads_df = pd.concat([edisgo_obj.topology.loads_df, loads_df])
    logger.info(
        f"{sum(loads_df.p_set):.2f} MW of heat pumps for individual heating integrated."
    )

    return edisgo_obj


def setup_directory(cfg_m):
    working_dir = (
        Path(cfg_m["working-dir"]) / Path(cfg_m["grid-id"]) / Path(cfg_m["feeder-id"])
    )
    os.makedirs(working_dir, exist_ok=True)


# @logger.catch
def build_model(cfg):
    """
    Builds the model.
    """
    logger.info("Build model")
    cfg_m = cfg["model"]
    setup_directory(cfg_m)
    logger.info(f"Model settings:{cfg_m}")

    # import Grid
    import_dir = Path(cfg_m["feeder-dir"]) / Path(cfg_m["feeder-id"])
    edisgo_obj = import_edisgo_from_files(import_dir, import_timeseries=True)
    logger.info(f"eDisGo object imported: {cfg_m['grid-id']}/{cfg_m['feeder-id']}")

    # Resampling not possible as no coherent timeseries
    # edisgo_obj.timeseries.resample("h")
    # logger.info("Resampled Time series to h")

    # hp_name = edisgo_obj.add_component(
    #     "load",
    #     # bus="Bus 3",
    #     bus="BranchTee_mvgd_2534_lvgd_20547_1",
    #     type="heat_pump",
    #     p_set=0.003,
    #     sector="flexible",
    #     add_ts=False,  # dispatch will be optimized
    # )
    edisgo_obj = create_heatpumps_from_db(edisgo_obj)
    logger.info("Added heat pumps to eDisGo")

    # Due to different voltage levels, impedances need to adapted
    # TODO alternatively p.u.
    edisgo_obj = convert_impedances_to_mv(edisgo_obj)
    logger.info("Converted impedances to mv")
    downstream_nodes_matrix = get_downstream_matrix(import_dir, edisgo_obj)
    logger.info("Downstream node matrix imported")

    # Create dict with time invariant parameters
    parameters = lopf.prepare_time_invariant_parameters(
        edisgo_obj,
        downstream_nodes_matrix,
        pu=False,
        optimize_storage=False,
        optimize_ev_charging=False,
        optimize_hp=True,
    )
    logger.info("Time-invariant parameters extracted")

    return edisgo_obj, downstream_nodes_matrix, parameters


if __name__ == "__main__":

    cfg = get_config()

    setup_logfile(cfg)
    print = logger.info

    edisgo_obj, downstream_nodes_matrix, parameters = build_model(cfg)
    logger.info("Model is build")

    # cfg_o = get_config()["opt"]

    timesteps = list(range(24))
    timesteps = edisgo_obj.timeseries.timeindex[timesteps]
    model = lopf.setup_model(
        parameters,
        timesteps=timesteps,
        objective="residual_load",
    )
    logger.info("Model is setup")

    logger.info("Optimize model")
    results = lopf.optimize(model, "gurobi")
    logger.info("Model solved")
