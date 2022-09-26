""""""
import logging
import os

from pathlib import Path

import edisgo.opf.lopf as lopf
import numpy as np
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files
from edisgo.tools.tools import convert_impedances_to_mv
from loguru import logger

from tools import get_config, setup_logfile

# from edisgo.tools import logger

logger = logging.getLogger(__name__)


def setup_directory(cfg_m):
    working_dir = (
        Path(cfg_m["working-dir"]) / Path(cfg_m["grid-id"]) / Path(cfg_m["feeder-id"])
    )
    os.makedirs(working_dir, exist_ok=True)


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


if __name__ == "__main__":

    cfg = get_config()
    cfg_m = cfg["model"]

    setup_logfile(cfg)
    # print = logger.info
    # logger = logging.getLogger("pyomo")
    # logger = logging.getLogger("pyomo.contrib.appsi.solvers.gurobi")
    # logger.handlers.clear()
    # console_handler = logging.StreamHandler(stream=sys.stdout)
    # console_handler.setLevel(logging.INFO)
    # stream_formatter = logging.Formatter("%(name)s - %(levelname)s: %(message)s")
    # console_handler.setFormatter(stream_formatter)
    # logger.addHandler(console_handler)
    # logger.setLevel(logging.INFO)
    # logger.propagate = False

    # import Grid
    import_dir = Path(f"{cfg_m['working-dir']}/{cfg_m['grid-id']}/{cfg_m['feeder-id']}")
    # TODO add import for heatpumps
    edisgo_obj = import_edisgo_from_files(import_dir,
                                          import_timeseries=True,
                                          import_heat_pump=True)

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

    # existing_loggers = [logging.getLogger()]  # get the root logger
    # existing_loggers = existing_loggers + [
    #     logging.getLogger(name) for name in logging.root.manager.loggerDict
    # ]
    #
    # for logger in existing_loggers:
    #     print(logger)
    #     print(logger.handlers)
    #     print("-"*20)
    logger.info("Model solved")
