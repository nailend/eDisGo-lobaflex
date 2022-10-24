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

from config import __path__ as config_dir
from data import __path__ as data_dir
from results import __path__ as results_dir
from tools import get_config, setup_logfile

# from edisgo.tools import logger


data_dir = data_dir[0]
results_dir = results_dir[0]
config_dir = config_dir[0]


# logger = logging.getLogger(__name__)


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

    cfg = get_config(Path(config_dir) / "model_config.yaml")
    cfg_m = cfg["model"]
    grid_id = cfg_m["grid-id"]
    feeder_id = cfg_m["feeder-id"]
    # setup_logfile(cfg)
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
    # import_dir = Path(results_dir) / str(cfg_m["grid-id"]) / str(cfg_m["feeder-id"])
    import_dir = os.path.join(
        results_dir, f"edisgo_objects_emob_hp" f"/{grid_id}/{feeder_id}"
    )

    # TODO add import for heatpumps
    edisgo_obj = import_edisgo_from_files(
        import_dir, import_timeseries=True, import_heat_pump=True
    )

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

    # get break between weeks
    # timesteps = pd.Series(edisgo_obj.timeseries.timeindex).diff().idxmax()
    # timesteps = edisgo_obj.timeseries.timeindex[:timesteps]
    model = lopf.setup_model(
        parameters,
        timesteps=timesteps,
        objective="residual_load",
    )
    logger.info("Model is setup")

    logger.info("Optimize model")
    results = lopf.optimize(model, "gurobi")

    # Export HP-results
    export_dir = Path(f"{cfg_m['working-dir']}/{cfg_m['grid-id']}/{cfg_m['feeder-id']}")
    for res_name, res in results.items():
        try:
            res = res.loc[timesteps]
        except Exception:
            pass
        if "slack" in res_name:
            res = res[res > 1e-6]
            res = res.dropna(how="all")
            res = res.dropna(how="all")
        if not res.empty:
            res.astype(np.float16).to_csv(
                export_dir
                / Path("results/powerflow_results")
                / Path(f"{res_name}_{cfg_m['grid-id']}_{cfg_m['feeder-id']}.csv")
            )
    print(f"Saved results to: {export_dir}/powerflow_results.")

    # TODO return results for df
    # TODO reinforce
    # export reinforce results
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

    # TODO
    # # TODO add emob ts to obj
    # edisgo_obj.set_time_series_manual(
    #         loads_p=None,
    #         storage_units_p=None,
    #         generators_q=None,
    #         loads_q=None,
    #     )
    #
    # # compute q
    # edisgo_obj.set_time_series_reactive_power_control(
    #     control="fixed_cosphi",
    #     generators_parametrisation="default",
    #     loads_parametrisation="default",
    #     storage_units_parametrisation="default",)
    #
    #
    # edisgo_obj.check_integrity()
    #
    # edisgo_obj.reinforce()
    #
    # edisgo_obj.save(directory,
    #     save_topology=True,
    #     save_timeseries=True,
    #     save_results=True,
    #     save_electromobility=True,
    #     save_heatpump=True,
    #     archive=True,
    #     archive_type="zip")
    #
    #
    # # optimize again
    # # check for curtailment?
