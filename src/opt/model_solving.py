""""""
import logging
import os

from copy import deepcopy
from pathlib import Path

import edisgo.opf.lopf as lopf
import numpy as np
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files
from edisgo.tools.tools import convert_impedances_to_mv
from loguru import logger

# import eDisGo_lobaflex as loba
from feeder_extraction import get_flexible_loads
from tools import get_config, get_dir

# from edisgo.tools import logger


config_dir = get_dir(key="config")
data_dir = get_dir(key="data")
results_dir = get_dir(key="results")


# logger = logging.getLogger(__name__)


def export_results(results, path):
    """
    Export results to csv. Slacks are only exported if > 1e-6

    Parameters
    ----------
    results : dict
        Results of the optimization
    path : PosixPath
        Export path for results
    """
    os.makedirs(path, exist_ok=True)
    logger.info(f"Export HP-results to: {path}")
    # timesteps = list(range(24 * 8))
    # Export HP-results
    for res_name, res in results.items():
        # try:
        #     res = res.loc[timesteps]
        # except Exception:
        #     # TODO handle this properly
        #     logger.debug(f"Exception {res_name}")
        #     pass
        if "slack" in res_name:
            res = res[res > 1e-6]  # TODO tolerance?
            res = res.dropna(how="all")
            res = res.dropna(how="all")
        if not res.empty:
            filename = path / f"{res_name}.csv"
            res.astype(np.float16).to_csv(filename, mode="w")
            # res.astype(np.float16).to_csv(filename, header=False, mode="a")


# TODO
#   rolling horizon
def run_optimization(
    grid_id, feeder_id=False, edisgo_obj=False, save=False, doit=False
):
    """

    Parameters
    ----------
    grid_id :
    feeder_id :
    edisgo_obj :
    save :
    doit :

    Returns
    -------

    """

    cfg_g = get_config(path=config_dir / ".grids.yaml")
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    # mvgds = cfg_g["model"].get("mvgd")

    if not edisgo_obj:
        if not feeder_id:
            import_dir = cfg_g["hp_integration"].get("export")
            import_path = data_dir / import_dir / str(grid_id)
            logger.info(f"Import Grid from file: {import_path}")
        else:
            feeder_id = f"{int(feeder_id):02}"
            import_dir = cfg_g["feeder_extraction"].get("export")
            import_path = (
                data_dir
                / import_dir
                / str(grid_id)
                / "feeder"
                / str(feeder_id)
            )
            logger.info(f"Import Feeder from file: {import_path}")

        edisgo_obj = import_edisgo_from_files(
            import_path,
            import_topology=True,
            import_timeseries=True,
            import_heat_pump=True,
            import_electromobility=True,
        )

    # TODO temporary workaround
    # timesteps = list(range(24 * 5))
    timesteps = list(range(24))
    timesteps = edisgo_obj.timeseries.timeindex[timesteps]

    logger.info("Convert impedances to mv")
    # Due to different voltage levels, impedances need to adapted
    # alternatively p.u.
    edisgo_obj = convert_impedances_to_mv(edisgo_obj)

    logger.info("Import downstream node matrix.")
    if not feeder_id:
        dnm_path = import_path / f"downstream_node_matrix_{grid_id}.csv"
    else:
        dnm_path = (
            import_path
            / f"downstream_node_matrix_{grid_id}_{import_path.name}.csv"
        )
    downstream_nodes_matrix = pd.read_csv(os.path.join(dnm_path), index_col=0)
    downstream_nodes_matrix = downstream_nodes_matrix.astype(np.uint8)

    logger.info("Get flexible loads")
    flexible_loads = get_flexible_loads(
        edisgo_obj=edisgo_obj,
        heat_pump=cfg_o["opt_hp"],
        electromobility=cfg_o["opt_emob"],
        bess=cfg_o["opt_bess"],
        electromobility_sectors=cfg_o["emob_sectors"],
    )

    logger.info("Extract time-invariant parameters")
    # Create dict with time invariant parameters
    parameters = lopf.prepare_time_invariant_parameters(
        edisgo_obj,
        downstream_nodes_matrix,
        pu=False,
        optimize_bess=cfg_o["opt_bess"],
        optimize_emob=cfg_o["opt_emob"],
        optimize_hp=cfg_o["opt_hp"],
        flexible_loads=flexible_loads,
    )

    if cfg_o["rolling_horizon"]:
        t_max = cfg_o["timesteps_per_iteration"]

        interval_start = list(range(0, len(timesteps), t_max))
        interval_end = list(range(t_max - 1, len(timesteps), t_max))
        if len(timesteps) % t_max > 0:
            interval_end += [len(timesteps)]

        iterations = [
            timesteps[start:end]
            for start, end in zip(interval_start, interval_end)
        ]
    else:
        iterations = [timesteps]

    energy_level = pd.DataFrame(index=timesteps)
    charging_hp = pd.DataFrame(index=timesteps)
    charging_tes = pd.DataFrame(index=timesteps)

    for i, timesteps in enumerate(iterations):

        if i == 0:
            logger.info("Setup model")
            model = lopf.setup_model(
                fixed_parameters=parameters,
                timesteps=timesteps,
                objective=cfg_o["objective"],
                # optimize_storage=cfg_o["opt_bess"],
                # optimize_ev_charging=cfg_o["opt_emob"],
                # optimize_hp=cfg_o["opt_hp"],
                name=cfg_o["objective"] + f"_{grid_id}_{feeder_id}",
                # overlap_interations=cfg_o["overlap_iterations"]
                # charging_start_hp=charging_start,
                # energy_level_start_tes=energy_level_start,
                # energy_level_end_tes=energy_level_end,
                # **kwargs,
            )
            logger.info(f"Optimize model: Iteration {i}")
            results = lopf.optimize(model=model, solver=cfg_o["solver"])
            collected_results = deepcopy(results)

        elif i > 0 & i < len(iterations):

            model = lopf.update_model(
                model,
                timesteps,
                parameters,
                # optimize_storage=cfg_o["opt_bess"],
                # optimize_ev_charging=cfg_o["opt_emob"],
                # optimize_hp=cfg_o["opt_hp"],
                # charging_start_hp=charging_start,
                # energy_level_start_tes=energy_level_start,
                # energy_level_end_tes=energy_level_end,
                # **kwargs,
            )
            logger.info(f"Optimize model: Iteration {i}")
            results = lopf.optimize(model=model, solver=cfg_o["solver"])

            # for name, df in results.items():
            #     collected_results[name] = pd.concat(
            #         [collected_results[name], results[name]], axis=1)

        else:
            logger.debug("Last iteration?")

        # charging_hp.loc[timesteps] = results["charging_hp_el"]
        # charging_tes.loc[timesteps] = results["charging_tes"]
        # energy_level.loc[timesteps] = results["energy_tes"]

    export_path = (
        results_dir
        / str(grid_id)
        / feeder_id
        / "results"
        / "powerflow_results"
    )
    export_results(results=collected_results, path=export_path)

    # list_attr = ["charging_hp_el",
    #              "charging_tes",
    #              "energy_tes"
    #              ]
    # loads_p = pd.DataFrame()
    # if cfg_o["opt_hp"]:
    #     loads_p = pd.concat([loads_p, results["charging_hp_el"]], axis=1)
    # if cfg_o["opt_ev"]:
    #     loads_p = pd.concat([loads_p, results["charging_hp_el"]], axis=1)
    # if cfg_p
    #
    # # storage_units_p = results[""]
    #
    # edisgo_obj.set_time_series_manual()
    #         loads_p=None,
    #         storage_units_p=None,
    #         generators_q=None,

    # TODO
    # # TODO add results to obj
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


if __name__ == "__main__":

    from dodo import task_split_model_config_in_subconfig

    task_split_model_config_in_subconfig()
    run_optimization(
        grid_id=1056, feeder_id=1, edisgo_obj=False, save=False, doit=False
    )
