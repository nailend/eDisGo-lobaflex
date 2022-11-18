import logging
import os

from datetime import datetime
from pathlib import Path
from time import perf_counter

import edisgo.opf.lopf as lopf
import numpy as np
import pandas as pd

from DEV_optimisation import load_values_from_previous_failed_run
from edisgo.edisgo import import_edisgo_from_files

# from edisgo.opf.lopf import (
#     optimize,
#     prepare_time_invariant_parameters,
#     setup_model,
#     update_model,
# )
from edisgo.tools.tools import convert_impedances_to_mv
from loguru import logger

# import eDisGo_lobaflex as loba
from feeder_extraction import get_flexible_loads
from tools import dump_yaml, get_config, get_dir

config_dir = get_dir(key="config")
data_dir = get_dir(key="data")
results_dir = get_dir(key="results")


def get_dnm(mvgd, feeder):
    """
    Get Downstream Node Matrix that describes the influence of the nodes on
    each other

    Parameters
    ----------
    mvgd : int
        MVGD id
    feeder : int
        Feeder id

    Returns
    -------
    pd.DataFrame
        Downstream node matrix
    """
    cfd_g = get_config(path=config_dir / ".grids.yaml")
    feeder_dir = cfd_g["dnm_generation"].get("import")
    feeder = f"{int(feeder):02}"

    import_path = data_dir / feeder_dir / str(mvgd) / "feeder" / feeder
    # TODO dirty quick fix
    # file_path = import_path / f"downstream_node_matrix_{mvgd}" \
    #                           f"_{feeder}.csv"
    file_path = import_path / f"downstream_node_matrix_{mvgd}_{feeder}.csv"
    downstream_nodes_matrix = pd.read_csv(os.path.join(file_path), index_col=0)
    downstream_nodes_matrix = downstream_nodes_matrix.astype(np.uint8)
    # TODO warum hier loc? vermutlich weil anja nur eine downstream node
    #  matrix für alle hatte
    # downstream_nodes_matrix = downstream_nodes_matrix.loc[
    #     edisgo_obj.topology.buses_df.index, edisgo_obj.topology.buses_df.index
    # ]
    return downstream_nodes_matrix


def rolling_horizon_optimization(
    edisgo_obj,
    grid_id,
    feeder_id,
    run=datetime.now().isoformat(),
    load_results=False,
    iteration=0,
    save=False,
):

    # cfg_g = get_config(path=config_dir / ".grids.yaml")
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    # mvgds = cfg_g["model"].get("mvgd")
    feeder_id = f"{int(feeder_id):02}"

    # objective = "minimize_loading"
    # timesteps_per_iteration = 24 * 4
    # iterations_per_era = 7
    # overlap_iterations = 24
    # solver = "gurobi"

    # kwargs = {}  # {'v_min':0.91, 'v_max':1.09, 'thermal_limit':0.9}
    # config_dict = {
    #     "objective": objective,
    #     "solver": solver,
    #     "timesteps_per_iteration": timesteps_per_iteration,
    #     "iterations_per_era": iterations_per_era,
    #     "overlap_iterations": overlap_iterations,
    # }

    # import_dir = cfg_g["feeder_extraction"].get("export")
    # import_path = data_dir / import_dir / str(grid_id) / "feeder" / str(
    #     feeder_id)
    result_path = results_dir / run / str(grid_id) / feeder_id
    os.makedirs(result_path, exist_ok=True)

    # Check existing results and load values
    if (len(os.listdir(result_path)) > 239) and load_results:
        logging.info(
            "Feeder {} of grid {} already solved.".format(feeder_id, grid_id)
        )
        return
    elif (len(os.listdir(result_path)) > 1) and load_results:
        # iterations_finished = int((len(os.listdir(result_path)) - 1) / 17)
        logging.info("Reload former results")
        (
            charging_start,
            energy_level_start,
            start_iter,
        ) = load_values_from_previous_failed_run(
            feeder_id=feeder_id,
            grid_id=grid_id,
            iteration=iteration,
            iterations_per_era=cfg_o["iterations_per_era"],
            overlap_interations=cfg_o["overlap_iterations"],
            result_dir=result_path,
        )

    else:
        logger.info("Start from scratch.")
        charging_start = None
        energy_level_start = None
        start_iter = 0

    # Dump opt configs to results
    dump_yaml(yaml_file=cfg_o, save_to=result_path)

    # # try:
    # logger.info(f"Import Grid from file: {import_path}")
    # edisgo_obj = import_edisgo_from_files(import_path,
    #                                        import_topology=True,
    #                                        import_timeseries=True,
    #                                        import_electromobility=True,
    #                                        import_heat_pump=True,
    #                                        )

    logger.info("Convert impedances to mv")
    # Due to different voltage levels, impedances need to adapted
    # alternatively p.u.
    edisgo_obj = convert_impedances_to_mv(edisgo_obj)

    logger.info("Downstream node matrix imported")
    downstream_nodes_matrix = get_dnm(mvgd=grid_id, feeder=int(feeder_id))

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
    fixed_parameters = lopf.prepare_time_invariant_parameters(
        edisgo_obj=edisgo_obj,
        downstream_nodes_matrix=downstream_nodes_matrix,
        per_unit=False,
        optimize_bess=cfg_o["opt_bess"],
        optimize_emob=cfg_o["opt_emob"],
        optimize_hp=cfg_o["opt_hp"],
        flexible_loads=flexible_loads,
    )

    energy_level = {}
    charging_hp = {}
    charging_tes = {}
    kwargs = {}

    # TODO adhoc workaround
    interval = edisgo_obj.timeseries.timeindex[:50]

    for iteration in range(
        start_iter, int(len(interval) / cfg_o["timesteps_per_iteration"])
    ):  # edisgo_obj.timeseries.timeindex.week.unique()

        logging.info(f"Starting optimisation for iteration {iteration}.")

        # if last iteration of era
        if (
            iteration % cfg_o["iterations_per_era"]
            != cfg_o["iterations_per_era"] - 1
        ):
            timesteps = edisgo_obj.timeseries.timeindex[
                iteration
                * cfg_o["timesteps_per_iteration"] : (iteration + 1)
                * cfg_o["timesteps_per_iteration"]
                + cfg_o["overlap_iterations"]
            ]
            energy_level_end = None
        else:
            timesteps = edisgo_obj.timeseries.timeindex[
                iteration
                * cfg_o["timesteps_per_iteration"] : (iteration + 1)
                * cfg_o["timesteps_per_iteration"]
            ]
            energy_level_end = True
        try:
            model = lopf.update_model(
                model=model,
                timesteps=timesteps,
                fixed_parameters=fixed_parameters,
                objective=cfg_o["objective"],
                optimize_bess=cfg_o["opt_bess"],
                optimize_emob=cfg_o["opt_emob"],
                optimize_hp=cfg_o["opt_hp"],
                charging_start_hp=charging_start,
                energy_level_start_tes=energy_level_start,
                energy_level_end_tes=energy_level_end,
                **kwargs,
            )
        except NameError:
            model = lopf.setup_model(
                fixed_parameters=fixed_parameters,
                timesteps=timesteps,
                objective=cfg_o["objective"],
                optimize_bess=cfg_o["opt_bess"],
                optimize_emob=cfg_o["opt_emob"],
                optimize_hp=cfg_o["opt_hp"],
                charging_start_hp=charging_start,
                energy_level_start_tes=energy_level_start,
                energy_level_end_tes=energy_level_end,
                **kwargs,
            )

        logger.info(f"Set up model for week {iteration}.")
        lp_filename = result_path / f"lp_file_iteration_{iteration}.lp"
        result_dict = lopf.optimize(
            model, cfg_o["solver"], lp_filename=lp_filename
        )
        charging_hp[iteration] = result_dict["charging_hp_el"]
        charging_tes[iteration] = result_dict["charging_tes"]
        energy_level[iteration] = result_dict["energy_tes"]

        if (
            iteration % cfg_o["iterations_per_era"]
            != cfg_o["iterations_per_era"] - 1
        ):
            charging_start = {
                "hp": charging_hp[iteration].iloc[
                    -cfg_o["overlap_iterations"]
                ],
                "tes": charging_tes[iteration].iloc[
                    -cfg_o["overlap_iterations"]
                ],
            }
            energy_level_start = energy_level[iteration].iloc[
                -cfg_o["overlap_iterations"]
            ]
        else:
            charging_start = None
            energy_level_start = None

        logger.info(f"Finished optimisation for week {iteration}.")

        if save:
            for res_name, res in result_dict.items():
                try:
                    res = res.loc[edisgo_obj.timeseries.timeindex]
                except Exception:
                    pass
                if "slack" in res_name:
                    res = res[res > 1e-6]
                    res = res.dropna(how="all")
                    res = res.dropna(how="all")
                if not res.empty:
                    filename = (
                        result_path / f"{res_name}_{grid_id}_{feeder_id}"
                        f"_{iteration}.csv"
                    )
                    res.astype(np.float16).to_csv(filename)
            logger.info(f"Saved results for week {iteration}.")

    # except Exception as e:
    #     print('Something went wrong with feeder {} of grid {}'.format(
    #         feeder_id, grid_id))
    #     print(e)
    #     if 'iteration' in locals():
    #         if iteration >= 1:
    #             charging_start = charging_hp[iteration-1].iloc[-overlap_iterations]
    #             charging_start.to_csv(
    #                 result_path +'/charging_start_{}_{}_{}.csv'.format(
    #                 grid_id, feeder_id, iteration))
    #             energy_level_start = energy_level[iteration-1].iloc[
    #                 -overlap_iterations]
    #             energy_level_start.to_csv(
    #                 result_path +'/energy_level_start_{}_{}_{}.csv'.format(
    #                     grid_id, feeder_id, iteration))


def run_dispatch_optimization(
    grid_id, feeder_id=False, edisgo_obj=False, save=False, doit=False
):

    cfg_g = get_config(path=config_dir / ".grids.yaml")
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    # mvgds = cfg_g["model"].get("mvgd")
    feeder_id = f"{int(feeder_id):02}"

    if not edisgo_obj:
        if not feeder_id:
            # TODO if feeder_id False
            raise NotImplementedError

        import_dir = cfg_g["feeder_extraction"].get("export")
        import_path = (
            data_dir / import_dir / str(grid_id) / "feeder" / str(feeder_id)
        )
        logger.info(f"Import Grid from file: {import_path}")

        edisgo_obj = import_edisgo_from_files(
            import_path,
            import_topology=True,
            import_timeseries=True,
            import_heat_pump=True,
            import_electromobility=True,
        )

        rolling_horizon_optimization(
            edisgo_obj,
            grid_id,
            feeder_id,
            run="test",
            load_results=cfg_o["load_results"],
            iteration=0,
            save=True,
        )

    if doit:
        return True


if __name__ == "__main__":

    from dodo import task_split_model_config_in_subconfig

    task_split_model_config_in_subconfig()
    run_dispatch_optimization(
        grid_id=2534, feeder_id=2, edisgo_obj=False, save=False, doit=False
    )

    # lopf.combine_results_for_grid
