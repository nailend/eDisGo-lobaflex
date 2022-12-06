import logging
import os
import shutil

from datetime import datetime

import edisgo.opf.lopf as lopf
import numpy as np
import pandas as pd

from DEV_optimisation import load_values_from_previous_failed_run
from edisgo.edisgo import import_edisgo_from_files
from edisgo.tools.tools import convert_impedances_to_mv

from lobaflex import config_dir, data_dir, results_dir
from lobaflex.grids.feeder_extraction import get_flexible_loads
from lobaflex.tools.logger import logger
from lobaflex.tools.tools import dump_yaml, get_config


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

    result_path = results_dir / run / str(grid_id) / feeder_id
    # TODO maybe add if condition/parameter
    shutil.rmtree(result_path, ignore_errors=True)
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

    # Due to different voltage levels, impedances need to adapted
    # alternatively p.u.
    logger.info("Convert impedances to mv reference system")
    edisgo_obj = convert_impedances_to_mv(edisgo_obj)

    logger.info("Import downstream node matrix")
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
    # interval = edisgo_obj.timeseries.timeindex[:50]
    interval = edisgo_obj.timeseries.timeindex[: cfg_o["total_timesteps"]]
    # window = timesteps_per_iteration # besserer Name?
    # intervals = timesteps / (timesteps_per_iteration - overlap_iterations)
    # for i in range(intervals)
    # timeindex.shift(periods = timesteps_per_iteration - overlap_iterations,
    #                     freq = "h")
    # iterations_per_era = 24*7 / timesteps_per_iteration - overlap_iterations

    # number of iterations is derived from number of total timesteps /
    # timesteps per iteration

    for iteration in range(
        start_iter, int(len(interval) / cfg_o["timesteps_per_iteration"])
    ):  # edisgo_obj.timeseries.timeindex.week.unique()

        logging.info(f"Starting optimisation for iteration {iteration}.")

        # if last iteration of era
        # overlap is added to window
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
            # Defines windows of iteration with timesteps
            #  24h
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
                # optimize_bess=cfg_o["opt_bess"],
                # optimize_emob=cfg_o["opt_emob"],
                # optimize_hp=cfg_o["opt_hp"],
                charging_start_hp=charging_start,
                energy_level_start_tes=energy_level_start,
                energy_level_end_tes=energy_level_end,
                flexible_loads=flexible_loads,
                **kwargs,
            )
        except NameError:
            model = lopf.setup_model(
                fixed_parameters=fixed_parameters,
                timesteps=timesteps,
                objective=cfg_o["objective"],
                # optimize_bess=cfg_o["opt_bess"],
                # optimize_emob=cfg_o["opt_emob"],
                # optimize_hp=cfg_o["opt_hp"],
                charging_start_hp=charging_start,
                energy_level_start_tes=energy_level_start,
                energy_level_end_tes=energy_level_end,
                flexible_loads=flexible_loads,
                **kwargs,
            )

        logger.info(f"Set up model for iteration {iteration}.")
        lp_filename = result_path / f"lp_file_iteration_{iteration}.lp"
        result_dict = lopf.optimize(
            model, cfg_o["solver"], lp_filename=lp_filename
        )
        # TODO workaround if hps not exist
        charging_hp[iteration] = result_dict["charging_hp_el"]
        charging_tes[iteration] = result_dict["charging_tes"]
        energy_level[iteration] = result_dict["energy_tes"]

        # if iteration is not the last one of era use results from last
        # iteration as starting value (overlapping hours are neglected)
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
        # if iteration is the last one of era
        #
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
                        result_path / f"{res_name}_{grid_id}-"
                        f"{feeder_id}_iteration"
                        f"_{iteration}.csv"
                    )
                    res.astype(np.float16).to_csv(filename)
            logger.info(f"Saved results for iteration {iteration}.")

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

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    # mvgds = cfg_g["model"].get("mvgd")
    feeder_id = f"{int(feeder_id):02}"

    if not edisgo_obj:
        if not feeder_id:
            # TODO if feeder_id False
            raise NotImplementedError

        else:
            import_dir = cfg_o.get("import_dir")
            import_path = (
                data_dir
                / import_dir
                / str(grid_id)
                / "feeder"
                / str(feeder_id)
            )
            logger.info(f"Import Grid from file: {import_path}")

        edisgo_obj = import_edisgo_from_files(
            import_path,
            import_topology=True,
            import_timeseries=True,
            import_heat_pump=True,
            import_electromobility=True,
        )

        # TODO workaround different year flex bands / timeseries
        for name, df in edisgo_obj.electromobility.flexibility_bands.items():
            if df.index.shape[0] == edisgo_obj.timeseries.timeindex.shape[0]:
                df.index = edisgo_obj.timeseries.timeindex
                edisgo_obj.electromobility.flexibility_bands.update({name: df})

        rolling_horizon_optimization(
            edisgo_obj,
            grid_id,
            feeder_id,
            run=cfg_o["run"],
            load_results=cfg_o["load_results"],
            iteration=0,
            save=save,
        )

    if doit:
        return True


if __name__ == "__main__":

    import sys

    from lobaflex.tools.tools import split_model_config_in_subconfig

    # logger_lopf = logging.getLogger("edisgo.opf.lopf")
    # logger_lopf.handlers.clear()
    # console_handler = logging.StreamHandler(stream=sys.stdout)
    # console_handler.setLevel(logging.INFO)
    # stream_formatter = logging.Formatter("%(name)s - %(levelname)s: %(message)s")
    # console_handler.setFormatter(stream_formatter)
    # logger_lopf.addHandler(console_handler)
    # logger_lopf.setLevel(logging.INFO)
    # logger_lopf.propagate = False

    split_model_config_in_subconfig()
    run_dispatch_optimization(
        grid_id=1056, feeder_id=1, edisgo_obj=False, save=True, doit=False
    )

    # lopf.combine_results_for_grid
