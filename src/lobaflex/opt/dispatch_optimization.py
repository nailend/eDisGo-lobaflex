import logging
import os
import re
import shutil

from datetime import datetime

import edisgo.opf.lopf as lopf
import numpy as np
import pandas as pd

from DEV_optimisation import load_values_from_previous_failed_run
from edisgo.edisgo import import_edisgo_from_files
from edisgo.tools.tools import (
    assign_voltage_level_to_component,
    convert_impedances_to_mv,
)

from lobaflex import config_dir, data_dir, logs_dir, results_dir
from lobaflex.grids.feeder_extraction import get_flexible_loads
from lobaflex.tools.logger import setup_logging

# from lobaflex.tools.logger import logger
from lobaflex.tools.tools import dump_yaml, get_config

logger = logging.getLogger(__name__)


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


def tie_end_to_start_hp(result_dict, iteration, cfg_o):
    """

    Parameters
    ----------
    result_dict :
    iteration :
    cfg_o :

    Returns
    -------

    """
    start_values_hp = dict()

    iterations_per_era = cfg_o["iterations_per_era"]
    overlap_iterations = cfg_o["overlap_iterations"]

    # if iteration is the last one of era
    if iteration % iterations_per_era == iterations_per_era - 1:
        start_values_hp["charging_start"] = None
        start_values_hp["energy_level_start"] = None

    # if iteration is not the last one of era use results from last
    # iteration as starting value (overlapping hours are neglected)
    else:
        charging_start = {
            "hp": result_dict["charging_hp_el"].iloc[-overlap_iterations],
            "tes": result_dict["charging_tes"].iloc[-overlap_iterations],
        }
        start_values_hp["charging_start_hp"] = charging_start

        energy_level_start = result_dict["energy_tes"].iloc[
            -overlap_iterations
        ]
        start_values_hp["energy_level_start_tes"] = energy_level_start

    return start_values_hp


def export_results(result_dict, result_path, timesteps, filename):
    """

    Parameters
    ----------
    result_dict :
    result_path :
    timesteps :
    filename :

    Returns
    -------

    """

    # iteration = re.search(r"iteration_(\d+)", filename).group(1)
    for res_name, res in result_dict.items():
        try:
            res = res.loc[timesteps]
            # TODO properly handle exception
        except Exception as e:
            logger.info(f"No results for {res_name}.")
            continue
        if "slack" in res_name:
            res = res[res > 1e-6]
            res = res.dropna(how="all")
            res = res.dropna(how="all")
        if not res.empty:
            file_path = result_path / filename.replace("$res_name$", res_name)
            res.astype(np.float16).to_csv(file_path)
            logger.info(f"Saved results for {res_name}.")


def tie_end_to_start_emob(result_dict, iteration, cfg_o):
    """

    Parameters
    ----------
    result_dict :
    iteration :
    cfg_o :

    Returns
    -------

    """

    start_values_emob = dict()

    iterations_per_era = cfg_o["iterations_per_era"]
    overlap_iterations = cfg_o["overlap_iterations"]

    # if iteration is the last one of era
    if iteration % iterations_per_era == iterations_per_era - 1:
        start_values_emob["charging_start_ev"] = None
        start_values_emob["energy_level_start_ev"] = None

    # if iteration is not the last one of era use results from last
    # iteration as starting value (overlapping hours are neglected)
    else:

        start_values_emob["charging_start_ev"] = result_dict[
            "x_charge_ev"
        ].iloc[-overlap_iterations]

        start_values_emob["energy_level_start_ev"] = result_dict[
            "energy_level_cp"
        ].iloc[-overlap_iterations]

    return start_values_emob


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

    # logfile
    date = datetime.now().isoformat()[:10]
    logfile = logs_dir / f"{date}_gurobi.log"

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

    # get v_min, v_max per bus

    v_minmax = pd.DataFrame(
        data=fixed_parameters["grid_object"].buses_df.index, columns=["bus"]
    )
    v_minmax = assign_voltage_level_to_component(
        v_minmax, fixed_parameters["grid_object"].buses_df
    )
    v_minmax.loc[v_minmax["voltage_level"] == "mv", "v_min"] = 0.985
    v_minmax.loc[v_minmax["voltage_level"] == "mv", "v_max"] = 1.05
    v_minmax.loc[v_minmax["voltage_level"] == "lv", "v_min"] = 0.9
    v_minmax.loc[v_minmax["voltage_level"] == "lv", "v_max"] = 1.1
    v_minmax = v_minmax.set_index("bus")

    start_values_hp = {}
    start_values_emob = {}

    # TODO adhoc workaround
    # interval = edisgo_obj.timeseries.timeindex[:50]
    interval = edisgo_obj.timeseries.timeindex[: cfg_o["total_timesteps"]]

    for iteration in range(
        start_iter, int(len(interval) / cfg_o["timesteps_per_iteration"])
    ):

        logging.info(f"Starting optimisation for iteration {iteration}.")

        # Defines windows of iteration with timesteps
        # if last iteration of era, no overlap is added but energy_level
        # at the end needs to be reached
        if (
            iteration % cfg_o["iterations_per_era"]
            == cfg_o["iterations_per_era"] - 1
        ):
            timesteps = edisgo_obj.timeseries.timeindex[
                iteration
                * cfg_o["timesteps_per_iteration"] : (iteration + 1)
                * cfg_o["timesteps_per_iteration"]
            ]
            energy_level_end = True

        # in all other iterations overlap is added to the timeframe
        else:
            timesteps = edisgo_obj.timeseries.timeindex[
                iteration
                * cfg_o["timesteps_per_iteration"] : (iteration + 1)
                * cfg_o["timesteps_per_iteration"]
                + cfg_o["overlap_iterations"]
            ]
            energy_level_end = None

        logger.info(f"Set up model for iteration {iteration}.")
        try:
            model = lopf.update_model(
                model=model,
                timesteps=timesteps,
                fixed_parameters=fixed_parameters,
                objective=cfg_o["objective"],
                # energy_level_end_tes=energy_level_end,
                flexible_loads=flexible_loads,
                **start_values_hp,
                **start_values_emob,
                # **kwargs,
            )
        except NameError:
            model = lopf.setup_model(
                fixed_parameters=fixed_parameters,
                timesteps=timesteps,
                objective=cfg_o["objective"],
                charging_start_hp=charging_start,
                energy_level_start_tes=energy_level_start,
                energy_level_end_tes=energy_level_end,
                flexible_loads=flexible_loads,
                v_min=v_minmax["v_min"],
                v_max=v_minmax["v_max"],
                logfile=logfile,  # doesnt work
                # **kwargs,
            )

        lp_filename = result_path / f"lp_file_iteration_{iteration}.lp"
        result_dict = lopf.optimize(
            model, cfg_o["solver"], lp_filename=lp_filename
        )
        # TODO workaround if hps not exist
        try:
            start_values_hp = tie_end_to_start_hp(
                result_dict, iteration, cfg_o
            )
        except Exception:
            pass

        try:
            start_values_emob = tie_end_to_start_emob(
                result_dict, iteration, cfg_o
            )
        except Exception:
            pass

        logger.info(f"Finished optimisation for week {iteration}.")

        if save:

            filename = (
                f"$res_name$_{grid_id}-{feeder_id}_iteration"
                f"_{iteration}.csv"
            )
            try:
                export_results(
                    result_dict=result_dict,
                    result_path=result_path,
                    timesteps=timesteps[: cfg_o["timesteps_per_iteration"]],
                    filename=filename,
                )
            except Exception:
                logger.info(
                    "Optimization Error. Result's couldn't be exported."
                )
                raise ValueError("Results not valid")

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
    grid_id,
    feeder_id=False,
    edisgo_obj=False,
    save=False,
    doit=False,
    version=None,
):

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    feeder_id = f"{int(feeder_id):02}"

    date = datetime.now().isoformat()[:10]
    logfile = logs_dir / f"opt_{cfg_o['run']}_{grid_id}-{feeder_id}_{date}.log"
    setup_logging(file_name=logfile)

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

        logger.info("Check integrity.")
        edisgo_obj.check_integrity()

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
        return {"version": version}


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")

    run_dispatch_optimization(
        # grid_id=1056, feeder_id=1, edisgo_obj=False, save=True, doit=False
        grid_id=2534,
        feeder_id=8,
        edisgo_obj=False,
        save=True,
        doit=False,
    )

    # lopf.combine_results_for_grid
