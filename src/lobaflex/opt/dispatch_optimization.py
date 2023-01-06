import logging
import os
import shutil
import warnings

from datetime import datetime

import edisgo.opf.lopf as lopf
import numpy as np
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files
from edisgo.tools.tools import (
    assign_voltage_level_to_component,
    convert_impedances_to_mv,
)

from lobaflex import config_dir, data_dir, logs_dir, results_dir
from lobaflex.grids.feeder_extraction import get_flexible_loads
from lobaflex.tools.logger import setup_logging
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


def close_iteration_windows(result_dict, iteration, cfg_o):
    """
    End values of the iteration results are extracted to be used as
    starting values for the next iteration. End values is the last timestep
    minus the overlap.

    Parameters
    ----------
    result_dict :
    iteration :
    cfg_o :

    Returns
    -------

    """

    iterations_per_era = cfg_o["iterations_per_era"]
    overlap_iterations = cfg_o["overlap_iterations"]

    start_values = {
        "energy_level_starts": {
            "ev": None,
            "tes": None,
        },
        "charging_starts": {
            "ev": None,
            "tes": None,
            "hp": None,
        },
    }

    # if iteration is the last one of era
    if iteration % iterations_per_era == iterations_per_era - 1:
        start_values = start_values.update(
            {
                "energy_level_starts": {
                    "ev": None,
                    "tes": None,
                },
                "charging_starts": {
                    "ev": None,
                    "tes": None,
                    "hp": None,
                },
            }
        )

    # if iteration is not the last one of era use results from last
    # iteration as starting value (overlapping hours are neglected)

    else:
        if cfg_o["opt_emob"]:

            start_values["charging_starts"].update(
                {"ev": result_dict["x_charge_ev"].iloc[-overlap_iterations]}
            )

            start_values["energy_level_starts"].update(
                {
                    "ev": result_dict["energy_level_cp"].iloc[
                        -overlap_iterations
                    ]
                }
            )

        else:
            logging.info("No start values for electromobility")

        if cfg_o["opt_hp"]:

            start_values["charging_starts"].update(
                {
                    "hp": result_dict["charging_hp_el"].iloc[
                        -overlap_iterations
                    ],
                    "tes": result_dict["charging_tes"].iloc[
                        -overlap_iterations
                    ],
                }
            )

            start_values["energy_level_starts"].update(
                {"tes": result_dict["energy_tes"].iloc[-overlap_iterations]}
            )
        else:
            logging.info("No start values for heat pumps")
    return start_values


def rolling_horizon_optimization(
    edisgo_obj,
    grid_id,
    feeder_id,
    save=False,
    run=datetime.now().isoformat(),
    save_lp_file=False,
):

    # cfg_g = get_config(path=config_dir / ".grids.yaml")
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    # mvgds = cfg_g["model"].get("mvgd")
    feeder_id = f"{int(feeder_id):02}"

    result_path = results_dir / run / str(grid_id) / feeder_id
    # TODO maybe add if condition/parameter
    shutil.rmtree(result_path, ignore_errors=True)
    os.makedirs(result_path, exist_ok=True)

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
        data=fixed_parameters["grid_object"].buses_df.index.rename("bus"),
        # columns=[
        #     "bus"]
    )
    v_minmax = assign_voltage_level_to_component(
        v_minmax, fixed_parameters["grid_object"].buses_df
    )
    v_minmax.loc[v_minmax["voltage_level"] == "mv", "v_min"] = 0.985
    v_minmax.loc[v_minmax["voltage_level"] == "mv", "v_max"] = 1.05
    v_minmax.loc[v_minmax["voltage_level"] == "lv", "v_min"] = 0.9
    v_minmax.loc[v_minmax["voltage_level"] == "lv", "v_max"] = 1.1
    v_minmax = v_minmax.set_index("bus")

    # Define optimization timeframe
    if cfg_o["start_datetime"] is not None:
        start_index = edisgo_obj.timeseries.timeindex.slice_indexer(
            cfg_o["start_datetime"]
        ).start
        timeframe = edisgo_obj.timeseries.timeindex[
            start_index : start_index + cfg_o["total_timesteps"]
        ]
    else:
        logger.info("No start_datetime given. Start with first timestep")
        timeframe = edisgo_obj.timeseries.timeindex[: cfg_o["total_timesteps"]]

    for iteration in range(
        0, int(len(timeframe) / cfg_o["timesteps_per_iteration"])
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
            # Fixes end energy level to specific percentage (50%)
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

        if iteration == 0:
            logger.info(f"Set up model for first iteration {iteration}.")
            model = lopf.setup_model(
                fixed_parameters=fixed_parameters,
                timesteps=timesteps,
                objective=cfg_o["objective"],
                v_min=v_minmax["v_min"],
                v_max=v_minmax["v_max"],
                flexible_loads=flexible_loads,
                charging_starts={"ev": 0, "hp": 0, "tes": 0},
                load_factor_rings=0.5
                # **kwargs,
            )
        else:
            logger.info(f"Update model for iteration {iteration}.")
            model = lopf.update_model(
                model=model,
                timesteps=timesteps,
                fixed_parameters=fixed_parameters,
                objective=cfg_o["objective"],
                energy_level_end_tes=energy_level_end,
                flexible_loads=flexible_loads,
                **start_values,
                # **kwargs,
            )

        if save_lp_file:
            lp_filename = result_path / f"lp_file_iteration_{iteration}.lp"
        else:
            lp_filename = False

        # logfile
        date = datetime.now().date().isoformat()
        logfile = logs_dir / f"gurobi_{date}_iteration_{iteration}.log"

        result_dict = lopf.optimize(
            model, cfg_o["solver"], lp_filename=lp_filename, logfile=logfile
        )

        try:
            start_values = close_iteration_windows(
                result_dict, iteration, cfg_o
            )
        except Exception as e:
            logger.warning(
                f"No starting values extracted for iteration" f" {iteration}"
            )
            logger.warning(e)
            pass

        logger.info(f"Finished optimisation for iteration {iteration}.")

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


def run_dispatch_optimization(
    grid_id,
    feeder_id=False,
    edisgo_obj=False,
    save=False,
    doit=False,
    version=None,
):

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    feeder_id = f"{int(feeder_id):02}"
    # get run id for export path
    run_id = cfg_o.get("run", f"no_id_{datetime.now().isoformat()}")

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"opt_{cfg_o['run']}_{grid_id}-{feeder_id}_{date}.log"
    setup_logging(file_name=logfile)

    logger.info(
        f"Run optimization for grid: {grid_id}, feeder: {feeder_id}"
        f" with run id: {run_id}"
    )

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

    logger.info("Run Powerflow for first timestep")
    edisgo_obj.analyze(timesteps=edisgo_obj.timeseries.timeindex[0])

    rolling_horizon_optimization(
        edisgo_obj,
        grid_id,
        feeder_id,
        save=save,
        run=run_id,
        save_lp_file=cfg_o.get("save_lp_files", False),
    )

    if doit:
        return {"version": version, "run": run_id}


if __name__ == "__main__":

    from datetime import datetime

    from lobaflex import logs_dir
    from lobaflex.tools.logger import setup_logging
    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"dispatch_optimization_{date}_local.log"
    setup_logging(file_name=logfile)

    run_dispatch_optimization(
        # grid_id=1056, feeder_id=1, edisgo_obj=False, save=True, doit=False
        # grid_id=2534,
        # grid_id=1056,
        grid_id="5_bus_testgrid",
        # feeder_id=8,
        feeder_id=1,
        edisgo_obj=False,
        save=True,
        doit=False,
    )

    # lopf.combine_results_for_grid
