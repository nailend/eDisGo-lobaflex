import logging
import os
import re
import shutil
import warnings

from datetime import datetime
from pathlib import Path

import edisgo.opf.lopf as lopf
import networkx as nx
import numpy as np
import pandas as pd

from edisgo.edisgo import EDisGo, import_edisgo_from_files
from edisgo.network.topology import Topology
from edisgo.tools.tools import convert_impedances_to_mv

from lobaflex import config_dir, data_dir, logs_dir, results_dir
from lobaflex.opt.feeder_extraction import get_flexible_loads
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import dump_yaml, get_config, log_errors, timeit

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
else:
    logger = logging.getLogger(__name__)


@timeit
def get_downstream_nodes_matrix_iterative(grid):
    """
    Method that returns matrix M with 0 and 1 entries describing the relation
    of buses within the network. If bus b is descendant of a (assuming the
    station is the root of the radial network) M[a,b] = 1, otherwise M[a,b] = 0.
    The matrix is later used to determine the power flow at the different buses
    by multiplying with the nodal power flow. S_sum = M * s, where s is the
    nodal power vector.

    Note: only works for radial networks.

    Parameters
    ----------
    grid : either Topology, MVGrid or LVGrid

    Returns
    -------
    downstream_node_matrix : pd.DataFrame

    Todo: Check version with networkx successor
    """

    def recursive_downstream_node_matrix_filling(
        current_bus,
        current_feeder,
        downstream_node_matrix,
        grid,
        visited_buses,
    ):
        current_feeder.append(current_bus)
        for neighbor in tree.successors(current_bus):
            if (
                neighbor not in visited_buses
                and neighbor not in current_feeder
            ):
                recursive_downstream_node_matrix_filling(
                    neighbor,
                    current_feeder,
                    downstream_node_matrix,
                    grid,
                    visited_buses,
                )
        # current_bus = current_feeder.pop()
        downstream_node_matrix.loc[current_feeder, current_bus] = 1
        visited_buses.append(current_bus)
        # if len(visited_buses) % 10 == 0:
        #     logger.info(
        #         "{} % of the buses have been checked".format(
        #             len(visited_buses) / len(buses) * 100
        #         )
        #     )
        current_feeder.pop()

    buses = grid.buses_df.index.values
    if isinstance(grid, Topology):
        graph = grid.to_graph()
        slack = grid.mv_grid.station.index[0]
    else:
        graph = grid.graph
        slack = grid.transformers_df.bus1.iloc[0]
    tree = nx.bfs_tree(graph, slack)

    logger.info(f"Extract Downstream Note Matrix for {len(buses)} buses.")
    downstream_node_matrix = pd.DataFrame(columns=buses, index=buses)
    downstream_node_matrix.fillna(0, inplace=True)
    visited_buses = []
    current_feeder = []

    recursive_downstream_node_matrix_filling(
        slack, current_feeder, downstream_node_matrix, grid, visited_buses
    )

    return downstream_node_matrix


def export_results(result_dict, export_path, timesteps, filename):
    """Exports results to csv. Dropping all slack timesteps
    with values < 1e-6. Dropping overlap timesteps.

    Parameters
    ----------
    result_dict : dict
    export_path : PosixPath
    timesteps : pd.DatetimeIndex
    filename : str

    Returns
    -------

    """

    iteration = re.findall(r"iteration_(\d+)", filename)[0]

    for res_name, res in result_dict.items():

        # dont export overlap
        if "slack_initial" not in res_name:
            res = res.loc[timesteps]

        if "slack" in res_name:
            mask = res > 1e-6
            if mask.any().any():
                logger.warning(
                    f"Values > 1e-6 in {res_name} at iteration {iteration}."
                )
            res = res[mask]
            res = res.dropna(how="all")
            res = res.dropna(how="all")
        if res.empty:
            logger.info(f"No results for {res_name}.")
        else:
            file_path = export_path / filename.replace("$res_name$", res_name)
            res.astype(np.float16).to_csv(file_path)
            logger.info(f"Saved results for {res_name}.")


def update_start_values(result_dict, fixed_parameters):
    """
    End values of the iteration results are extracted to be used as
    starting values for the next iteration. End value is the last timestep
    minus the overlap.

    Parameters
    ----------
    result_dict : dict
    fixed_parameters : dict

    Returns
    -------
    start_values : dict
    """
    cfg_o = get_config(path=config_dir / ".opt.yaml")

    overlap_iterations = cfg_o["overlap_iterations"]

    # define start_values
    # will get updated afterwards
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

    # if iteration is not the last one of era use results from last
    # iteration as starting value (overlapping hours are neglected)

    if fixed_parameters["optimize_emob"]:

        start_values["charging_starts"].update(
            {"ev": result_dict["charging_ev"].iloc[-overlap_iterations]}
        )

        start_values["energy_level_starts"].update(
            {"ev": result_dict["energy_level_ev"].iloc[-overlap_iterations]}
        )

    else:
        logger.warning("No start values for electromobility")

    if fixed_parameters["optimize_hp"]:
        start_values["charging_starts"].update(
            {
                "hp": result_dict["charging_hp_el"].iloc[-overlap_iterations],
                "tes": result_dict["charging_tes"].iloc[-overlap_iterations],
            }
        )

        start_values["energy_level_starts"].update(
            {"tes": result_dict["energy_level_tes"].iloc[-overlap_iterations]}
        )
    else:
        logger.warning("No start values for heat pumps")
    return start_values


def prepare_input_parameters(edisgo_obj, timeframe_only=False):
    """Prepare input parameters for the LOPF.

    Parameters
    ----------
    edisgo_obj : :class:`edisgo.EDisGo`
        EDisGo object
    timeframe_only : bool
        If True, the optimization is performed for the in the config defined
        time frame only. If False, the optimization is performed for the whole
        time series of the edisgo object (default=False).

    Returns
    -------

    """

    cfg_o = get_config(path=config_dir / ".opt.yaml")

    logger.info("Compute downstream nodes matrix")
    downstream_nodes_matrix = get_downstream_nodes_matrix_iterative(
        edisgo_obj.topology
    )

    logger.info("Get flexible loads")
    cfg_flexible_loads = cfg_o["flexible_loads"]
    flexible_loads = get_flexible_loads(
        edisgo_obj=edisgo_obj,
        hp=cfg_flexible_loads["hp"],
        bev=cfg_flexible_loads["bev"],
        bess=cfg_flexible_loads["bess"],
        bev_flex_sectors=cfg_flexible_loads["bev_flex_sectors"],
    )

    logger.info("Extract time-invariant parameters")
    fixed_parameters = lopf.prepare_time_invariant_parameters(
        edisgo_obj=edisgo_obj,
        downstream_nodes_matrix=downstream_nodes_matrix,
        voltage_limits=True,
        per_unit=False,
        optimize_bess=cfg_flexible_loads["bess"],
        optimize_emob=cfg_flexible_loads["bev"],
        optimize_hp=cfg_flexible_loads["hp"],
        flexible_loads=flexible_loads,
    )

    if timeframe_only:
        # Define optimization timeframe by config
        start_datetime = cfg_o["start_datetime"]
        total_timesteps = cfg_o["total_timesteps"]

        # Slice timeframe
        start_index = edisgo_obj.timeseries.timeindex.slice_indexer(
            start_datetime
        ).start
        timeframe = edisgo_obj.timeseries.timeindex[
            start_index : start_index + total_timesteps
        ]
    else:
        logger.info("Whole time series is used for optimization")
        total_timesteps = edisgo_obj.timeseries.timeindex.shape[0]
        timeframe = edisgo_obj.timeseries.timeindex

    theretical_total_timesteps = len(
        pd.date_range(start=timeframe[0], end=timeframe[-1])
    )
    if total_timesteps < theretical_total_timesteps:
        logger.warning("You might have a splitted time series. Please check!")

    logger.info(
        f"Optimized timeframe is: {timeframe[0]} -> "
        f"{timeframe[-1]} including {total_timesteps} timesteps."
    )

    return fixed_parameters, flexible_loads, total_timesteps, timeframe


def long_term_optimization(
    edisgo_obj,
    grid_id,
    feeder_id,
    objective,
    timeframe_only=False,
    export_path=None,
):
    """Rolling horizon optimization for flexibilities like EVs and heat pumps.

    Parameters
    ----------
    edisgo_obj : :class:`edisgo.EDisGo`
        EDisGo object
    grid_id : int
        Grid id of the MVGD
    feeder_id : str or int
        Feeder id of the feeder of the MVGD, e.g. '01'
    objective : str
        Objective function to be optimized
    timeframe_only : bool
        If True, the optimization is performed for the in the config defined
        time frame only. If False, the optimization is performed for the whole
        time series of the edisgo object (default=False).
    export_path :

    Returns
    -------

    """

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    feeder_id = f"{int(feeder_id):02}"

    if export_path is not None:
        shutil.rmtree(export_path, ignore_errors=True)
        os.makedirs(export_path, exist_ok=True)
        # Dump opt configs to results
        dump_yaml(yaml_file=cfg_o, save_to=export_path)

    # Due to different voltage levels, impedances need to adapted
    # alternatively p.u.
    logger.info("Convert impedances to MV reference system")
    edisgo_obj = convert_impedances_to_mv(edisgo_obj)

    (
        fixed_parameters,
        flexible_loads,
        total_timesteps,
        timeframe,
    ) = prepare_input_parameters(edisgo_obj, timeframe_only)

    equal_splits = len(edisgo_obj.timeseries.timeindex) / (
        cfg_o["timesteps_per_iteration"] * cfg_o["iterations_per_era"]
    )
    windows = np.split(edisgo_obj.timeseries.timeindex, equal_splits)

    for iteration, window in enumerate(windows):

        if iteration == 0:
            logger.info("Set up model.")
            model = lopf.setup_model(
                fixed_parameters=fixed_parameters,
                timesteps=window,
                objective=objective,
                flexible_loads=flexible_loads,
                # charging_starts={"ev": 0, "hp": 0, "tes": 0},
                # **start_values, # TODO not needed?
                load_factor_rings=0.5 if cfg_o["n-1"] else None  # TODO N-1
                # DEACTIVATED!
                # **kwargs,
            )
        else:

            logger.info(f"Update model for iteration {iteration}.")
            model = lopf.update_model(
                model=model,
                timesteps=window,
                fixed_parameters=fixed_parameters,
                objective=objective,
                flexible_loads=flexible_loads,
                # **start_values,
                # **kwargs,
            )

        # lpfile
        if cfg_o["save_lp_files"]:
            lp_filename = export_path / f"lp_file_iteration_{iteration}.lp"
            logger.info(
                f"LP files for iteration {iteration} are saved to:"
                f" {lp_filename}"
            )

        else:
            lp_filename = None

        # logfile
        if cfg_o["save_solver_logs"]:
            date = datetime.now().date().isoformat()
            logfile = (
                logs_dir / f"gurobi_{date}_{grid_id}_{feeder_id}"
                f"_iteration_{iteration}.log"
            )
            logger.info(
                f"Solver logs for iteration {iteration} are saved to:"
                f" {logfile}"
            )
        else:
            logfile = None

        try:
            result_dict = lopf.optimize(
                model=model,
                solver=cfg_o["solver"],
                tee=cfg_o["print_solver_logs"],
                lp_filename=lp_filename,
                logfile=logfile,
                options=cfg_o["options"],
            )

            if result_dict is None:
                raise ValueError(
                    f"Optimization failed for iteration {iteration}."
                )

        except ValueError as e:
            logger.warning(e)
            logger.info("Tuning Gurobi parameters.")
            options = cfg_o["options"]
            options.update(
                {
                    "OptimalityTol": 1e-5,
                    "FeasibilityTol": 1e-5,
                    "BarConvTol": 1e-5,
                    "NumericFocus": 3,
                    "BarHomogeneous": 1,
                }
            )

            result_dict = lopf.optimize(
                model=model,
                solver=cfg_o["solver"],
                tee=cfg_o["print_solver_logs"],
                lp_filename=lp_filename,
                logfile=logfile,
                options=options,
            )
            if result_dict is None:
                raise ValueError(
                    f"Optimization failed for iteration {iteration} even "
                    "after Gurobi parameter tuning."
                )

        logger.info(f"Finished optimisation for iteration {iteration}.")

        # if export_path is not None:
        filename = (
            f"$res_name$_{grid_id}-{feeder_id}_iteration_{iteration}.csv"
        )
        try:
            export_results(
                result_dict=result_dict,
                export_path=export_path,
                timesteps=window,
                filename=filename,
            )
        except Exception:
            logger.warning(
                "Optimization Error. Result's couldn't be exported."
            )
            raise ValueError("Results not valid")


def rolling_horizon_optimization(
    edisgo_obj,
    grid_id,
    feeder_id,
    objective,
    timeframe_only=False,
    export_path=None,
):
    """Rolling horizon optimization for flexibilities like EVs and heat pumps.

    Parameters
    ----------
    edisgo_obj : :class:`edisgo.EDisGo`
        EDisGo object
    grid_id : int
        Grid id of the MVGD
    feeder_id : str or int
        Feeder id of the feeder of the MVGD, e.g. '01'
    objective : str
        Objective function to be optimized
    timeframe_only : bool
        If True, the optimization is performed for the in the config defined
        time frame only. If False, the optimization is performed for the whole
        time series of the edisgo object (default=False).
    export_path :

    Returns
    -------

    """

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    feeder_id = f"{int(feeder_id):02}"

    if export_path is not None:
        shutil.rmtree(export_path, ignore_errors=True)
        os.makedirs(export_path, exist_ok=True)
        # Dump opt configs to results
        dump_yaml(yaml_file=cfg_o, save_to=export_path)

    # Due to different voltage levels, impedances need to adapted
    # alternatively p.u.
    logger.info("Convert impedances to MV reference system")
    edisgo_obj = convert_impedances_to_mv(edisgo_obj)

    (
        fixed_parameters,
        flexible_loads,
        total_timesteps,
        timeframe,
    ) = prepare_input_parameters(edisgo_obj, timeframe_only)

    # Define rolling horizon parameters
    timesteps_per_iteration = cfg_o["timesteps_per_iteration"]
    iterations_per_era = cfg_o["iterations_per_era"]
    logger.info(
        f"Rolling horizon with {timesteps_per_iteration} timesteps "
        f"per iteration and {iterations_per_era} iterations per era."
    )

    # ####################### Rolling Horizon ############################
    # define result_dict for first iteration
    # will be overwritten afterwards
    result_dict = {}

    for iteration in range(0, int(len(timeframe) / timesteps_per_iteration)):

        logger.info(f"Starting optimisation for iteration {iteration}.")

        # Defines windows of iteration with timesteps
        # if last iteration of era, no overlap is added but energy_level
        # at the end needs to be reached
        if iteration % iterations_per_era == iterations_per_era - 1:
            timesteps = edisgo_obj.timeseries.timeindex[
                iteration
                * timesteps_per_iteration : (iteration + 1)
                * timesteps_per_iteration
            ]
            # Fixes end energy level to specific percentage (50%)
            energy_level_end = True
            logger.info("End of era")

        # in all other iterations overlap is added to the timeframe
        else:
            timesteps = edisgo_obj.timeseries.timeindex[
                iteration
                * timesteps_per_iteration : (iteration + 1)
                * timesteps_per_iteration
                + cfg_o["overlap_iterations"]
            ]
            energy_level_end = None

        if iteration % timesteps_per_iteration == 0:

            # define start_values for first iteration of era
            # will get updated afterwards
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

        else:
            logger.info("Update start values for next iteration.")
            start_values = update_start_values(result_dict, fixed_parameters)

        if iteration == 0:

            logger.info(f"Set up model for first iteration {iteration}.")
            model = lopf.setup_model(
                fixed_parameters=fixed_parameters,
                timesteps=timesteps,
                objective=objective,
                flexible_loads=flexible_loads,
                # charging_starts={"ev": 0, "hp": 0, "tes": 0},
                **start_values,
                load_factor_rings=0.5 if cfg_o["n-1"] else None  # TODO N-1
                # DEACTIVATED!
                # **kwargs,
            )
        else:

            logger.info(f"Update model for iteration {iteration}.")
            model = lopf.update_model(
                model=model,
                timesteps=timesteps,
                fixed_parameters=fixed_parameters,
                objective=objective,
                energy_level_end_tes=energy_level_end,
                energy_level_end_ev=energy_level_end,
                flexible_loads=flexible_loads,
                **start_values,
                # **kwargs,
            )

        # lpfile
        if cfg_o["save_lp_files"]:
            lp_filename = export_path / f"lp_file_iteration_{iteration}.lp"
            logger.info(
                f"LP files for iteration {iteration} are saved to:"
                f" {lp_filename}"
            )

        else:
            lp_filename = None

        # logfile
        if cfg_o["save_solver_logs"]:
            date = datetime.now().date().isoformat()
            logfile = (
                logs_dir / f"gurobi_{date}_{grid_id}_{feeder_id}"
                f"_iteration_{iteration}.log"
            )
            logger.info(
                f"Solver logs for iteration {iteration} are saved to:"
                f" {logfile}"
            )
        else:
            logfile = None

        try:
            result_dict = lopf.optimize(
                model=model,
                solver=cfg_o["solver"],
                tee=cfg_o["print_solver_logs"],
                lp_filename=lp_filename,
                logfile=logfile,
                options=cfg_o["options"],
            )

            if result_dict is None:
                raise ValueError(
                    f"Optimization failed for iteration {iteration}."
                )

        except ValueError as e:
            logger.warning(e)
            logger.info("Tuning Gurobi parameters.")
            options = cfg_o["options"]
            options.update(
                {
                    "OptimalityTol": 1e-5,
                    "FeasibilityTol": 1e-5,
                    "BarConvTol": 1e-5,
                    "NumericFocus": 3,
                    "BarHomogeneous": 1,
                }
            )

            result_dict = lopf.optimize(
                model=model,
                solver=cfg_o["solver"],
                tee=cfg_o["print_solver_logs"],
                lp_filename=lp_filename,
                logfile=logfile,
                options=options,
            )
            if result_dict is None:
                raise ValueError(
                    f"Optimization failed for iteration {iteration} even "
                    "after Gurobi parameter tuning."
                )

        logger.info(f"Finished optimisation for iteration {iteration}.")

        # if export_path is not None:
        filename = (
            f"$res_name$_{grid_id}-{feeder_id}_iteration_{iteration}.csv"
        )
        try:
            export_results(
                result_dict=result_dict,
                export_path=export_path,
                timesteps=timesteps[:timesteps_per_iteration],
                filename=filename,
            )
        except Exception:
            logger.warning(
                "Optimization Error. Result's couldn't be exported."
            )
            raise ValueError("Results not valid")


@log_errors
def run_dispatch_optimization(
    obj_or_path,
    grid_id,
    feeder_id,
    objective,
    rolling_horizon=False,
    run_id=None,
    version_db=None,
):
    """

    Parameters
    ----------
    obj_or_path : :class:`edisgo.EDisGo` or PosixPath
        edisgo object or path to edisgo dump
    grid_id :
        grid id of MVGD
    feeder_id : int or str
        feeder id, respective folder name of feeder
    objective :
        objective function to be used for optimization
    rolling_horizon : bool
        If True, rolling horizon optimization is performed else long-term
        optimization (default = False). !NOTE Currently fixed to objectives!
    run_id : str or None
        run id used for pydoit versioning
    version_db : dict or None
        Dictionary with version information for pydoit versioning

    Returns
    -------
    If run_id and version are not None, a dictionary with these values is
    given for the pydoit versioning.
    """
    # Log to pipeline log file
    logger.info(f"Run dispatch optimization of {grid_id}/{feeder_id}")

    warnings.simplefilter(action="ignore", category=FutureWarning)
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    feeder_id = f"{int(feeder_id):02}"

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"opt_{run_id}_{grid_id}-{feeder_id}" f"_{date}.log"
    setup_logging(file_name=logfile)

    logger.info(
        f"Run optimization for grid: {grid_id}, feeder: {feeder_id}"
        f" with run id: {run_id}"
    )
    logger.info(f"Objective: {objective}")
    if isinstance(obj_or_path, EDisGo):
        edisgo_obj = obj_or_path
        export_path = results_dir / run_id / objective
    else:

        logger.info(f"Import Grid from file: {obj_or_path}")

        edisgo_obj = import_edisgo_from_files(
            obj_or_path,
            import_topology=True,
            import_timeseries=True,
            import_heat_pump=True,
            import_electromobility=True,
        )
        if objective in [
            "maximize_grid_power",
            "minimize_grid_power",
            "maximize_energy_level",
            "minimize_energy_level",
        ]:

            # Use long-term optimization for these objectives if false
            if cfg_o["rolling_horizon"]["pot"]:
                rolling_horizon = True
            else:
                rolling_horizon = False

            # Add extra directory layer for potentials
            directory = Path("potential") / obj_or_path.parent.parent.name

        else:

            # use rolling horizon optimization for all other objectives
            rolling_horizon = True

            # No extra directory layer needed
            directory = ""

        export_path = (
            results_dir
            / run_id
            / str(grid_id)
            / directory
            / objective
            / "results"
            / feeder_id
        )
        os.makedirs(export_path, exist_ok=True)

    # TODO Move to edisgo feeder extraction + timeseries extraction
    # logger.info("Check integrity.")
    # edisgo_obj.check_integrity()

    logger.info("Run Powerflow for first timestep")
    try:
        edisgo_obj.analyze(timesteps=edisgo_obj.timeseries.timeindex[0])
    except Exception as e:
        logger.warning("Powerflow not successful.")
        logger.warning(e)

    if rolling_horizon:
        logger.info("Run rolling horizon optimization.")
        rolling_horizon_optimization(
            edisgo_obj,
            grid_id,
            feeder_id,
            objective=objective,
            timeframe_only=False,
            export_path=export_path,
        )
    else:
        logger.info("Run long-term optimization.")
        long_term_optimization(
            edisgo_obj,
            grid_id,
            feeder_id,
            objective=objective,
            timeframe_only=False,
            export_path=export_path,
        )

    if version_db is not None:
        return version_db["db"]


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"dispatch_optimization_{date}_local.log"
    setup_logging(file_name=logfile)

    grid_id = 9999
    run_dispatch_optimization(
        # obj_or_path=results_dir / "debug" / "1111" / "timeframe_feeder" / "01",
        obj_or_path=data_dir / cfg_o["import_dir"] / str(grid_id),
        grid_id=grid_id,
        feeder_id=2,
        # objective="minimize_loading",
        objective="maximize_grid_power",
        rolling_horizon=False,
        run_id="long_term",
        version_db=None,
    )
