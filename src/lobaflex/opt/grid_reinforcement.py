import logging
import os
import re
import warnings

from copy import deepcopy
from datetime import datetime

import numpy as np
import pandas as pd

from edisgo.edisgo import EDisGo, import_edisgo_from_files

from lobaflex import config_dir, logs_dir, results_dir
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, log_errors

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
else:
    logger = logging.getLogger(__name__)


def iterative_reinforce(
    edisgo_obj,
    timesteps=None,
    mode=None,
    iterations=10,
    iteration_start=0.5,
    combined_analysis=False,
):
    """Edisgo reinforce is conducted if Value Error is raised.

    Parameters
    ----------
    edisgo_obj : :class:`edisgo.EDisGo`
        EDisGo object
    timesteps : pd.DatetimeIndex, pd.Timestamp
        Timesteps to be analyzed and reinforced. If None all timesteps are
        taken. Default: None
    mode : str
            * 'split'
                Catch not converged time steps from error message and rerun
                converged ones first. Then rerun reinforced grid with not
                converged time steps. Limitation to max 100 time steps!
            * 'lpf'
                Non-linear power flow initial guess is seeded with the voltage
                angles from the linear power flow.
            * 'iterative'
                Reinforcement is conducted by reducing all power values of
                generators and loads to a fraction starting from x% to 100%,
                e.g. solving the load flow and reinforcement with 50% and then
                60%,....
    iteration_start : float
        relative start value for 'iterative' mode. Default 0.5
    iterations : int
        number of iterations taken in 'iterative' mode from 50% until 100%
        gen/load is reached e.g. 5 : [0.5, 0.6, 0.7, 0.8, 0.9, 1]
    combined_analysis : bool
        If True allowed voltage deviations for combined analysis of MV
        and LV topology are used. If False different allowed voltage
        deviations for MV and LV are used. See also config section
        `grid_expansion_allowed_voltage_deviations`. If `mode` is set to
        'mv' `combined_analysis` should be False. Default: False.
    Returns
    -------

    """

    if timesteps is None:
        timesteps = edisgo_obj.timeseries.timeindex

    logger.info(f"Start reinforce for {len(timesteps)} time steps.")

    try:
        edisgo_obj.reinforce(
            combined_analysis=combined_analysis, timesteps_pfa=timesteps
        )
    except ValueError as e:
        if mode is not None:
            logger.warning(f"Reinforce failed. Restart in {mode} mode.")
        if mode == "split":
            # might not catch all timesteps if > 100 print out is limited to
            # 50 lines. Better to use return values of _check_convergence()
            # in analyze()
            exluded_timesteps = re.findall(
                pattern=r"DatetimeIndex\(\[([\S\s]*)\],[\s]*dtype=",
                string=str(e),
            )[0]
            exluded_timesteps = re.sub(
                pattern=r"',[\s]*'", string=exluded_timesteps, repl="', '"
            )
            exluded_timesteps = exluded_timesteps.split(", ")

            exluded_timesteps = pd.to_datetime(exluded_timesteps)

            logger.warning(
                f"Powerflow didn't converge for {len(exluded_timesteps)} time "
                f"steps: {exluded_timesteps}."
            )
            reduced_timesteps = edisgo_obj.timeseries.timeindex.drop(
                exluded_timesteps
            )
            logger.info(
                "Start partial reinforce with reduced time steps ("
                f"{len(reduced_timesteps)})."
            )
            edisgo_obj.reinforce(
                reduced_timesteps,
                combined_analysis=combined_analysis,
                # troubleshooting_mode="lpf", # said to be only useful in
                # case of non-convergence, mostly bad seed
                raise_not_converged=False,
                max_while_iterations=50,
            )
            logger.info(
                "Continue partial reinforce with excluded time steps ("
                f"{len(exluded_timesteps)})."
            )
            edisgo_obj.reinforce(
                exluded_timesteps,
                combined_analysis=combined_analysis,
                # troubleshooting_mode="lpf",
                raise_not_converge=False,
                max_while_iterations=50,
            )

            logger.info("Final reinforce.")
            edisgo_obj.reinforce(
                combined_analysis=combined_analysis,
                max_while_iterations=50,
                timesteps_pfa=timesteps,
            )
        elif mode == "lpf":
            edisgo_obj.reinforce(
                combined_analysis=combined_analysis,
                troubleshooting_mode="lpf",
                timesteps_pfa=timesteps,
            )
        elif mode == "iterative":

            ts_orig = deepcopy(edisgo_obj.timeseries)
            for n in np.linspace(iteration_start, 1, iterations):

                logger.info(f"Fraction: {n} x load")

                for attr in ts_orig._attributes:
                    setattr(
                        edisgo_obj.timeseries, attr, getattr(ts_orig, attr) * n
                    )

                edisgo_obj.reinforce(
                    combined_analysis=combined_analysis,
                    max_while_iterations=50,
                    timesteps_pfa=timesteps,
                    raise_not_converge=False,
                )

            logger.info("Final reinforce.")
            edisgo_obj.reinforce(
                combined_analysis=combined_analysis, max_while_iterations=50
            )
        else:
            logging.warning(e)
            raise ValueError("No reinforcement mode selected")

    return edisgo_obj


@log_errors
def reinforce_grid(
    obj_or_path, grid_id=None, objective=None, run_id=None, version_db=None
):
    """

    Parameters
    ----------
    obj_or_path : :class:`edisgo.EDisGo` or PosixPath
        edisgo object or path to edisgo dump
    grid_id : int
        grid id of MVGD
    objective : str
        Keyword after which the grid is reinforced. This is used for logging
        and result path purposes only
    run_id : str
        run id used for pydoit versioning
    version_db : dict
        Dictionary with version information for pydoit versioning

    Returns
    -------

    """
    # Log to pipeline log file
    logger.info(f"Run grid reinforcement for {objective} of {grid_id} ")

    warnings.simplefilter(action="ignore", category=FutureWarning)

    date = datetime.now().date().isoformat()

    logfile = logs_dir / f"opt_{objective}_reinforcement_{run_id}_{date}.log"
    setup_logging(file_name=logfile)

    logger.info(
        f"Start reinforcement for {objective} of {grid_id} in {run_id}."
    )

    if isinstance(obj_or_path, EDisGo):
        edisgo_obj = obj_or_path
    else:
        logger.info(f"Import Grid from file: {obj_or_path}")

        edisgo_obj = import_edisgo_from_files(
            obj_or_path,
            import_topology=True,
            import_timeseries=True,
            import_electromobility=True,
            import_heat_pump=True,
        )

    export_path = (
        results_dir / run_id / str(grid_id) / objective / "reinforced"
    )
    os.makedirs(export_path, exist_ok=True)

    edisgo_obj = iterative_reinforce(
        edisgo_obj, combined_analysis=False, mode="split", iterations=5
    )

    logger.info(f"Save reinforced grid to {export_path}")
    edisgo_obj.save(
        export_path,
        save_topology=True,
        save_timeseries=True,
        save_heatpump=True,
        save_electromobility=True,
        electromobility_attributes=[
            "integrated_charging_parks_df",
            "simbev_config_df",
            "flexibility_bands",
        ],
        save_results=True,
    )

    if version_db is not None:
        return version_db["db"]


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"minimal_reinforcement_{date}_local.log"
    setup_logging(file_name=logfile)

    reinforce_grid(
        obj_or_path=results_dir / "debug" / "1111" / "minimize_loading_mvgd",
        grid_id=1111,
        objective="minimize_loading",
        run_id="debug",
        version_db=None,
    )
