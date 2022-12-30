import os
import re

from datetime import datetime

import pandas as pd

from lobaflex import config_dir, logs_dir, results_dir
from lobaflex.tools.logger import logging, setup_logging
from lobaflex.tools.tools import get_config, get_files_in_subdirs

logger = logging.getLogger("lobaflex.opt." + __name__)


def concat_results(run_id, grids=None, parameters=None, fillna=None):
    run_dir = results_dir / run_id

    # get list of all iteration_*.csv
    list_of_files = pd.Series(
        get_files_in_subdirs(run_dir, pattern="*iteration_*.csv")
    )

    # identify file by filename pattern:
    pattern = r"\d+/\d+/(.*)_(\d+)-(\d+)_iteration_(\d+).csv"
    mapping = pd.DataFrame(
        {re.search(pattern, string=i).groups() for i in list_of_files},
        columns=["parameter", "grid", "feeder", "iteration"],
    )
    mapping = mapping.sort_values(
        by=["grid", "parameter", "feeder", "iteration"]
    )

    # select specific parameters or grids if given
    if grids is not None:
        grids if isinstance(grids, list) else list(grids)
        mapping = mapping.loc[mapping.grid.isin(grids)]
    if parameters is not None:
        parameters if isinstance(parameters, list) else list(parameters)
        mapping = mapping.loc[mapping.parameter.isin(parameters)]

    # concat iterations and feeder per grid and parameter
    collected_results = {}
    for group, grid_param in mapping.groupby(["grid", "parameter"]):

        df_feeder = pd.DataFrame()

        for (grid, parameter, feeder), grid_param_feeder in grid_param.groupby(
            ["grid", "parameter", "feeder"]
        ):

            # concat all iterations
            df_all_iterations = pd.concat(
                map(pd.read_csv, list_of_files[grid_param_feeder.index]),
                axis=1,
            )
            # concat all feeder
            df_grid_parameter = pd.concat(
                [df_feeder, df_all_iterations], axis=0
            )

        if df_grid_parameter.isna().any().any():
            logger.warning(f"There are NaN values in {grid}/{parameter}")

        if fillna is not None:
            df_grid_parameter = df_grid_parameter.fillna(**fillna)
            logger.warning(
                f"Nan Values replace in {grid}/{parameter}"
                f" by {fillna.get('value', 'not specified')}"
            )

        collected_results.update({group: df_grid_parameter})

    return collected_results


def save_concatinated_results(doit=False, version=None):

    cfg_o = get_config(path=config_dir / ".opt.yaml")

    date = datetime.now().isoformat()[:10]
    logfile = logs_dir / f"opt_concat_results_{cfg_o['run']}_{date}.log"
    setup_logging(file_name=logfile)

    logger.info("Start concating files")
    # selected_parameters = ["charging_hp_el", "charging_tes", "energy_tes",
    #                        "x_charge_ev", "energy_level_cp"]
    selected_parameters = None

    results = concat_results(
        run_id=cfg_o["run"],
        parameters=selected_parameters,
        fillna={"value": 0},
    )

    for (grid, parameter), df in results.items():
        path = results_dir / cfg_o["run"] / grid / "concated"
        os.makedirs(path, exist_ok=True)
        filename = path / f"{grid}_{parameter}.csv"
        df.to_csv(filename)
        logger.info(f"Concated results saved to {filename}.")

    if doit:
        return {"version": version, "run": f"concat_{cfg_o['run']}"}
