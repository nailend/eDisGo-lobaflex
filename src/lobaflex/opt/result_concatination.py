import functools
import os
import re

from datetime import datetime

import pandas as pd

from lobaflex import config_dir, logs_dir, results_dir
from lobaflex.tools.logger import logging, setup_logging
from lobaflex.tools.tools import get_config, get_files_in_subdirs, log_errors

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
else:
    logger = logging.getLogger(__name__)


def concat_results(
    list_of_files, timeframe, grids=None, parameters=None, fillna=None
):
    """

    Parameters
    ----------
    list_of_files :
    timeframe :
    grids :
    parameters :
    fillna :

    Returns
    -------

    """

    # identify file by filename pattern:
    pattern = r"\d+/feeder/\d+/(.*)_(\d+)-(\d+)_iteration_(\d+).csv"
    mapping = pd.DataFrame(
        [re.search(pattern, string=i).groups() for i in list_of_files],
        columns=["parameter", "grid", "feeder", "iteration"],
    )
    mapping = mapping.sort_values(
        by=["grid", "parameter", "feeder", "iteration"]
    )

    # select specific parameters or grids if given
    if grids is not None:
        grids = grids if isinstance(grids, list) else list(grids)
        grids = [str(i) for i in grids]
        mapping = mapping.loc[mapping.grid.isin(grids)]
    if parameters is not None:
        parameters if isinstance(parameters, list) else list(parameters)
        mapping = mapping.loc[mapping.parameter.isin(parameters)]

    # concat iterations and feeder per grid and parameter
    collected_results = {}
    for group, grid_param in mapping.groupby(["grid", "parameter"]):

        df_grid_parameter = pd.DataFrame()

        for (grid, parameter, feeder), grid_param_feeder in grid_param.groupby(
            ["grid", "parameter", "feeder"]
        ):

            df_all_iterations = pd.concat(
                map(
                    functools.partial(
                        pd.read_csv, index_col=0, parse_dates=True
                    ),
                    list_of_files[grid_param_feeder.index],
                ),
                axis=0,
            )

            # slack_initial is only one timestep
            if "slack_initial" in parameter:
                df_all_iterations = df_all_iterations.T
            else:
                # only select defined timeframe
                df_all_iterations = df_all_iterations.loc[timeframe]
            # concat all feeder
            df_grid_parameter = pd.concat(
                [df_grid_parameter, df_all_iterations], axis=1
            )

        if df_grid_parameter.isna().any().any():
            logger.warning(
                f"There are NaN values in {grid}/{feeder}/{parameter}"
            )

            if fillna is not None:
                df_grid_parameter = df_grid_parameter.fillna(**fillna)
                logger.warning(
                    f"Nan Values replace in {grid}/{parameter}"
                    f" by {fillna.get('value', 'not specified')}"
                )

        collected_results.update({group: df_grid_parameter})

    return collected_results


@log_errors()
def save_concatinated_results(
    grids=None, remove=False, doit=False, version=None
):

    cfg_o = get_config(path=config_dir / ".opt.yaml")

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"opt_concat_results_{cfg_o['run_id']}_{date}.log"
    setup_logging(file_name=logfile)

    logger.info("Start concatenating files")
    # selected_parameters = ["charging_hp_el", "charging_tes", "energy_tes",
    #                        "x_charge_ev", "energy_level_cp"]
    selected_parameters = None

    run_dir = results_dir / cfg_o["run_id"]

    # get list of all iteration_*.csv
    list_of_files = pd.Series(
        get_files_in_subdirs(run_dir, pattern="*iteration_*.csv")
    )
    # define timeframe to concat
    timeframe = pd.date_range(
        start=cfg_o["start_datetime"],
        periods=cfg_o["total_timesteps"],
        freq="1h",
    )

    results = concat_results(
        list_of_files=list_of_files,
        timeframe=timeframe,
        grids=grids,
        parameters=selected_parameters,
        fillna={"value": 0},
    )

    for (grid, parameter), df in results.items():
        path = results_dir / cfg_o["run_id"] / grid / "mvgd"
        os.makedirs(path, exist_ok=True)
        filename = path / f"{grid}_{parameter}.csv"
        df.to_csv(filename, index=True)
        logger.info(f"Save concatenated results to {filename}.")

    if doit:
        return {"version": version, "run_id": f"concat_{cfg_o['run_id']}"}


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"result_concatination_{date}_local.log"
    setup_logging(file_name=logfile)

    save_concatinated_results(
        # grid_id=1056, feeder_id=1, edisgo_obj=False, save=True, doit=False
        # grid_id=2534,
        # grid_id=1056,
        grids=[1111],
        # feeder_id=8,
        doit=False,
    )

    # lopf.combine_results_for_grid
