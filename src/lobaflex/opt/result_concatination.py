import functools
import os
import re

from datetime import datetime
from pathlib import Path

import pandas as pd

from lobaflex import config_dir, logs_dir, results_dir
from lobaflex.tools.logger import logging, setup_logging
from lobaflex.tools.tools import get_config, get_files_in_subdirs, log_errors

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.opt." + __name__)
else:
    logger = logging.getLogger(__name__)


def concat_results(path, timeframe=None, parameters=None, fillna=None):
    """
    Concat results of all iterations and feeders of one grid and
    selected parameters.

    Parameters
    ----------
    path : PosixPath
        Path to directory containing results of dispatch
        optimization with pattern `iteration_*.csv`
    timeframe : pd.DatetimeIndex
        Timeframe to be selected, if None all times steps are selected
    parameters : list of str, optional
        List of parameters to be collected. If None, all parameters are
    fillna : dict, optional
        kwargs from `pandas.core.frame.DataFrame.fillna`

    Returns
    -------
    collected_results : dict
        Dictionary with parameter as key and concatinated results as value

    """
    # get list of all iteration_*.csv
    list_of_files = pd.Series(
        get_files_in_subdirs(path, pattern="*iteration_*.csv")
    )

    # identify file by filename pattern:
    pattern = r"\d+/.*/\d+/(.*)_(\d+)-(\d+)_iteration_(\d+).csv"
    mapping = pd.DataFrame(
        [re.search(pattern, string=i).groups() for i in list_of_files],
        columns=["parameter", "grid", "feeder", "iteration"],
    )
    mapping = mapping.sort_values(
        by=["grid", "parameter", "feeder", "iteration"]
    )

    # select parameters from list of csv files
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
            # concat all iterations of one feeder
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
            # elif "slack" in parameter:
            #     # all other slacks only contain timesteps with values
            #     pass
            # else:
            #     # only select defined timeframe
            #     logger.debug(
            #         f"Select timesteps of {grid}/{feeder}" f"/{parameter}."
            #     )
            #     df_all_iterations = df_all_iterations.loc[timeframe]
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


@log_errors
def save_concatenated_results(grid_id, path, run_id=None, version_db=None):
    """Concatenate all results of one grid and save them to csv.

    Parameters
    ----------
    grid_id : int
    path : PosixPath
    run_id : str
        run id used for pydoit versioning
    version_db : dict
        Dictionary with version information for pydoit versioning

    Returns
    -------
    If run_id and version are not None, a dictionary with these values is
    given for the pydoit versioning.

    """
    # Log to pipeline log file
    logger.info(f"Run result concatenation of {grid_id}")

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"opt_concat_results_{run_id}_{grid_id}_{date}.log"
    setup_logging(file_name=logfile)

    logger.info("Start concatenating files")

    # # define timeframe to concat
    # timeframe = pd.date_range(
    #     start=cfg_o["start_datetime"],
    #     periods=cfg_o["total_timesteps"],
    #     freq="1h",
    # )

    results = concat_results(
        path=path,
        parameters=None,
        fillna={"value": 0},
    )

    # export_path = Path(str(path).split("_results")[0] + "_concat")
    export_path = path.parent / "concat"

    os.makedirs(export_path, exist_ok=True)

    for (grid, parameter), df in results.items():
        filename = export_path / f"{grid}_{parameter}.csv"
        df.to_csv(filename, index=True)
        logger.info(f"Save concatenated results to {filename}.")

    if version_db is not None:
        return version_db["db"]


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"result_concatenation_{date}_local.log"
    setup_logging(file_name=logfile)

    grid_id = 1111
    save_concatenated_results(
        grid_id=1111,
        path=results_dir / "debug" / str(grid_id) / "minimize_loading_feeder",
        run_id=None,
        version=None,
    )
