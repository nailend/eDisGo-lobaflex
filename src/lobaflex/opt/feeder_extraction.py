""""""

import logging
import os
import shutil
import warnings

from datetime import datetime

from edisgo.edisgo import EDisGo, import_edisgo_from_files
from edisgo.tools.complexity_reduction import extract_feeders_nx

from lobaflex import config_dir, logs_dir, results_dir
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, timeit, write_metadata

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.grids." + __name__)
else:
    logger = logging.getLogger(__name__)


def get_flexible_loads(edisgo_obj, hp=False, bev=False, bess=False, **kwargs):
    """Identifies flexible loads in the edisgo object and selects them from
    edisgo.topology.loads_df.

    Parameters
    ----------
    edisgo_obj : :class:`edisgo.EDisGo`
        EDisGo object
    hp : bool
        Include heat pumps
    bev : bool
        Include battery electric vehicles
    bess : bool
        Include battery energy storage systems
    kwargs :
        bev_flex_sectors : None or set(str)
            Specifies which electromobility sectors are flexible. By default
            this is set to None, in which case all sectors are taken.

    Returns
    -------
    flexible_loads : pd.DataFrame

    """
    emob_sectors_flex = kwargs.get("bev_flex_sectors", ["home", "work"])

    emob_sectors = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["type"] == "charging_point", "sector"
    ].unique()
    emob_sectors_fix = [i for i in emob_sectors if i not in emob_sectors_flex]

    types = list()
    if hp:
        types += ["heat_pump"]
    if bev:
        types += ["charging_point"]
    if bess:
        # TODO
        raise NotImplementedError()
        # types += "storages" #

    flexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["type"].isin(types)
    ]

    if bev:
        flexible_loads = flexible_loads.drop(
            flexible_loads.loc[
                (flexible_loads["type"] == "charging_point")
                & (flexible_loads["sector"].isin(emob_sectors_fix))
            ].index
        )

    return flexible_loads


def extract_feeders_parallel(
    edisgo_obj,
    export_path,
    cfg_flexible_loads,
):
    """Identifies flexible loads in the edisgo object and extracts feeders.
    Currently not dropping timeseries of flexible loads, keeping all. But
    flexbands only exist for flexible bevs.

    Parameters
    ----------
    edisgo_obj : :class:`edisgo.EDisGo`
        EDisGo object
    export_path : PosixPath
        Path to export feeders to
    cfg_flexible_loads : dict
        Flexible loads configuration


    Returns
    -------
    feeders: list of edisgo_obj, buses_with_feeder: pandas.DataFrame
    """

    logger.info("Get flexible loads")

    flexible_loads = get_flexible_loads(
        edisgo_obj=edisgo_obj,
        bess=cfg_flexible_loads["bess"],
        hp=cfg_flexible_loads["hp"],
        bev=cfg_flexible_loads["bev"],
        bev_flex_sectors=cfg_flexible_loads["bev_flex_sectors"],
    )

    feeders, buses_with_feeders = extract_feeders_nx(
        edisgo_obj=edisgo_obj,
        export_path=export_path,
        flexible_loads=flexible_loads,
    )
    return feeders, buses_with_feeders


@timeit
def run_feeder_extraction(
    obj_or_path,
    grid_id,
    export_path=None,
    run_id=None,
    version_db=None
    # obj_or_path, grid_id, version_db
):
    """

    Parameters
    ----------
    obj_or_path : :class:`edisgo.EDisGo` or PosixPath
        edisgo object or path to edisgo dump
    grid_id : int or str
        grid id of MVGD
    export_path : PosixPath or None
        Path to export feeders to, if non given feeders are not exported
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
    logger.info(f"Run feeder extraction of {grid_id}")

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    cfg_flexible_loads = cfg_o["flexible_loads"]

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"feeder_extraction_{run_id}_{grid_id}_{date}.log"
    setup_logging(file_name=logfile)

    if isinstance(obj_or_path, EDisGo):
        edisgo_obj = obj_or_path
    else:
        logger.info(f"Import Grid from file: {obj_or_path}")

        edisgo_obj = import_edisgo_from_files(
            edisgo_path=obj_or_path,
            import_topology=True,
            import_timeseries=True,
            import_electromobility=True,
            import_heat_pump=True,
        )

    logger.info(f"Extract feeders of {grid_id}.")

    if export_path is not None:

        shutil.rmtree(export_path, ignore_errors=True)
        os.makedirs(export_path, exist_ok=True)

        feeders, buses_with_feeders = extract_feeders_parallel(
            edisgo_obj=edisgo_obj,
            export_path=export_path,
            cfg_flexible_loads=cfg_flexible_loads,
        )
        for feeder_id, feeder in enumerate(feeders):
            # TODO sth is off here. Feeder id == 0 doesnt exist? investigate!
            #     might be hvmv station
            try:
                # meta_path = export_path / "feeder" / str(feeder_id + 1)
                folder = f"{feeder_id+1:02}"
                meta_path = export_path / "feeder" / folder
                write_metadata(meta_path, edisgo_obj=feeder)
            except Exception:
                logger.debug(
                    f"No feeder folder {feeder_id} found for metadata"
                )
        write_metadata(export_path, edisgo_obj=edisgo_obj)

    else:
        feeders, buses_with_feeders = extract_feeders_parallel(
            edisgo_obj=edisgo_obj,
            export_path=False,
            cfg_flexible_loads=cfg_flexible_loads,
        )

    if version_db is not None:
        return version_db["db"]

    else:
        return feeders, buses_with_feeders


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"feeder_extraction_{date}_local.log"
    setup_logging(file_name=logfile)

    path = "/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/data/load_n_gen_n_emob_n_hp_grids/1111"
    run_feeder_extraction(
        obj_or_path=path,
        grid_id=1111,
        run_id="test",
        export_path=results_dir / "test" / str(1111) / "feeder",
    )
