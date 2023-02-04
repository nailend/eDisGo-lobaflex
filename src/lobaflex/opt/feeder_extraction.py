""""""

import logging
import os
import shutil
import warnings

from edisgo.edisgo import EDisGo
from edisgo.edisgo import import_edisgo_from_files
from edisgo.tools.complexity_reduction import extract_feeders_nx

from lobaflex import config_dir, data_dir, results_dir
from lobaflex.tools.tools import get_config, timeit, write_metadata

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.grids." + __name__)
else:
    logger = logging.getLogger(__name__)


def get_flexible_loads(
    edisgo_obj, hp=False, ev=False, bess=False, **kwargs
):
    """

    Parameters
    ----------
    edisgo_obj :  eDisGo-obj
    hp :
    ev :
    bess :
    kwargs :
        ev_flex_sectors : None or set(str)
            Specifies which electromobility sectors are flexible. By default this
            is set to None, in which case all sectors are taken.

    Returns
    -------
    flexible_loads : pd.DataFrame

    """
    emob_sectors_flex = kwargs.get("ev_flex_sectors", ["home", "work"])

    emob_sectors = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["type"] == "charging_point", "sector"
    ].unique()
    emob_sectors_fix = [i for i in emob_sectors if i not in emob_sectors_flex]

    types = list()
    if hp:
        types += ["heat_pump"]
    if ev:
        types += ["charging_point"]
    if bess:
        # TODO
        raise NotImplementedError()
        # types += "storages" #

    flexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["type"].isin(types)
    ]

    if ev:
        flexible_loads = flexible_loads.drop(
            flexible_loads.loc[
                (flexible_loads["type"] == "charging_point")
                & (flexible_loads["sector"].isin(emob_sectors_fix))
            ].index
        )

    return flexible_loads


def extract_feeders_parallel(
    edisgo_obj, export_path, cfg_flexible_loads, **kwargs
):
    """

    Parameters
    ----------
    edisgo_obj :
    export_path :
    cfg_flexible_loads : dict

    kwargs : default = False
        only_flex_ev :

    Returns
    -------

    """

    logger.info("Get flexible loads")
    only_flex_ev = kwargs.get("only_flex_ev", False)

    flexible_loads = get_flexible_loads(
        edisgo_obj=edisgo_obj,
        bess=cfg_flexible_loads["bess"],
        hp=cfg_flexible_loads["hp"],
        ev=cfg_flexible_loads["ev"],
        ev_flex_sectors=cfg_flexible_loads["ev_flex_sectors"],
    )

    feeders, buses_with_feeders = extract_feeders_nx(
        edisgo_obj=edisgo_obj,
        save_dir=export_path,
        only_flex_ev=only_flex_ev,
        flexible_loads=flexible_loads,
    )
    return feeders, buses_with_feeders


@timeit
def run_feeder_extraction(obj_or_path, run_id=None, version=None, export_path=None):
    """

    Parameters
    ----------
    obj_or_path :
    run_id :
    version :
    export_path :

    Returns
    -------

    """
    logger.info(f"Run feeder extraction")
    # logger.info(f"Extracting feeders of {grid_id}.")

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    cfg_flexible_loads = cfg_o["flexible_loads"]

    if isinstance(obj_or_path, EDisGo):
        edisgo_obj = obj_or_path
    else:
        # import_dir = cfg["feeder_extraction"].get("import")
        # import_path = data_dir / import_dir / str(grid_id)
        # import_path = results_dir
        # logger.info(f"Import Grid from file: {import_path}")
        logger.info(f"Import Grid from file: {obj_or_path}")

        edisgo_obj = import_edisgo_from_files(
            edisgo_path=obj_or_path,
            import_topology=True,
            import_timeseries=True,
            import_electromobility=True,
            import_heat_pump=True,
        )

    grid_id = edisgo_obj.topology.mv_grid.id
    logger.info(f"Extracting feeders of {grid_id}.")

    if export_path is not None:
        # export_dir = cfg_o["feeder_extraction"].get("export")
        # export_path = data_dir / export_dir / str(grid_id)
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

    if version and run_id is not None:
        return {"version": version, "run_id": run_id}
    else:
        return feeders, buses_with_feeders


if __name__ == "__main__":

    from datetime import datetime

    from lobaflex import logs_dir
    from lobaflex.tools.logger import setup_logging
    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"feeder_extraction_{date}_local.log"
    setup_logging(file_name=logfile)

    path = "/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/data/load_n_gen_n_emob_n_hp_grids/1111"
    run_feeder_extraction(obj_or_path=path, run_id=2,
                          export_path=results_dir / "test" / str(1111) /
                                        "feeder")
