""""""

import os
import shutil
import warnings

from pathlib import Path

from edisgo.edisgo import import_edisgo_from_files
from edisgo.tools.complexity_reduction import extract_feeders_nx

from logger import logger
from tools import get_config, get_dir, timeit, write_metadata

config_dir = get_dir(key="config")
data_dir = get_dir(key="data")


def get_flexible_loads(
    edisgo_obj, heat_pump=False, electromobility=False, bess=False, **kwargs
):
    """

    :param edisgo_obj:
    :type edisgo_obj: eDisGo-obj
    :param heat_pump:
    :type heat_pump: bool
    :param electromobility:
    :type electromobility: bool
    :param bess:
    :type bess: bool
    :return:
    :rtype:

    Other Parameters
    ------------------
    electromobility_sectors : None or set(str)
        Specifies which electromobility sectores are flexible. By default this
        is set to None, in which case all sectors are taken.

    """
    emob_sectors_flex = kwargs.get("electromobility_sectors", ["home", "work"])

    emob_sectors = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["type"] == "charging_point", "sector"
    ].unique()
    emob_sectors_fix = [i for i in emob_sectors if i not in emob_sectors_flex]

    types = list()
    if heat_pump:
        types += ["heat_pump"]
    if electromobility:
        types += ["charging_point"]
    if bess:
        # TODO
        raise NotImplementedError()
        # types += "storages" #

    flexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["type"].isin(types)
    ]

    if electromobility:
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
        heat_pump=cfg_flexible_loads["hp"],
        electromobility=cfg_flexible_loads["emob"],
        electromobility_sectors=cfg_flexible_loads["emob_flex_sectors"],
    )

    feeders, buses_with_feeders = extract_feeders_nx(
        edisgo_obj=edisgo_obj,
        save_dir=export_path,
        only_flex_ev=only_flex_ev,
        flexible_loads=flexible_loads,
    )
    return feeders, buses_with_feeders


@timeit
def run_feeder_extraction(
    grid_id, edisgo_obj=False, save=False, doit=False, version=None
):

    logger.info(f"Extracting feeders of {grid_id}.")

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg = get_config(path=config_dir / ".grids.yaml")

    # TODO maybe remove only flex ev?
    only_flex_ev = cfg["feeder_extraction"].get("only_flex_ev")
    cfg_flexible_loads = cfg["feeder_extraction"].get("flexible_loads")

    if not edisgo_obj:
        import_dir = cfg["feeder_extraction"].get("import")
        import_path = data_dir / import_dir / str(grid_id)
        logger.info(f"Import Grid from file: {import_path}")

        edisgo_obj = import_edisgo_from_files(
            import_path,
            import_topology=True,
            import_timeseries=True,
            import_electromobility=True,
            import_heat_pump=True,
        )

    if save:
        export_dir = cfg["feeder_extraction"].get("export")
        export_path = data_dir / export_dir / str(grid_id)
        shutil.rmtree(export_path, ignore_errors=True)
        os.makedirs(export_path, exist_ok=True)

        feeders, buses_with_feeders = extract_feeders_parallel(
            edisgo_obj=edisgo_obj,
            export_path=export_path,
            only_flex_ev=only_flex_ev,
            cfg_flexible_loads=cfg_flexible_loads,
        )
        for feeder_id, feeder in enumerate(feeders):
            # TODO sth is off here. Feeder id == 0 doesnt exist? investigate!
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
            only_flex_ev=only_flex_ev,
            cfg_flexible_loads=cfg_flexible_loads,
        )

    if doit:
        return {"version": version}
    else:
        return feeders, buses_with_feeders


if __name__ == "__main__":

    from dodo import task_split_model_config_in_subconfig

    task_split_model_config_in_subconfig()
    run_feeder_extraction(grid_id=177, save=True)
