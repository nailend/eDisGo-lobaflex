""""""

import os
import warnings

from pathlib import Path

from edisgo.edisgo import import_edisgo_from_files
from edisgo.tools.complexity_reduction import extract_feeders_nx

from logger import logger
from tools import get_config, get_dir, timeit, write_metadata

config_dir = get_dir(key="config")
data_dir = get_dir(key="data")


def extract_feeders_parallel(
    edisgo_obj, export_path, flexible_loads, only_flex_ev=False
):

    # filter flexible loads: heat_pump, charging_point [home, work]
    if flexible_loads:
        flexible_loads = edisgo_obj.topology.loads_df.loc[
            edisgo_obj.topology.loads_df["type"].isin(["heat_pump", "charging_point"])
        ]

        flexible_loads = flexible_loads.drop(
            flexible_loads.loc[
                (flexible_loads["type"] == "charging_point")
                & (flexible_loads["sector"] == "public")
            ].index
        )
        flexible_loads = flexible_loads.index.to_list()

    feeders, buses_with_feeders = extract_feeders_nx(
        edisgo_obj=edisgo_obj,
        save_dir=export_path,
        only_flex_ev=only_flex_ev,
        flexible_loads=flexible_loads,
    )
    return feeders, buses_with_feeders


@timeit
def run_feeder_extraction(grid_id, edisgo_obj=False, save=False, doit=False):

    logger.info(f"Extracting feeders of {grid_id}.")

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg = get_config(path=config_dir / "model_config.yaml")

    only_flex_ev = cfg["grid_generation"]["feeder_extraction"].get("only_flex_ev")
    flexible_loads = cfg["grid_generation"]["feeder_extraction"].get("flexible_loads")

    if not edisgo_obj:
        import_dir = cfg["grid_generation"]["feeder_extraction"].get("import")
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
        # if isinstance(targets, Path):
        #     logger.debug("Use export dir given as parameter.")
        #     export_path = targets
        # elif isinstance(targets, str):
        #     export_path = Path(targets)
        # else:
        logger.debug("Use export dir from config file.")
        export_dir = cfg["grid_generation"]["feeder_extraction"].get("export")
        export_path = data_dir / export_dir / str(grid_id)
        os.makedirs(export_path, exist_ok=True)

        feeders, buses_with_feeders = extract_feeders_parallel(
            edisgo_obj=edisgo_obj,
            export_path=export_path,
            only_flex_ev=only_flex_ev,
            flexible_loads=flexible_loads,
        )
        for feeder_id, feeder in enumerate(feeders):
            write_metadata(export_path / "feeder" / str(feeder_id),
                           edisgo_obj=feeder)
        write_metadata(export_path, edisgo_obj=edisgo_obj)

    else:
        feeders, buses_with_feeders = extract_feeders_parallel(
            edisgo_obj=edisgo_obj,
            export_path=False,
            only_flex_ev=only_flex_ev,
            flexible_loads=flexible_loads,
        )

    if doit:
        return True
    else:
        return feeders, buses_with_feeders


if __name__ == "__main__":

    run_feeder_extraction(grid_id=176)
