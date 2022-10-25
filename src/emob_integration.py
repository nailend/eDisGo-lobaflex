import os

from pathlib import Path

# from edisgo.edisgo import EDisGo,
from edisgo.edisgo import import_edisgo_from_files

# from loguru import logger
from logger import logger

# from config import __path__ as config_dir
# from data import __path__ as data_dir
# from logs import __path__ as logs_dir
# from results import __path__ as results_dir
from tools import get_config, timeit

# import pandas as pd


# from src.tools import setup_logger


# data_dir = Path(data_dir[0])
# results_dir = Path(results_dir[0])
# config_dir = Path(config_dir[0])
# logs_dir = Path(logs_dir[0])

data_dir = Path("/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/data")
logs_dir = Path("/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/logs")
config_dir = Path(
    "/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/config"
)


@timeit
def run_emob_integration(edisgo_obj=False, save=False):

    cfg = get_config(Path(f"{config_dir}/model_config.yaml"))
    if not edisgo_obj:

        grid_id = cfg["model"].get("grid-id")
        import_dir = cfg["directories"]["emob_integration"].get("import")
        import_path = data_dir / import_dir / str(grid_id)
        logger.info(f"Import Grid from file: {import_path}")

        edisgo_obj = import_edisgo_from_files(
            import_path,
            import_topology=True,
            import_timeseries=True,
            import_electromobility=True,
        )

    # resample time series to have a temporal resolution of 15 minutes, which is the same
    # as the electromobility time series
    freq = "15min"
    logger.info(f"Resample timeseries to: {freq}")
    edisgo_obj.resample_timeseries(method="ffill", freq=freq)

    logger.info("Import emobility from files")
    edisgo_obj.import_electromobility(
        simbev_directory=data_dir / "simbev_results" / str(grid_id),
        tracbev_directory=data_dir / "tracbev_results" / str(grid_id),
    )

    logger.info("Calculate flexibility bands")
    flex_bands = edisgo_obj.electromobility.get_flexibility_bands(
        edisgo_obj, ["home", "work"]
    )

    if save:
        export_dir = cfg["directories"]["emob_integration"].get("export")
        export_path = data_dir / export_dir / str(grid_id)
        os.makedirs(export_path, exist_ok=True)
        edisgo_obj.save(
            export_path,
            save_topology=True,
            save_timeseries=True,
            save_electromobility=True,
        )
        logger.info(f"Saved grid to {export_path}")

        for name, df in flex_bands.items():
            df.to_csv(f"{export_dir}/{name}_flexibility_band.csv")
        logger.info(f"Flexibility bands exported to: {export_dir}")

    return edisgo_obj, flex_bands


if __name__ == "__main__":

    edisgo_obj, flex_bands = run_emob_integration(save=True)
