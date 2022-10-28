import os

from pathlib import Path

from edisgo.edisgo import import_edisgo_from_files

from logger import logger
from tools import get_config, get_dir, timeit

# import pandas as pd

logs_dir = get_dir(key="logs")
data_dir = get_dir(key="data")
config_dir = get_dir(key="config")


@timeit
def run_emob_integration(edisgo_obj=False, grid_id=False, save=False, freq="1h"):

    cfg = get_config(Path(f"{config_dir}/model_config.yaml"))
    if not grid_id:
        grid_id = cfg["model"].get("grid-id")

    if not edisgo_obj:

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

    logger.info(f"Resample timeseries to {freq}.")
    edisgo_obj.resample_timeseries(method="ffill", freq=freq)

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
            df.to_csv(f"{export_path}/{name}_flexibility_band.csv")
        logger.info(f"Flexibility bands exported to: {export_path}")

    return edisgo_obj, flex_bands


if __name__ == "__main__":

    edisgo_obj, flex_bands = run_emob_integration(save=True)
