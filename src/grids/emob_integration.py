import os

from pathlib import Path

import pandas as pd

from edisgo.edisgo import import_edisgo_from_files

from logger import logger
from tools import get_config, get_dir, timeit, write_metadata

# import pandas as pd

logs_dir = get_dir(key="logs")
data_dir = get_dir(key="data")
config_dir = get_dir(key="config")


@timeit
def run_emob_integration(
    grid_id, edisgo_obj=False, save=False, to_freq="1h", doit=False
):

    logger.info(f"Start emob integration for {grid_id}.")
    cfg = get_config(path=config_dir / ".grids.yaml")

    if not edisgo_obj:

        import_dir = cfg["emob_integration"].get("import")
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
    freq_load = pd.Series(edisgo_obj.timeseries.timeindex).diff().min()
    if not freq_load == "15min":
        logger.info("Resample timeseries to: 15min")
        edisgo_obj.resample_timeseries(method="ffill", freq="15min")

    logger.info("Import emobility from files")
    edisgo_obj.import_electromobility(
        simbev_directory=data_dir / "simbev_results" / str(grid_id),
        tracbev_directory=data_dir / "tracbev_results" / str(grid_id),
    )

    edisgo_obj.apply_charging_strategy()

    logger.info("Calculate flexibility bands")
    flex_bands = edisgo_obj.electromobility.get_flexibility_bands(
        edisgo_obj, ["home", "work"]
    )
    # TODO Resampling flex bands not working yet @birgit
    logger.info(f"Resample timeseries to {to_freq}.")
    edisgo_obj.resample_timeseries(method="ffill", freq=to_freq)

    if save:
        export_dir = cfg["emob_integration"].get("export")
        export_path = data_dir / export_dir / str(grid_id)
        os.makedirs(export_path, exist_ok=True)
        edisgo_obj.save(
            export_path,
            save_topology=True,
            save_timeseries=True,
            save_electromobility=True,
            electromobility_attributes=[
                "integrated_charging_parks_df",
                "simbev_config_df",
                "flexibility_bands",
            ],
        )
        write_metadata(export_path, edisgo_obj)
        logger.info(f"Saved grid to {export_path}")

        # for name, df in flex_bands.items():
        #     df.to_csv(f"{export_path}/{name}_flexibility_band.csv")
        # logger.info(f"Flexibility bands exported to: {export_path}")

    if doit:
        return True
    else:
        return edisgo_obj, flex_bands


if __name__ == "__main__":

    edisgo_obj, flex_bands = run_emob_integration(grid_id=176)
