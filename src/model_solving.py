""""""
import logging
import os

from pathlib import Path

import edisgo.opf.lopf as lopf
import numpy as np
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files
from edisgo.tools.tools import convert_impedances_to_mv
from loguru import logger

from tools import get_config, get_dir

# from edisgo.tools import logger


config_dir = get_dir(key="config")
data_dir = get_dir(key="data")
results_dir = get_dir(key="results")


# logger = logging.getLogger(__name__)


def get_dnm(mvgd, feeder):
    """Get Downstream Node Matrix that describes the influence of the nodes on
    each other"""
    cfd_g = get_config(path=config_dir / ".grids.yaml")
    feeder_dir = cfd_g["dnm_generation"].get("import")

    import_path = data_dir / feeder_dir / str(mvgd) / "feeder" / str(feeder)
    file_path = import_path / f"downstream_node_matrix_{mvgd}" \
                              f"_{feeder}.csv"
    downstream_nodes_matrix = pd.read_csv(os.path.join(file_path), index_col=0)
    # TODO could be sparse?
    downstream_nodes_matrix = downstream_nodes_matrix.astype(np.uint8)
    # TODO warum hier loc? vermutlich weil anja nur eine downstream node
    #  matrix für alle hatte
    # downstream_nodes_matrix = downstream_nodes_matrix.loc[
    #     edisgo_obj.topology.buses_df.index, edisgo_obj.topology.buses_df.index
    # ]
    return downstream_nodes_matrix


# TODO
#   rolling horizon
#
def run_optimization(grid_id, feeder_id=False, edisgo_obj=False,
                     save=False, doit=False):

    cfg_g = get_config(path=config_dir / ".grids.yaml")
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    # mvgds = cfg_g["model"].get("mvgd")

    if not edisgo_obj:
        if not feeder_id:
            # TODO if feeder_id False
            raise NotImplementedError

        import_dir = cfg_g["feeder_extraction"].get("export")
        import_path = data_dir / import_dir / str(grid_id) / "feeder" / str(
            feeder_id)
        logger.info(f"Import Grid from file: {import_path}")

        edisgo_obj = import_edisgo_from_files(
            import_path,
            import_topology=True,
            import_timeseries=True,
            import_heat_pump=True,
            import_electromobility=True,
        )

    # TODO temporary workaround
    timesteps = list(range(24))
    timesteps = edisgo_obj.timeseries.timeindex[timesteps]

    # Due to different voltage levels, impedances need to adapted
    # TODO alternatively p.u.
    logger.info("Convert impedances to mv")
    edisgo_obj = convert_impedances_to_mv(edisgo_obj)

    logger.info("Downstream node matrix imported")
    downstream_nodes_matrix = get_dnm(mvgd=grid_id, feeder=feeder_id)

    logger.info("Extract time-invariant parameters")
    # Create dict with time invariant parameters
    parameters = lopf.prepare_time_invariant_parameters(
        edisgo_obj,
        downstream_nodes_matrix,
        pu=False,
        optimize_storage=cfg_o["opt_storage"],
        optimize_ev_charging=cfg_o["opt_ev"],
        optimize_hp=cfg_o["opt_hp"],
    )

    logger.info("Setup model")
    model = lopf.setup_model(
        parameters,
        timesteps=timesteps,
        objective=cfg_o["objective"],
    )

    logger.info("Optimize model")
    results = lopf.optimize(model=model, solver=cfg_o["solver"])

    export_path = results_dir / grid_id / feeder_id
    os.makedirs(export_path, exist_ok=True)
    logger.info(f"Export HP-results to: {export_path}")

    # Export HP-results
    for res_name, res in results.items():
        try:
            res = res.loc[timesteps]
        except Exception:
            # TODO handle this properly
            logger.debug(f"Exception {res_name}")
            pass
        if "slack" in res_name:
            res = res[res > 1e-6] # TODO tolerance?
            res = res.dropna(how="all")
            res = res.dropna(how="all")
        if not res.empty:
            filename = export_path / f"{res_name}.csv"
            res.astype(np.float16).to_csv(path=filename)

    # TODO
    # # TODO add results to obj
    # edisgo_obj.set_time_series_manual(
    #         loads_p=None,
    #         storage_units_p=None,
    #         generators_q=None,
    #         loads_q=None,
    #     )
    #
    # # compute q
    # edisgo_obj.set_time_series_reactive_power_control(
    #     control="fixed_cosphi",
    #     generators_parametrisation="default",
    #     loads_parametrisation="default",
    #     storage_units_parametrisation="default",)
    #
    #
    # edisgo_obj.check_integrity()
    #
    # edisgo_obj.reinforce()
    #
    # edisgo_obj.save(directory,
    #     save_topology=True,
    #     save_timeseries=True,
    #     save_results=True,
    #     save_electromobility=True,
    #     save_heatpump=True,
    #     archive=True,
    #     archive_type="zip")
    #
    #
    # # optimize again
    # # check for curtailment?
