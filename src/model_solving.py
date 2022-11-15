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
# import eDisGo_lobaflex as loba
from feeder_extraction import get_flexible_loads
from tools import get_config, get_dir

# from edisgo.tools import logger


config_dir = get_dir(key="config")
data_dir = get_dir(key="data")
results_dir = get_dir(key="results")


# logger = logging.getLogger(__name__)

def get_dnm(mvgd, feeder):
    """
    Get Downstream Node Matrix that describes the influence of the nodes on
    each other

    Parameters
    ----------
    mvgd : int
        MVGD id
    feeder : int
        Feeder id

    Returns
    -------
    pd.DataFrame
        Downstream node matrix
    """
    cfd_g = get_config(path=config_dir / ".grids.yaml")
    feeder_dir = cfd_g["dnm_generation"].get("import")
    feeder = f"{int(feeder):02}"

    import_path = data_dir / feeder_dir / str(mvgd) / "feeder" / str(feeder)
    # TODO dirty quick fix
    # file_path = import_path / f"downstream_node_matrix_{mvgd}" \
    #                           f"_{feeder}.csv"
    file_path = import_path / f"downstream_node_matrix_{mvgd}_{feeder}.csv"
    downstream_nodes_matrix = pd.read_csv(os.path.join(file_path), index_col=0)
    # TODO could be sparse?
    downstream_nodes_matrix = downstream_nodes_matrix.astype(np.uint8)
    # TODO warum hier loc? vermutlich weil anja nur eine downstream node
    #  matrix für alle hatte
    # downstream_nodes_matrix = downstream_nodes_matrix.loc[
    #     edisgo_obj.topology.buses_df.index, edisgo_obj.topology.buses_df.index
    # ]
    return downstream_nodes_matrix


def export_results(results, path):
    """

    Parameters
    ----------
    results : dict
        Results of the optimiziation
    path : PosixPath
        Export path for results
    """

    os.makedirs(path, exist_ok=True)
    logger.info(f"Export HP-results to: {path}")

    # Export HP-results
    for res_name, res in results.items():
        try:
            res = res.loc[timesteps]
        except Exception:
            # TODO handle this properly
            logger.debug(f"Exception {res_name}")
            pass
        if "slack" in res_name:
            res = res[res > 1e-6]  # TODO tolerance?
            res = res.dropna(how="all")
            res = res.dropna(how="all")
        if not res.empty:
            filename = path / f"{res_name}.csv"
            res.astype(np.float16).to_csv(filename, mode="w")
            # res.astype(np.float16).to_csv(filename, header=False, mode="a")



# TODO
#   rolling horizon
#
def run_optimization(grid_id, feeder_id=False, edisgo_obj=False,
                     save=False, doit=False):
    """

    Parameters
    ----------
    grid_id :
    feeder_id :
    edisgo_obj :
    save :
    doit :

    Returns
    -------

    """

    cfg_g = get_config(path=config_dir / ".grids.yaml")
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    # mvgds = cfg_g["model"].get("mvgd")
    feeder_id = f"{int(feeder_id):02}"

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
    timesteps = list(range(24*8+1))
    timesteps = edisgo_obj.timeseries.timeindex[timesteps]


    # Due to different voltage levels, impedances need to adapted
    # TODO alternatively p.u.
    logger.info("Convert impedances to mv")
    edisgo_obj = convert_impedances_to_mv(edisgo_obj)

    logger.info("Downstream node matrix imported")
    downstream_nodes_matrix = get_dnm(mvgd=grid_id, feeder=int(feeder_id))

    logger.info("Get flexible loads")
    flexible_loads = get_flexible_loads(
        edisgo_obj=edisgo_obj,
        heat_pump=cfg_o["opt_hp"],
        electromobility=cfg_o["opt_emob"],
        bess=cfg_o["opt_bess"],
        electromobility_sectors=cfg_o["emob_sectors"],
    )

    logger.info("Extract time-invariant parameters")
    # Create dict with time invariant parameters
    parameters = lopf.prepare_time_invariant_parameters(
        edisgo_obj,
        downstream_nodes_matrix,
        pu=False,
        optimize_storage=cfg_o["opt_bess"],
        optimize_ev_charging=cfg_o["opt_emob"],
        optimize_hp=cfg_o["opt_hp"],
        flexible_loads=flexible_loads
    )

    # energy_level = {}
    # charging_hp = {}
    # charging_tes = {}
    if cfg_o["rolling_horizon"]:
        t_max = cfg_o["timesteps_per_iteration"]
        # interval_start = timesteps[::t_max]
        # interval_end = timesteps[t_max-1:][::t_max]
        # interval_end = interval_end.append(timesteps[-1:])
        # [timesteps[start:end] for start, end in zip(interval_start,
        #                                             interval_end)]

        interval_start = list(range(0, len(timesteps), t_max))
        interval_end = list(range(t_max-1, len(timesteps), t_max))
        if len(timesteps) % t_max > 0:
            interval_end += [len(timesteps)]

        [timesteps[start:end] for start, end in zip(interval_start,
                                                    interval_end)]




    logger.info("Setup model")
    model = lopf.setup_model(
        timeinvariant_parameters=parameters,
        timesteps=timesteps,
        objective=cfg_o["objective"],
        optimize_storage=cfg_o["opt_bess"],
        optimize_ev_charging=cfg_o["opt_ev"],
        optimize_hp=cfg_o["opt_hp"],
        # charging_start_hp=charging_start,
        # energy_level_start_tes=energy_level_start,
        # energy_level_end_tes=energy_level_end,
        # **kwargs,
    )






    logger.info("Optimize model")
    results = lopf.optimize(model=model, solver=cfg_o["solver"])


    export_path = results_dir / str(grid_id) / str(feeder_id) / "results" / \
                  "powerflow_results"
    export_results(results=results, path=export_path)

    # list_attr = ["charging_hp_el",
    #              "charging_tes",
    #              "energy_tes"
    #              ]
    # loads_p = pd.DataFrame()
    # if cfg_o["opt_hp"]:
    #     loads_p = pd.concat([loads_p, results["charging_hp_el"]], axis=1)
    # if cfg_o["opt_ev"]:
    #     loads_p = pd.concat([loads_p, results["charging_hp_el"]], axis=1)
    # if cfg_p
    #
    # # storage_units_p = results[""]
    #
    # edisgo_obj.set_time_series_manual()
    #         loads_p=None,
    #         storage_units_p=None,
    #         generators_q=None,

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
