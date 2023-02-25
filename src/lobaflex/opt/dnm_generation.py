""""""
import logging
import os
import warnings

from datetime import datetime
from pathlib import Path

import networkx as nx
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files
from edisgo.network.topology import Topology

from lobaflex import config_dir, logs_dir
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, timeit  # , write_metadata

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.grids." + __name__)
else:
    logger = logging.getLogger(__name__)


def get_downstream_nodes_matrix_iterative(grid):
    """
    Method that returns matrix M with 0 and 1 entries describing the relation
    of buses within the network. If bus b is descendant of a (assuming the
    station is the root of the radial network) M[a,b] = 1, otherwise M[a,b] = 0.
    The matrix is later used to determine the power flow at the different buses
    by multiplying with the nodal power flow. S_sum = M * s, where s is the
    nodal power vector.

    Note: only works for radial networks.

    :param grid: either Topology, MVGrid or LVGrid
    :return:
    Todo: Check version with networkx successor
    """

    def recursive_downstream_node_matrix_filling(
        current_bus,
        current_feeder,
        downstream_node_matrix,
        grid,
        visited_buses,
    ):
        current_feeder.append(current_bus)
        for neighbor in tree.successors(current_bus):
            if (
                neighbor not in visited_buses
                and neighbor not in current_feeder
            ):
                recursive_downstream_node_matrix_filling(
                    neighbor,
                    current_feeder,
                    downstream_node_matrix,
                    grid,
                    visited_buses,
                )
        # current_bus = current_feeder.pop()
        downstream_node_matrix.loc[current_feeder, current_bus] = 1
        visited_buses.append(current_bus)
        # if len(visited_buses) % 10 == 0:
        #     logger.info(
        #         "{} % of the buses have been checked".format(
        #             len(visited_buses) / len(buses) * 100
        #         )
        #     )
        current_feeder.pop()

    buses = grid.buses_df.index.values
    if str(type(grid)) == str(Topology):
        graph = grid.to_graph()
        slack = grid.mv_grid.station.index[0]
    else:
        graph = grid.graph
        slack = grid.transformers_df.bus1.iloc[0]
    tree = nx.bfs_tree(graph, slack)

    logger.info("Matrix for {} buses is extracted.".format(len(buses)))
    downstream_node_matrix = pd.DataFrame(columns=buses, index=buses)
    downstream_node_matrix.fillna(0, inplace=True)

    logger.info("Starting iteration.")
    visited_buses = []
    current_feeder = []

    recursive_downstream_node_matrix_filling(
        slack, current_feeder, downstream_node_matrix, grid, visited_buses
    )

    return downstream_node_matrix


@timeit
def run_dnm_generation(
    path, grid_id, feeder=False, run_id=None, version_db=None
):
    """

    Parameters
    ----------
    path : PosixPath
        Path to the grid/feeder
    grid_id : int
        Grid id of the MVGD
    feeder : bool
        If true generates dnm matrix for all feeders in path
    run_id : str
        run id used for pydoit versioning
    version_db : dict
        Dictionary with version information for pydoit versioning

    Returns
    -------
    If run_id and version are not None, a dictionary with these values is
    given for the pydoit versioning.


    """

    logger.info(f"Get Downstream Node Matrix of {grid_id}")
    warnings.simplefilter(action="ignore", category=FutureWarning)

    if feeder:
        feeder_list = sorted(os.listdir(path))
        feeder_list = [i for i in feeder_list if "md" not in i]
        logger.info(
            f"Getting downstream nodes matrices of {len(feeder_list)} feeder."
        )
        grid_dirs = [path / str(feeder) for feeder in feeder_list]
    else:
        grid_dirs = [path]

    for grid_dir in grid_dirs:

        logger.info(f"Feeder {grid_dir.name} of grid: {grid_id}")

        edisgo_obj = import_edisgo_from_files(
            grid_dir,
            import_topology=True,
            import_electromobility=True,
            import_heat_pump=True,
            import_timeseries=True,
            # TODO ass results?!
        )

        downstream_node_matrix = get_downstream_nodes_matrix_iterative(
            edisgo_obj.topology
        )

        if feeder is True:
            export_path_dnm = (
                grid_dir
                / f"downstream_node_matrix_{grid_id}_{grid_dir.name}.csv"
            )
        else:
            export_path_dnm = (
                grid_dir / f"downstream_node_matrix_{grid_id}.csv"
            )

        os.makedirs(export_path_dnm.parent, exist_ok=True)
        downstream_node_matrix.to_csv(export_path_dnm)
    # if save:
    #     write_metadata(
    #         export_path,
    #         edisgo_obj,
    #         text=f"Downstream Node Matrix of {int(feeder)+1} feeder",
    #     )

    if version_db is not None:
        return version_db["db"]


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"dnm_generation{date}_local.log"
    setup_logging(file_name=logfile)

    run_dnm_generation(
        path=Path(
            "/home/local/RL-INSTITUT/julian.endres/Projekte/eDisGo-lobaflex/results/test/1111"
        ),
        grid_id=1111,
        feeder=True,
        doit=False,
        version=1,
    )
