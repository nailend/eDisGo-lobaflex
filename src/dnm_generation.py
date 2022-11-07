""""""
import os
import warnings

from pathlib import Path

import networkx as nx
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files
from edisgo.network.topology import Topology

from logger import logger
from tools import get_config, get_dir, timeit, write_metadata

config_dir = get_dir(key="config")
data_dir = get_dir(key="data")


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
        current_bus, current_feeder, downstream_node_matrix, grid, visited_buses
    ):
        current_feeder.append(current_bus)
        for neighbor in tree.successors(current_bus):
            if neighbor not in visited_buses and neighbor not in current_feeder:
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
def run_dnm_generation(grid_id, save=False, doit=False):

    logger.info(f"Extracting feeders of {grid_id}.")

    warnings.simplefilter(action="ignore", category=FutureWarning)

    cfg = get_config(path=config_dir / "model_config.yaml")

    import_dir = cfg["grid_generation"]["dnm_generation"].get("import")
    import_path = data_dir / import_dir / str(grid_id)

    # if isinstance(targets, Path):
    #     logger.debug("Use export dir given as parameter.")
    #     export_path = targets
    # elif isinstance(targets, str):
    #     logger.debug("Use export dir given as parameter.")
    #     export_path = Path(targets)
    # else:
    logger.debug("Use export dir from config file.")
    export_dir = cfg["grid_generation"]["dnm_generation"].get("export")
    export_path = data_dir / export_dir / str(grid_id)

    feeder_dir = import_path / "feeder"
    feeder_list = sorted(os.listdir(feeder_dir))
    logger.info(f"Getting downstream nodes matrices of {len(feeder_list)} "
                f"feeder.")
    for feeder in sorted(os.listdir(feeder_dir)):

        logger.info(
            f"Generate downstream node matrix. \n"
            f"Feeder {feeder} of grid:"
            f" {grid_id}"
        )

        edisgo_obj = import_edisgo_from_files(
            feeder_dir / str(feeder),
            import_topology=True,
            import_electromobility=True,
            import_heat_pump=True,
            import_timeseries=True,
        )

        # if os.path.isfile(export_path / f"downstream_node_matrix_{grid_id}"
        #                                 f"_{feeder}.csv"):
        #     continue

        downstream_node_matrix = get_downstream_nodes_matrix_iterative(
            edisgo_obj.topology
        )

        if save:
            # if isinstance(targets, Path):
            #     export_path = targets
            # elif isinstance(targets, str):
            #     export_path = Path(targets)
            # else:
            export_dir = cfg["grid_generation"]["feeder_extraction"].get("export")
            export_path = data_dir / export_dir / str(grid_id)
            os.makedirs(export_path, exist_ok=True)

            downstream_node_matrix.to_csv(
                export_path / f"downstream_node_matrix_{grid_id}_{feeder}.csv"
            )
        if save:
            write_metadata(
                export_path,
                edisgo_obj,
                text=f"Downstream Node Matrix of {feeder+1} feeder",
            )

    if doit:
        return True


if __name__ == "__main__":

    run_dnm_generation(grid_id=176)
