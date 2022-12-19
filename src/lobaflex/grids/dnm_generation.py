""""""
import logging
import os
import warnings

import networkx as nx
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files
from edisgo.network.topology import Topology

from lobaflex import config_dir, data_dir
from lobaflex.tools.tools import get_config, timeit, write_metadata

logger = logging.getLogger("lobaflex.grids." + __name__)


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
    grid_id, feeder=False, save=False, doit=False, version=None
):

    logger.info(f"Get Downstream Node Matrix of {grid_id}")
    warnings.simplefilter(action="ignore", category=FutureWarning)
    cfg = get_config(path=config_dir / ".grids.yaml")

    import_dir = cfg["dnm_generation"].get("import")
    import_path = data_dir / import_dir / str(grid_id)

    logger.debug("Use export dir from config file.")
    export_dir = cfg["dnm_generation"].get("export")
    export_path = data_dir / export_dir / str(grid_id)

    if feeder:
        feeder_dir = import_path / "feeder"
        feeder_list = sorted(os.listdir(feeder_dir))
        logger.info(
            f"Getting downstream nodes matrices of {len(feeder_list)} feeder."
        )
        grid_dirs = [feeder_dir / str(feeder) for feeder in feeder_list]
    else:
        grid_dirs = [import_path]

    for grid_dir in grid_dirs:

        logger.info(
            f"Generate downstream node matrix. \n"
            f"Feeder {grid_dir.name} of grid:"
            f" {grid_id}"
        )

        edisgo_obj = import_edisgo_from_files(
            grid_dir,
            import_topology=True,
            import_electromobility=True,
            import_heat_pump=True,
            import_timeseries=True,
        )

        downstream_node_matrix = get_downstream_nodes_matrix_iterative(
            edisgo_obj.topology
        )

        if save:
            if feeder:
                export_path_dnm = (
                    export_path
                    / "feeder"
                    / grid_dir.name
                    / f"downstream_node_matrix_{grid_id}_{grid_dir.name}.csv"
                )

            else:
                export_path_dnm = (
                    export_path / f"downstream_node_matrix_{grid_id}.csv"
                )

            os.makedirs(export_path_dnm.parent, exist_ok=True)
            downstream_node_matrix.to_csv(export_path_dnm)
    # if save:
    #     write_metadata(
    #         export_path,
    #         edisgo_obj,
    #         text=f"Downstream Node Matrix of {int(feeder)+1} feeder",
    #     )

    if doit:
        return {"version": version}


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")

    run_dnm_generation(grid_id=1056, feeder=1, save=True)
