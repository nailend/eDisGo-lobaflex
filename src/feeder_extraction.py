""""""
import multiprocessing as mp
import os
import traceback
import warnings

from pathlib import Path

import networkx as nx
import pandas as pd

from edisgo.edisgo import import_edisgo_from_files

# from edisgo.network.electromobility import get_energy_bands_for_optimization
from edisgo.network.timeseries import TimeSeries
from edisgo.network.topology import Topology
from edisgo.opf.lopf import import_flexibility_bands
from edisgo.tools.complexity_reduction import (
    extract_feeders_nx,
    remove_1m_lines_from_edisgo,
)

from logger import logger
from tools import get_config, get_dir, timeit

config_dir = get_dir(key="config")
data_dir = get_dir(key="data")

# Script to prepare grids for optimisation. The necessary steps are:
# Timeseries: Extract extreme weeks
# Topology: Remove 1m lines, extract feeders, extract downstream nodes matrix


def remove_1m_lines_from_edisgo_parallel(import_path):

    edisgo = import_edisgo_from_files(import_path)
    no_bus_pre = len(edisgo.topology.buses_df)
    no_line_pre = len(edisgo.topology.lines_df)
    logger.info(
        "Grid has {} buses and {} lines before reduction".format(
            no_bus_pre, no_line_pre
        )
    )
    edisgo = remove_1m_lines_from_edisgo(edisgo)
    no_bus_after = len(edisgo.topology.buses_df)
    no_line_after = len(edisgo.topology.lines_df)
    logger.info(
        "Grid has {} buses and {} lines after reduction".format(
            no_bus_after, no_line_after
        )
    )
    logger.info(
        "{} buses and {} lines removed".format(
            no_bus_pre - no_bus_after, no_bus_pre - no_line_after
        )
    )
    os.renames(old=import_path / "topology", new=import_path / "topology_1m_lines")
    edisgo.topology.to_csv(import_path)


def extract_feeders_parallel(
    import_path, export_path, only_flex_ev: bool, flexible_loads: bool
):

    # try:

    edisgo_obj = import_edisgo_from_files(
        import_path,
        import_topology=True,
        import_timeseries=True,
        import_electromobility=True,
        import_heat_pump=True,
    )

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

    feeders = extract_feeders_nx(
        edisgo_obj=edisgo_obj,
        save_dir=export_path,
        only_flex_ev=only_flex_ev,
        flexible_loads=flexible_loads,
    )
    # except Exception as e:
    #     logger.info("Problem in grid {}.".format(grid_id))
    #     logger.info(e)


def get_downstream_node_matrix_feeders_parallel_server(
    import_path, export_path, grid_id_feeder_tuple
):
    grid_id = grid_id_feeder_tuple[0]
    feeder_id = grid_id_feeder_tuple[1]

    if os.path.isfile(
        export_path / f"downstream_node_matrix_{grid_id}_{feeder_id}.csv"
    ):
        return
    try:
        edisgo_obj = import_edisgo_from_files(
            import_path,
            import_electromobility=True,
            import_heat_pump=True,
            import_timeseries=True,
        )
        downstream_node_matrix = get_downstream_nodes_matrix_iterative(
            edisgo_obj.topology
        )
        downstream_node_matrix.to_csv(
            export_path / f"downstream_node_matrix_{grid_id}_{feeder_id}.csv"
        )
    except Exception as e:
        logger.info("Problem in feeder {} of grid {}.".format(feeder_id, grid_id))
        logger.info(e.args)
        logger.info(e)
    return


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
def run_feeder_extraction():

    warnings.simplefilter(action="ignore", category=FutureWarning)
    only_flex_ev = False
    use_mp = False
    remove_1m_lines = False
    # extract_bands = False
    extract_feeders = True
    flexible_loads = True
    get_downstream_node_matrix = True
    cpu_count = 1
    # cpu_count = int(mp.cpu_count()/2)

    cfg = get_config(Path(f"{config_dir}/model_config.yaml"))
    grid_id = cfg["model"].get("grid-id")
    grid_ids = [grid_id]

    import_dir = cfg["directories"]["feeder_extraction"].get("import")
    import_path = data_dir / import_dir / str(grid_id)
    export_dir = cfg["directories"]["feeder_extraction"].get("export")
    export_path = data_dir / export_dir / str(grid_id)
    os.makedirs(export_path, exist_ok=True)

    if cpu_count > 1:
        # pool = mp.Pool(cpu_count)
        # if remove_1m_lines:
        #     logger.info("Removing 1m lines")
        #     pool.map_async(remove_1m_lines_from_edisgo, grid_ids).get()
        # if extract_feeders:
        #     logger.info("Extracting feeders.")
        #     pool.map_async(
        #         extract_feeders_parallel, grid_ids, only_flex_ev, flexible_loads
        #     ).get()
        # if get_downstream_node_matrix:
        #     logger.info("Getting downstream nodes matrices")
        #     grid_id_feeder_tuples = []
        #     for grid_id in grid_ids:
        #         feeder_dir = import_path / "feeder"
        #         for feeder in os.listdir(feeder_dir):
        #             grid_id_feeder_tuples.append((grid_id, feeder))
        #     pool.map_async(
        #         get_downstream_node_matrix_feeders_parallel_server,
        #         grid_id_feeder_tuples,
        #     ).get()
        # pool.close()
        pass
    else:
        for grid_id in grid_ids:
            logger.info("Preparing grid {}".format(grid_id))
            if remove_1m_lines:
                logger.info("Removing 1m lines")
                remove_1m_lines_from_edisgo_parallel(import_path)
            if extract_feeders:
                logger.info("Extracting feeders.")
                extract_feeders_parallel(
                    import_path,
                    export_path,
                    only_flex_ev,
                    flexible_loads=flexible_loads,
                )
            if get_downstream_node_matrix:
                logger.info("Getting downstream nodes matrices")
                feeder_dir = export_path / "feeder"
                for feeder in sorted(os.listdir(feeder_dir)):
                    logger.info(f"Feeder: {feeder} of grid: {grid_id}")
                    get_downstream_node_matrix_feeders_parallel_server(
                        import_path=feeder_dir / str(feeder),
                        export_path=export_path,
                        grid_id_feeder_tuple=(grid_id, feeder),
                    )


if __name__ == "__main__":

    run_feeder_extraction()
