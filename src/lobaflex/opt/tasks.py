import logging
import os
import shutil

from datetime import datetime

import doit

from lobaflex import config_dir, data_dir, logs_dir, results_dir
from lobaflex.opt.dispatch_integration import integrate_dispatch
from lobaflex.opt.dispatch_optimization import run_dispatch_optimization
from lobaflex.opt.dnm_generation import run_dnm_generation
from lobaflex.opt.feeder_extraction import run_feeder_extraction
from lobaflex.opt.minimal_reinforcement import reinforce_grid
from lobaflex.opt.result_concatination import save_concatinated_results
from lobaflex.opt.timeframe_selection import run_timeframe_selection
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.pydoit import opt_uptodate
from lobaflex.tools.tools import get_config

logger = logging.getLogger("lobaflex.opt." + __name__)
date = datetime.now().date().isoformat()
cfg_o = get_config(path=config_dir / ".opt.yaml")
logfile = logs_dir / f"opt_tasks_{date}.log"
setup_logging(file_name=logfile)


def timeframe_selection_task(mvgd, run_id, version_db):
    """"""
    import_dir = cfg_o["import_dir"]
    import_path = data_dir / import_dir / str(mvgd)
    fix = cfg_o["fix_preparation"]

    return {
        "name": f"timeframe_{mvgd}",
        "actions": [
            (
                run_timeframe_selection,
                [],  # args
                {  # kwargs
                    "obj_or_path": import_path,
                    "grid_id": mvgd,
                    "version_db": version_db,
                    "run_id": run_id,
                },
            )
        ],
        "uptodate": [True] if fix else [opt_uptodate],
        "verbosity": 2,
    }


def feeder_extraction_task(mvgd, run_id, version_db):
    """Generator to define feeder extraction task for a mvgd"""

    fix = cfg_o["fix_preparation"]
    import_path = results_dir / run_id / str(mvgd) / "timeframe"
    export_path = import_path.parent / "timeframe_feeder"
    return {
        "name": f"feeder_{mvgd}",
        "actions": [
            (
                run_feeder_extraction,
                [],  # args
                {  # kwargs
                    "obj_or_path": import_path,
                    "grid_id": mvgd,
                    "export_path": export_path,
                    "version_db": version_db,
                    "run_id": run_id,
                },
            )
        ],
        "task_dep": [f"prep:timeframe_{mvgd}"],
        "uptodate": [True] if fix else [opt_uptodate],
        "verbosity": 2,
    }


# def dnm_generation_task(mvgd, fix, run_id, version):
#     """Generator to define dnm generation task for a feeder or mvgd"""

#     fix = cfg_o["fix_preparation"]
#     import_path = results_dir / run_id / str(mvgd) / "timeframe_feeder"
#
#     return {
#         "name": f"dnm_{mvgd}",
#         "actions": [
#             (
#                 run_dnm_generation,
#                 [],
#                 {
#                     "path": import_path,
#                     "grid_id": mvgd,
#                     "feeder": True,
#                     "version": version,
#                     "run_id": run_id,
#                 },
#             )
#         ],
#         "task_dep": [f"prep:feeder_{mvgd}"],
#         "uptodate": [True] if fix else [opt_uptodate],
#         "verbosity": 2,
#     }


def optimization_task(mvgd, feeder, objective, run_id, version_db):
    """Generator to define optimization task for a feeder"""

    import_path = (
        results_dir
        / run_id
        / str(mvgd)
        / "timeframe_feeder"
        / f"{int(feeder):02}"
    )

    return {
        "name": f"opt_{mvgd}/{int(feeder):02}",
        "actions": [
            (
                run_dispatch_optimization,
                [],
                {
                    "obj_or_path": import_path,
                    "grid_id": mvgd,
                    "feeder_id": feeder,
                    "objective": objective,
                    "version_db": version_db,
                    "run_id": run_id,
                },
            )
        ],
        "task_dep": [f"prep:feeder_{mvgd}"],
        "uptodate": [opt_uptodate],
        "verbosity": 2,
    }


def result_concatination_task(mvgd, objective, run_id, version_db, dep):
    """"""

    def teardown(path):
        logging.info("Remove intermediate results")
        shutil.rmtree(path)

    path = results_dir / run_id / str(mvgd) / (objective + "_feeder")

    return {
        "name": f"concat_{mvgd}",
        "actions": [
            (
                save_concatinated_results,
                [],
                {
                    "grid_id": mvgd,
                    "path": path,
                    "run_id": run_id,
                    "version_db": version_db,
                },
            )
        ],
        "doc": "per mvgd",
        # create dependency for every feeder in grid not only opt
        # results if not all opt succeeded
        "task_dep": dep,
        "uptodate": [opt_uptodate],
        # "teardown": [teardown(path)],
    }


def dispatch_integration_task(mvgd, objective, run_id, version_db, dep):
    """"""
    obj_path = data_dir / cfg_o["import_dir"] / str(mvgd)
    import_path = results_dir / run_id / str(mvgd) / (objective + "_concat")

    return {
        "name": f"add_ts_{mvgd}",
        "actions": [
            (
                integrate_dispatch,
                [],
                {
                    "obj_or_path": obj_path,
                    "import_path": import_path,
                    "grid_id": mvgd,
                    "run_id": run_id,
                    "version_db": version_db,
                },
            )
        ],
        "doc": "per mvgd",
        # create dependency for every feeder in grid not only opt
        # results if not all opt succeeded
        "task_dep": dep,
        "uptodate": [opt_uptodate],
    }


def grid_reinforcement_task(mvgd, objective, run_id, version_db, dep):
    """"""
    obj_path = results_dir / run_id / str(mvgd) / (objective + "_mvgd")

    return {
        "name": f"reinforce_{mvgd}",
        "actions": [
            (
                reinforce_grid,
                [],
                {
                    "obj_or_path": obj_path,
                    "grid_id": mvgd,
                    "run_id": run_id,
                    "version_db": version_db,
                },
            )
        ],
        "doc": "per mvgd",
        # create dependency for every feeder in grid not only opt
        # results if not all opt succeeded
        "task_dep": dep,
        "uptodate": [opt_uptodate],
    }


def dot_file_task(path):

    os.makedirs(path, exist_ok=True)
    return {
        "name": "dot_file",
        "actions": [
            f"doit graph -o {path /'tasks.dot'} " f"--show-subtasks --reverse"
        ],
        "doc": "per mvgd",
    }


def png_file_task(path):

    return {
        "name": "png_file",
        "actions": [
            f"dot -T png {path / 'tasks.dot'} -o {path / 'tasks.png'}"
        ],
        "doc": "per mvgd",
        # "task_dep": dep,
    }
