import logging
import os
import shutil

from datetime import datetime

from lobaflex import config_dir, data_dir, logs_dir, results_dir
from lobaflex.analysis.grid_analysis import create_grids_notebook
from lobaflex.opt.dispatch_integration import integrate_dispatch
from lobaflex.opt.dispatch_optimization import run_dispatch_optimization
from lobaflex.opt.expansion_scenario import run_expansion_scenario
from lobaflex.opt.feeder_extraction import run_feeder_extraction
from lobaflex.opt.grid_reinforcement import reinforce_grid
from lobaflex.opt.result_concatination import save_concatenated_results
from lobaflex.opt.timeframe_selection import run_timeframe_selection
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.pydoit import opt_uptodate
from lobaflex.tools.tools import get_config

logger = logging.getLogger("lobaflex.opt." + __name__)
date = datetime.now().date().isoformat()
cfg_o = get_config(path=config_dir / ".opt.yaml")
logfile = logs_dir / f"opt_tasks_{date}.log"
setup_logging(file_name=logfile)


def timeframe_selection_task(mvgd, import_path, run_id, version_db):
    """"""
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
        "doc": "per mvgd",
        "uptodate": [True] if fix else [opt_uptodate],
        "verbosity": 2,
    }


def feeder_extraction_task(mvgd, objective, source, run_id, version_db, dep):
    """Generator to define feeder extraction task for a mvgd"""

    # fix = cfg_o["fix_preparation"]
    import_path = results_dir / run_id / str(mvgd) / source
    export_path = import_path.parent / "feeder"
    return {
        "name": f"{objective}_feeder_{mvgd}",
        "actions": [
            (
                run_feeder_extraction,
                [],  # args
                {  # kwargs
                    "obj_or_path": import_path,
                    "grid_id": mvgd,
                    "objective": objective,
                    "export_path": export_path,
                    "version_db": version_db,
                    "run_id": run_id,
                },
            )
        ],
        "doc": "per mvgd",
        "task_dep": dep,
        # "uptodate": [True] if fix else [opt_uptodate],
        "uptodate": [opt_uptodate],
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


def optimization_task(
    mvgd,
    feeder,
    objective,
    rolling_horizon,
    directory,
    run_id,
    version_db,
    dep,
):
    """Generator to define optimization task for a feeder"""

    import_path = (
        results_dir / run_id / str(mvgd) / directory / f"{int(feeder):02}"
    )

    if directory.parent.parent.name == "scenarios":
        extra = directory.parent.name + "_"
    else:
        extra = ""

    return {
        "name": extra + f"{objective}_{mvgd}/{int(feeder):02}",
        "actions": [
            (
                run_dispatch_optimization,
                [],
                {
                    "obj_or_path": import_path,
                    "grid_id": mvgd,
                    "feeder_id": feeder,
                    "objective": objective,
                    "meta": directory.parent.name,
                    "rolling_horizon": rolling_horizon,
                    "version_db": version_db,
                    "run_id": run_id,
                },
            )
        ],
        "doc": "per feeder",
        "task_dep": dep,
        "uptodate": [opt_uptodate],
        "verbosity": 2,
    }


def result_concatenation_task(
    mvgd, objective, directory, run_id, version_db, dep
):
    """"""

    # TODO
    # def teardown(path):
    #     logging.info("Remove intermediate results")
    #     shutil.rmtree(path)

    path = results_dir / run_id / str(mvgd) / directory / objective / "results"

    if directory.parent.name == "potential":
        extra = directory.name + "_"
    else:
        extra = ""

    return {
        "name": extra + f"concat_{objective}_{mvgd}",
        "actions": [
            (
                save_concatenated_results,
                [],
                {
                    "grid_id": mvgd,
                    "path": path,
                    "objective": objective,
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
    import_path = results_dir / run_id / str(mvgd) / objective / "concat"

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
                    "objective": objective,
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
    if objective == "reference":
        obj_path = results_dir / run_id / str(mvgd) / "initial" / "mvgd"

    else:
        obj_path = results_dir / run_id / str(mvgd) / objective / "mvgd"

    return {
        "name": f"reinforce_{mvgd}",
        "actions": [
            (
                reinforce_grid,
                [],
                {
                    "obj_or_path": obj_path,
                    "grid_id": mvgd,
                    "objective": objective,
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


def expansion_scenario_task(mvgd, percentage, run_id, version_db, dep):
    """"""
    # obj_path = (
    #     results_dir / run_id / str(mvgd) / "minimize_loading" / "reinforced"
    # )
    obj_path = results_dir / run_id / str(mvgd) / "initial" / "mvgd"

    return {
        "name": f"{percentage}_pct_reinforced_{mvgd}",
        "actions": [
            (
                run_expansion_scenario,
                [],
                {
                    "obj_or_path": obj_path,
                    "grid_id": mvgd,
                    "percentage": percentage / 100,
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


def papermill_task(mvgd, name, template, period, run_id, version_db, dep):
    """"""

    task_name = template.rstrip(".ipynb")

    return {
        "name": f"{task_name}_{mvgd}",
        "actions": [
            (
                create_grids_notebook,
                [],
                {
                    "template": template,
                    "period": period,
                    "grid_id": mvgd,
                    "name": name,
                    # "import_dir": str(import_dir),
                    "run_id": run_id,
                    "version_db": version_db,
                    "kernel_name": os.path.basename(
                        os.environ.get("VIRTUAL_ENV")
                    ),
                },
            )
        ],
        "doc": "per mvgd",
        # create dependency for every feeder in grid not only opt
        # results if not all opt succeeded
        "task_dep": dep,
        "uptodate": [opt_uptodate],
    }


def trust_ipynb(mvgd, run_id, template, dep):
    """"""
    filename = f"{template.rstrip('.ipynb')}_{mvgd}.ipynb"
    filepath = results_dir / run_id / str(mvgd) / "analysis" / filename
    return {
        "name": f"trust_{filename}",
        "actions": [f"jupyter trust {filepath}"],
        "task_dep": [dep],
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
