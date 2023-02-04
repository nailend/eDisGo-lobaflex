import logging

from datetime import datetime

import doit

from lobaflex import config_dir, logs_dir
from lobaflex.opt.dnm_generation import run_dnm_generation
from lobaflex.opt.feeder_extraction import run_feeder_extraction
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.pydoit import opt_uptodate
from lobaflex.tools.tools import get_config

logger = logging.getLogger("lobaflex.opt." + __name__)
date = datetime.now().date().isoformat()
cfg_o = get_config(path=config_dir / ".opt.yaml")
logfile = logs_dir / f"opt_tasks_{date}.log"
setup_logging(file_name=logfile)


def feeder_extraction_task(
    mvgd, fix, run_id, version, import_path, export_path
):
    """Generator to define feeder extraction task for a mvgd"""
    # cfg = get_config(path=config_dir / ".grids.yaml")
    # dep_manager = doit.Globals.dep_manager
    # grids_version = dep_manager.get_result("_set_grids_version")["version"]

    return {
        "name": f"feeder_{mvgd}",
        "actions": [
            (
                run_feeder_extraction,
                [],  # args
                {  # kwargs
                    "obj_or_path": import_path,
                    "export_path": export_path,
                    "version": version,
                    "run_id": run_id,
                },
            )
        ],
        # "task_dep": [f"grids:{mvgd}_hp_integration"],
        # take current version number of dataset
        # "getargs": {"version": ("_get_grids_version", "version")},
        "uptodate": [True] if fix else [opt_uptodate],
        "verbosity": 2,
    }


def dnm_generation_task(mvgd, fix, run_id, version, path):
    """Generator to define dnm generation task for a feeder or mvgd"""
    # cfg = get_config(path=config_dir / ".grids.yaml")

    yield {
        "name": f"dnm_{mvgd}",
        "actions": [
            (
                run_dnm_generation,
                [],
                {
                    "path": path,
                    "grid_id": mvgd,
                    "doit": True,
                    "feeder": True,
                    "version": version,
                    "run_id": run_id,
                },
            )
        ],
        "task_dep": [f"prep:feeder_{mvgd}"],
        # take current version number of dataset
        # "getargs": {"version": ("_get_grids_version", "version")},
        "uptodate": [True] if fix else [opt_uptodate],
        "verbosity": 2,
    }
