import logging

from load_integration import run_load_integration
from datetime import datetime

from lobaflex import config_dir
from lobaflex.grids.dnm_generation import run_dnm_generation
from lobaflex.grids.emob_integration import run_emob_integration
from lobaflex.grids.feeder_extraction import run_feeder_extraction
from lobaflex.grids.hp_integration import run_hp_integration

# the following task are used by pydoit task generator
# fmt: off
from lobaflex.tools.pydoit import task__get_grids_version  # noqa: F401
from lobaflex.tools.pydoit import task__set_grids_version  # noqa: F401
from lobaflex.tools.pydoit import task__split_model_config_in_subconfig  # noqa: F401
from lobaflex.tools.pydoit import grids_uptodate
from lobaflex.tools.tools import TelegramReporter, get_config
from lobaflex.tools.logger import setup_logging
from lobaflex import logs_dir

logger = logging.getLogger("lobaflex.grids." + __name__)
date = datetime.now().date().isoformat()
cfg_o = get_config(path=config_dir / ".opt.yaml")
logfile = logs_dir / f"grids_dodo_{date}.log"
setup_logging(file_name=logfile)

# fmt: on

# TODO
#   4. alternative uptodate function: version,
#   6. clean
#   7. callback telegram bot
#   8. watch param
#   9. Check connection to db, maybe at beginning and raise warning

DOIT_CONFIG = {"default_tasks": ["grids"], "reporter": TelegramReporter}


# def task__init_dataset_version():
#     cfg = get_config(path=config_dir / ".grids.yaml")
#     dep_manager = doit.Globals.dep_manager
#     dataset_results = dep_manager.get_result("_set_dataset_version")
#     if dataset_results is None:
#         dep_manager.set(task_id="_set_dataset_version",
#                         value=0)


def load_integration_task(mvgd):
    """Generator to define load integration task for a mvgd"""

    yield {
        "name": f"{mvgd}_load_integration",
        "actions": [
            (
                run_load_integration,
                [],  # args
                {  # kwargs
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        "uptodate": [grids_uptodate],
        # take current version number of dataset
        "getargs": {"version": ("_get_grids_version", "version")},
        "verbosity": 2,
    }


def emob_integration_task(mvgd):
    """Generator to define emob integration task for a mvgd"""
    cfg = get_config(path=config_dir / ".grids.yaml")
    to_freq = cfg["emob_integration"].get("to_freq")

    yield {
        "name": f"{mvgd}_emob_integration",
        "actions": [
            (
                run_emob_integration,
                [],  # args
                {  # kwargs
                    "grid_id": mvgd,
                    "to_freq": to_freq,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        "task_dep": [f"grids:{mvgd}_load_integration"],
        # take current version number of dataset
        "getargs": {"version": ("_get_grids_version", "version")},
        "uptodate": [grids_uptodate],
        "verbosity": 2,
    }


def hp_integration_task(mvgd):
    """Generator to define hp integration task for a mvgd"""

    yield {
        "name": f"{mvgd}_hp_integration",
        "actions": [
            (
                run_hp_integration,
                [],  # args
                {  # kwargs
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        "task_dep": [f"grids:{mvgd}_emob_integration"],
        # take current version number of dataset
        "getargs": {"version": ("_get_grids_version", "version")},
        "uptodate": [grids_uptodate],
        "verbosity": 2,
    }


def feeder_extraction_task(mvgd):
    """Generator to define feeder extraction task for a mvgd"""

    yield {
        "name": f"{mvgd}_feeder_extraction",
        "actions": [
            (
                run_feeder_extraction,
                [],  # args
                {  # kwargs
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        "task_dep": [f"grids:{mvgd}_hp_integration"],
        # take current version number of dataset
        "getargs": {"version": ("_get_grids_version", "version")},
        "uptodate": [grids_uptodate],
        "verbosity": 2,
    }


def dnm_generation_task(mvgd):
    """Generator to define dnm generation task for a feeder or mvgd"""
    cfg = get_config(path=config_dir / ".grids.yaml")
    yield {
        "name": f"{mvgd}_dnm_generation",
        "actions": [
            (
                run_dnm_generation,
                [],
                {
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                    "feeder": cfg["dnm_generation"]["feeder"],
                },
            )
        ],
        "task_dep": [f"grids:{mvgd}_feeder_extraction"],
        # take current version number of dataset
        "getargs": {"version": ("_get_grids_version", "version")},
        "uptodate": [grids_uptodate],
        "verbosity": 2,
    }


def task_grids():
    """Sets up all tasks for Grid generation"""
    cfg = get_config(path=config_dir / ".grids.yaml")
    mvgds = cfg.get("mvgds")
    logger.info(f"{len(mvgds)} MVGD's in the pipeline")

    for mvgd in mvgds:
        yield load_integration_task(mvgd)
        yield emob_integration_task(mvgd)
        yield hp_integration_task(mvgd)
        yield feeder_extraction_task(mvgd)
        yield dnm_generation_task(mvgd)


# def optimization(mvgd, feeder):
#     """Generator to define optimization task for a feeder"""
#
#     yield {
#         "name": f"{mvgd}/{int(feeder):02}_optimization",
#         "actions": [
#             (
#                 run_dispatch_optimization,
#                 [],
#                 {
#                     "grid_id": mvgd,
#                     "feeder_id": feeder,
#                     "doit": True,
#                     "save": True,
#                 },
#             )
#         ],
#         # take current version number of dataset
#         "getargs": {"version": ("_set_dataset_version", "version")},
#         # "task_dep": [f"grids:{mvgd}_feeder_extraction"],
#         "uptodate": [version_uptodate],
#         "verbosity": 2,
#     }


# def task_opt():
#     """Sets up optimizations task for each feeder"""
#     cfg_o = get_config(path=config_dir / ".opt.yaml")
#     mvgds = sorted(cfg_o.get("mvgds"))
#
#     cfd_g = get_config(path=config_dir / ".grids.yaml")
#     feeder_dir = cfd_g["feeder_extraction"].get("export")
#
#     for mvgd in mvgds:
#         feeder_path = data_dir / feeder_dir / str(mvgd) / "feeder"
#         try:
#             feeder_ids = [
#                 feeder_id
#                 for feeder_id in os.listdir(feeder_path)
#                 if os.path.isdir(feeder_path / feeder_id)
#             ]
#         except FileNotFoundError:
#             continue
#         for feeder in sorted(feeder_ids):
#             yield optimization(mvgd=mvgd, feeder=feeder)


def task_grids_group():
    """Groups grid tasks"""
    cfg = get_config(path=config_dir / ".grids.yaml")
    mvgds = sorted(cfg.pop("mvgds"))
    cfg.pop("version", None)
    tasks = [i for i in cfg.keys() if "mvgds" not in i]
    for mvgd in mvgds:
        yield {
            "actions": None,
            "name": str(mvgd),
            "doc": "per mvgd",
            "task_dep": [f"grids:{mvgd}_{i}" for i in tasks],
        }

    for task in tasks:
        yield {
            "actions": None,
            "name": str(task),
            "doc": "per task",
            "task_dep": [f"grids:{i}_{task}" for i in mvgds],
        }


# def task_opt_group():
#     """Group opt tasks"""
#     cfg_o = get_config(path=config_dir / ".opt.yaml")
#     mvgds = sorted(cfg_o.get("mvgds"))
#
#     cfd_g = get_config(path=config_dir / ".grids.yaml")
#     feeder_dir = cfd_g["feeder_extraction"].get("export")
#
#     for mvgd in mvgds:
#         feeder_path = data_dir / feeder_dir / str(mvgd) / "feeder"
#         try:
#             feeder_ids = [
#                 feeder_id
#                 for feeder_id in sorted(os.listdir(feeder_path))
#                 if os.path.isdir(feeder_path / feeder_id)
#             ]
#         except FileNotFoundError:
#             continue
#         yield {
#             "actions": None,
#             "name": str(mvgd),
#             "doc": "per mvgd",
#             "task_dep": [
#                 f"opt:{mvgd}/{int(i):02}_optimization" for i in feeder_ids
#             ],
#         }


if __name__ == "__main__":

    logger = logging.getLogger("lobaflex.__main__")

    import doit

    doit.run(globals())
