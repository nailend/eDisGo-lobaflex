import os

from doit.reporter import ConsoleReporter, JsonReporter
from doit.tools import check_timestamp_unchanged, result_dep

# from model_solving import run_optimization
import doit

from dispatch_optimization import run_dispatch_optimization
from dnm_generation import run_dnm_generation
from emob_integration import run_emob_integration
from feeder_extraction import run_feeder_extraction
from hp_integration import run_hp_integration
from load_integration import run_load_integration
from logger import logger
from tools import TelegramReporter, dump_yaml, get_config, get_csv_in_subdirs, get_dir

src_dir = get_dir(key="src")
logs_dir = get_dir(key="logs")
data_dir = get_dir(key="data")
config_dir = get_dir(key="config")

# TODO
#   4. alternative uptodate function: version,
#   6. clean
#   7. callback telegram bot
#   8. watch param
#   9. Check connection to db, maybe at beginning and raise warning

DOIT_CONFIG = {"default_tasks": ["grids", "opt"],
               "reporter": TelegramReporter}

def task__split_model_config_in_subconfig():
    """This body is always executed to keep the respective configs uptodate"""
    config_dir = get_dir(key="config")
    cfg = get_config(path=config_dir / "model_config.yaml")
    dump_yaml(yaml_file=cfg, save_to=config_dir, split=True)


# def task__init_dataset_version():
#     cfg = get_config(path=config_dir / ".grids.yaml")
#     dep_manager = doit.Globals.dep_manager
#     dataset_results = dep_manager.get_result("_set_dataset_version")
#     if dataset_results is None:
#         dep_manager.set(task_id="_set_dataset_version",
#                         value=0)


def task__set_dataset_version():
    """This tasks sets the version number of the dataset"""

    def version():
        cfg = get_config(path=config_dir / ".grids.yaml")
        version = cfg["version"]
        print(f"Grids dataset version set to: {version}")
        return {"version": version}

    return {
        "actions": [version],
    }


def version_uptodate(task):
    """This function compares the version number of each task with the
    dataset version. If it's smaller, the task is not uptodate"""
    dep_manager = doit.Globals.dep_manager
    dataset_results = dep_manager.get_result("_set_dataset_version")
    if dataset_results is None:
        raise ValueError("Run 'doit _set_dataset_version -v %int' first!")
    task_results = dep_manager.get_result(task.name)
    if task_results is None:
        task_results = {"version": -1}
    return (
        True
        if task_results["version"] >= dataset_results["version"]
        else False
    )


def task__get_version():
    """Private task which print version of task and dataset. Task name needs
    to be pass via parameter -t."""

    def get_version(task):
        dep_manager = doit.Globals.dep_manager
        dataset_results = dep_manager.get_result("_set_dataset_version")
        if dataset_results is None:
            raise ValueError("Run '_doit _set_dataset_version -v %' first!")
        print(f"Dateset version: {dataset_results['version']}")

        if task != "_set_dataset_version":
            task_results = dep_manager.get_result(task)
            if task_results is None:
                raise ValueError(f"{task} was not executed yet.")
            print(f"Task {task} version: {task_results['version']}")

            return (
                True
                if task_results["version"] >= dataset_results["version"]
                else False
            )
        else:
            return True

    return {
        "actions": [
            (get_version,),
        ],
        "params": [
            {
                "name": "task",
                "short": "t",
                "long": "task",
                "type": str,
                "default": "_set_dataset_version",
                "help": "Name of task to get version from",
            },
        ],
    }


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
        "uptodate": [version_uptodate],
        # take current version number of dataset
        "getargs": {"version": ("_set_dataset_version", "version")},
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
        "getargs": {"version": ("_set_dataset_version", "version")},
        "uptodate": [version_uptodate],
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
        "getargs": {"version": ("_set_dataset_version", "version")},
        "uptodate": [version_uptodate],
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
        "getargs": {"version": ("_set_dataset_version", "version")},
        "uptodate": [version_uptodate],
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
        "getargs": {"version": ("_set_dataset_version", "version")},
        "uptodate": [version_uptodate],
        "verbosity": 2,
    }


def task_grids():
    """Sets up all tasks for Grid generation"""
    cfg = get_config(path=config_dir / ".grids.yaml")
    mvgds = sorted(cfg.get("mvgds"))
    logger.info(f"{len(mvgds)} MVGD's in the pipeline")

    for mvgd in mvgds:
        yield load_integration_task(mvgd)
        yield emob_integration_task(mvgd)
        yield hp_integration_task(mvgd)
        yield feeder_extraction_task(mvgd)
        yield dnm_generation_task(mvgd)


def optimization(mvgd, feeder):
    """Generator to define optimization task for a feeder"""

    yield {
        "name": f"{mvgd}/{int(feeder):02}_optimization",
        "actions": [
            (
                run_dispatch_optimization,
                [],
                {
                    "grid_id": mvgd,
                    "feeder_id": feeder,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # take current version number of dataset
        "getargs": {"version": ("_set_dataset_version", "version")},
        # "task_dep": [f"grids:{mvgd}_feeder_extraction"],
        "uptodate": [version_uptodate],
        "verbosity": 2,
    }


def task_opt():
    """Sets up optimizations task for each feeder"""
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o.get("mvgds"))

    cfd_g = get_config(path=config_dir / ".grids.yaml")
    feeder_dir = cfd_g["feeder_extraction"].get("export")

    for mvgd in mvgds:
        feeder_path = data_dir / feeder_dir / str(mvgd) / "feeder"
        try:
            feeder_ids = [
                feeder_id
                for feeder_id in os.listdir(feeder_path)
                if os.path.isdir(feeder_path / feeder_id)
            ]
        except FileNotFoundError:
            continue
        for feeder in sorted(feeder_ids):
            yield optimization(mvgd=mvgd, feeder=feeder)


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


def task_opt_group():
    """Group opt tasks"""
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o.get("mvgds"))

    cfd_g = get_config(path=config_dir / ".grids.yaml")
    feeder_dir = cfd_g["feeder_extraction"].get("export")

    for mvgd in mvgds:
        feeder_path = data_dir / feeder_dir / str(mvgd) / "feeder"
        try:
            feeder_ids = [
                feeder_id
                for feeder_id in sorted(os.listdir(feeder_path))
                if os.path.isdir(feeder_path / feeder_id)
            ]
        except FileNotFoundError:
            continue
        yield {
            "actions": None,
            "name": str(mvgd),
            "doc": "per mvgd",
            "task_dep": [
                f"opt:{mvgd}/{int(i):02}_optimization" for i in feeder_ids
            ],
        }




if __name__ == "__main__":
    import doit

    doit.run(globals())
