import os

from doit.reporter import ConsoleReporter, JsonReporter
from doit.tools import check_timestamp_unchanged, result_dep

from dnm_generation import run_dnm_generation
from emob_integration import run_emob_integration
from feeder_extraction import run_feeder_extraction
from hp_integration import run_hp_integration
from load_integration import run_load_integration
from logger import logger
from model_solving import run_optimization
from tools import get_config, get_csv_in_subdirs, get_dir, split_yaml

src_dir = get_dir(key="src")
logs_dir = get_dir(key="logs")
data_dir = get_dir(key="data")
config_dir = get_dir(key="config")

# TODO
#   4. alternative uptodate function: version,
#   6. clean
#   7. callback telegram bot
#   8. watch param
#   9. Check connection to db, maybe at beginnin and raise warning


def task_split_model_config_in_subconfig():
    config_dir = get_dir(key="config")
    cfg = get_config(path=config_dir / "model_config.yaml")
    split_yaml(yaml_file=cfg, save_to=config_dir)


# def task_get_config_global():
#     global cfg
#     cfg = get_config(path=config_dir / ".grids.yaml")


def load_integration_task(mvgd):

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
        "verbosity": 2,
    }


def emob_integration_task(mvgd):
    """Import emob"""
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
        "verbosity": 2,
    }


def hp_integration_task(mvgd):

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
        "verbosity": 2,
    }


def feeder_extraction_task(mvgd):

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
        "verbosity": 2,
    }


def dnm_generation_task(mvgd):

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
                },
            )
        ],
        "task_dep": [f"grids:{mvgd}_feeder_extraction"],
        "verbosity": 2,
    }


def task_grids():
    """Generate all task for Grid generation"""
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

    yield {
        "name": f"{mvgd}/{int(feeder):02}_optimization",
        "actions": [
            (
                run_optimization,
                [],
                {
                    "grid_id": mvgd,
                    "feeder_id": feeder,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # "task_dep": [f"grids:{mvgd}_feeder_extraction"],
        "verbosity": 2,
    }


def task_opt():
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
    mvgds = sorted(cfg.get("mvgds"))
    tasks = [i for i in cfg.keys() if "mvgd" not in i]
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
                for feeder_id in os.listdir(feeder_path)
                if os.path.isdir(feeder_path / feeder_id)
            ]
        except FileNotFoundError:
            continue
        yield {
            "actions": None,
            "name": str(mvgd),
            "doc": "per mvgd",
            "task_dep": [
                f"{mvgd}/{int(i):02}_optimization" for i in feeder_ids
            ],
        }


if __name__ == "__main__":
    import doit

    doit.run(globals())
