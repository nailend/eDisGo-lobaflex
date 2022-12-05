import os

from doit.reporter import ConsoleReporter, JsonReporter
from doit.tools import check_timestamp_unchanged, result_dep

# from model_solving import run_optimization
from dispatch_optimization import run_dispatch_optimization
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


def task_split_model_config_in_subconfig():
    """This task is always executed to update the split model configs"""
    config_dir = get_dir(key="config")
    cfg = get_config(path=config_dir / "model_config.yaml")
    dump_yaml(yaml_file=cfg, save_to=config_dir, split=True)


def optimization(mvgd, feeder):

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


DOIT_CONFIG = {
    "default_tasks": ["opt"],
    "reporter": TelegramReporter,
}


if __name__ == "__main__":
    import doit

    doit.run(globals())
