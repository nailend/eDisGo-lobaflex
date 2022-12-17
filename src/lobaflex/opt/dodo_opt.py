import logging
import os

from lobaflex import config_dir, data_dir
from lobaflex.opt.dispatch_optimization import run_dispatch_optimization
from lobaflex.tools.pydoit import (
    opt_uptodate,
    task__get_opt_version,
    task__set_opt_version,
    task__split_model_config_in_subconfig,
)
from lobaflex.tools.tools import TelegramReporter, get_config

logger = logging.getLogger("lobaflex.opt." + __name__)
# TODO
#   4. alternative uptodate function: version,
#   6. clean
#   7. callback telegram bot
#   8. watch param
#   9. Check connection to db, maybe at beginning and raise warning


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
        "getargs": {"version": ("_get_opt_version", "version")},
        "uptodate": [opt_uptodate],
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
        except FileNotFoundError as e:
            logger.info(f"No Files found for MVGD: {mvgd}")
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
        except FileNotFoundError as e:
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
