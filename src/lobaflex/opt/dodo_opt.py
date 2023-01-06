import logging
import os

from datetime import datetime

from lobaflex import config_dir, data_dir, logs_dir
from lobaflex.opt.dispatch_optimization import run_dispatch_optimization
from lobaflex.opt.result_concatination import save_concatinated_results
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.pydoit import (
    opt_uptodate,
    task__get_opt_version,
    task__set_opt_version,
    task__split_model_config_in_subconfig,
)
from lobaflex.tools.tools import TelegramReporter, get_config

logger = logging.getLogger("lobaflex.opt." + __name__)
date = datetime.now().date().isoformat()
cfg_o = get_config(path=config_dir / ".opt.yaml")
logfile = logs_dir / f"opt_dodo_{date}.log"
setup_logging(file_name=logfile)

# TODO
#   7. callback telegram bot
#   8. watch param
#   9. Check connection to db, maybe at beginning and raise warning

DOIT_CONFIG = {
    "default_tasks": ["opt", "concat_results"],
    "reporter": TelegramReporter,
}

logger.info(f"Run: {cfg_o['run']} - Version:{cfg_o['version']}")




def task_opt():
    """Generator for optimization tasks"""

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o["mvgds"])
    feeder_dir = cfg_o["import_dir"]

    # create opt task only for existing grid folders
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


def task_concat_results():
    """Generator for concatination tasks"""

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o["mvgds"])
    feeder_dir = cfg_o["import_dir"]

    # create opt task only for existing grid folders
    for mvgd in mvgds:
        mvgd_path = data_dir / feeder_dir / str(mvgd)
        if os.path.isdir(mvgd_path):
            feeder_path = data_dir / feeder_dir / str(mvgd) / "feeder"
            try:
                feeder_ids = [
                    feeder_id
                    for feeder_id in os.listdir(feeder_path)
                    if os.path.isdir(feeder_path / feeder_id)
                ]
                yield {
                    "name": f"{mvgd}_{cfg_o['run']}",
                    "actions": [
                        (
                            save_concatinated_results,
                            [],
                            {
                                "grids": [mvgd],
                                "doit": True,
                            },
                        )
                    ],
                    "task_dep": [
                        f"opt:{mvgd}/{int(feeder):02}_optimization"
                        for feeder in feeder_ids
                    ],
                    "getargs": {"version": ("_get_opt_version", "version")},
                    "uptodate": [opt_uptodate],
                }
            except FileNotFoundError as e:
                logger.info(
                    f"No Files found for concat dependency of" f" MVGD: {mvgd}"
                )
                continue


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


if __name__ == "__main__":
    import doit

    doit.run(globals())
