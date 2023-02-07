import logging
import os

from datetime import datetime

import doit

from doit import create_after

from lobaflex import config_dir, data_dir, logs_dir, results_dir
from lobaflex.opt.tasks import (  # dnm_generation_task,
    dispatch_integration_task,
    dot_file_task,
    feeder_extraction_task,
    grid_reinforcement_task,
    optimization_task,
    png_file_task,
    result_concatination_task,
    timeframe_selection_task,
)
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.pydoit import opt_uptodate  # noqa: F401
from lobaflex.tools.pydoit import task__get_opt_version  # noqa: F401
from lobaflex.tools.pydoit import task__set_opt_version  # noqa: F401
from lobaflex.tools.tools import (
    TelegramReporter,
    get_config,
    split_model_config_in_subconfig,
)

split_model_config_in_subconfig()
logger = logging.getLogger("lobaflex.opt." + __name__)
date = datetime.now().date().isoformat()
cfg_o = get_config(path=config_dir / ".opt.yaml")
logfile = logs_dir / f"pipeline_{date}.log"
setup_logging(file_name=logfile)

# TODO
#   7. callback telegram bot
#   8. watch param
#   9. Check connection to db, maybe at beginning and raise warning

DOIT_CONFIG = {
    "default_tasks": ["ref", "min_exp", "min_pot"],
    "reporter": TelegramReporter,
}


def task__do_graph():
    path = results_dir / "graph"
    yield dot_file_task(path)
    yield png_file_task(path)


def task_update():
    """Logs the current version and run_id with every doit command"""
    dep_manager = doit.Globals.dep_manager
    version_db = dep_manager.get_result("_set_opt_version")
    version_db = version_db if isinstance(version_db, dict) else {"db": {}}
    version = version_db.get("current", {"run_id": None, "version": None})
    logger.info(f"Run: {version['run_id']} - Version: {version['version']}")


# def task_grids():
    # Generate grids

def task_ref():
    """Generator for reference grids

    Feeder extraction and dnm matrix generation"""

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o["mvgds"])
    import_dir = cfg_o["import_dir"]
    objective = "reference"

    # Versioning
    dep_manager = doit.Globals.dep_manager
    version_db = dep_manager.get_result("_set_opt_version")
    version_db = version_db if isinstance(version_db, dict) else {"db": {}}
    task_version = version_db.get("current", {"run_id": "None"})
    run_id = task_version.get("run_id", "None")

    # TODO
    # observation_import = data_dir / import_dir / str(mvgd)
    # path = results_dir / run_id / str(mvgd) / timeframe
    for mvgd in mvgds:
        data_path = data_dir / import_dir / str(mvgd)
        if os.path.isdir(data_path):

            # TODO observation periods
            yield timeframe_selection_task(mvgd, run_id, version_db)

            yield feeder_extraction_task(mvgd=mvgd,
                                         objective=objective,
                                         source="reference_mvgd",
                                         run_id=run_id,
                                         version_db=version_db,
                                         dep=[f"ref:timeframe_{mvgd}"],
                                         )

            yield grid_reinforcement_task(
                mvgd=mvgd,
                objective=objective,
                run_id=run_id,
                version_db=version_db,
                dep=[f"ref:timeframe_{mvgd}"],
            )


@create_after(executed="ref")
def task_min_exp():
    """Generator for minimal grid expansion tasks

    minimize loading dispatch optimization, concatination, integration,
    reinforcement and feeder extraction"""

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o["mvgds"])
    objective = "minimize_loading"
    source = "reference_feeder"

    # Versioning
    dep_manager = doit.Globals.dep_manager
    version_db = dep_manager.get_result("_set_opt_version")
    version_db = version_db if isinstance(version_db, dict) else {"db": {}}
    task_version = version_db.get("current", {"run_id": "None"})
    run_id = task_version.get("run_id", "None")

    # create opt task only for existing grid folders
    for mvgd in mvgds:
        mvgd_path = results_dir / run_id / str(mvgd)
        if os.path.isdir(mvgd_path):

            feeder_path = mvgd_path / source

            feeder_ids = [
                f
                for f in os.listdir(feeder_path)
                if os.path.isdir(feeder_path / f)
            ]

            dependencies = []
            for feeder in sorted(feeder_ids):

                yield optimization_task(
                    mvgd=mvgd,
                    feeder=feeder,
                    objective=objective,
                    source=source,
                    run_id=run_id,
                    version_db=version_db,
                    dep=[f"ref:feeder_{mvgd}"]
                )

                dependencies += [f"min_exp:{objective}_{mvgd}/{int(feeder):02}"]

            yield result_concatination_task(
                mvgd=mvgd,
                objective=objective,
                run_id=run_id,
                version_db=version_db,
                dep=dependencies
            )

            yield dispatch_integration_task(
                mvgd=mvgd,
                objective=objective,
                run_id=run_id,
                version_db=version_db,
                dep=[f"min_exp:concat_{objective}_{mvgd}"],
            )

            yield grid_reinforcement_task(
                mvgd=mvgd,
                objective=objective,
                run_id=run_id,
                version_db=version_db,
                dep=[f"min_exp:add_ts_{mvgd}"],
            )

            yield feeder_extraction_task(
                mvgd=mvgd,
                objective=objective,
                source="minimize_loading_reinforced",
                run_id=run_id,
                version_db=version_db,
                dep=[f"min_exp:reinforce_{mvgd}"],
            )


# @create_after(executed="path")
@create_after(executed="min_exp")
def task_min_pot():
    """Generator for minimal load balancing potential tasks"""

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o["mvgds"])
    objectives = ["maximize_grid_power",
                  "maximize_energy_level",
                  "minimize_energy_level"]
    source = "minimize_loading_feeder"
    # TODO add pathways

    # Versioning
    dep_manager = doit.Globals.dep_manager
    version_db = dep_manager.get_result("_set_opt_version")
    version_db = version_db if isinstance(version_db, dict) else {"db": {}}
    task_version = version_db.get("current", {"run_id": "None"})
    run_id = task_version.get("run_id", "None")

    # create opt task only for existing grid folders
    for objective in objectives:
        for mvgd in mvgds:
            mvgd_path = results_dir / run_id / str(mvgd)
            if os.path.isdir(mvgd_path):

                feeder_path = mvgd_path / source

                feeder_ids = [
                    f
                    for f in os.listdir(feeder_path)
                    if os.path.isdir(feeder_path / f)
                ]

                dependencies = []
                for feeder in sorted(feeder_ids):

                    yield optimization_task(
                        mvgd=mvgd,
                        feeder=feeder,
                        objective=objective,
                        source=source,
                        run_id=run_id,
                        version_db=version_db,
                        dep=[f"ref:feeder_{mvgd}"]
                    )

                    dependencies += [f"min_pot:{objective}_{mvgd}"
                                     f"/{int(feeder):02}"]

                yield result_concatination_task(
                    mvgd, objective, run_id, version_db, dep=dependencies
                )



if __name__ == "__main__":

    doit.run(globals())
