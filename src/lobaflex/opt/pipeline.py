import logging
import os

from datetime import datetime

import doit
from pathlib import Path
from doit import create_after

from lobaflex import config_dir, data_dir, logs_dir, results_dir
from lobaflex.opt.tasks import (  # dnm_generation_task,
    dispatch_integration_task,
    dot_file_task,
    feeder_extraction_task,
    grid_reinforcement_task,
    optimization_task,
    png_file_task,
    result_concatenation_task,
    timeframe_selection_task,
    expansion_scenario_task,
)
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.pydoit import opt_uptodate  # noqa: F401
from lobaflex.tools.pydoit import task__get_opt_version  # noqa: F401
from lobaflex.tools.pydoit import task__set_opt_version  # noqa: F401
from lobaflex.tools.tools import (
    TelegramReporter,
    get_config,
    init_versioning,
    split_model_config_in_subconfig,
)

split_model_config_in_subconfig()
logger = logging.getLogger("lobaflex.opt." + __name__)
date = datetime.now().date().isoformat()
cfg_o = get_config(path=config_dir / ".opt.yaml")
logfile = logs_dir / f"pipeline_{date}.log"
setup_logging(file_name=logfile)

# TODO NICE-T0-HAVE:
#   - callback telegram bot
#   - watch param
#   - Check connection to db, maybe at beginning and raise warning
#   - python SSH for grids generation
#   - clean
#   - teardown

DOIT_CONFIG = {
    "default_tasks": ["ref", "min_exp", "min_pot", "exp_scn", "scn_pot"],
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
    version_db, run_id = init_versioning()
    # dep_manager = doit.Globals.dep_manager
    # version_db = dep_manager.get_result("_set_opt_version")
    # version_db = version_db if isinstance(version_db, dict) else {"db": {}}
    # task_version = version_db.get("current", {"run_id": "None"})
    # run_id = task_version.get("run_id", "None")

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
                                         source=Path(objective) / "mvgd",
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
    directory = Path("reference") / "feeder"

    # Versioning
    version_db, run_id = init_versioning()

    # create tasks only for existing grid folders
    for mvgd in mvgds:
        mvgd_path = results_dir / run_id / str(mvgd)
        if os.path.isdir(mvgd_path):

            feeder_path = mvgd_path / directory
            os.makedirs(feeder_path, exist_ok=True)

            # get all existing feeder_ids is in directory
            feeder_ids = [
                f for f in os.listdir(feeder_path)
                if os.path.isdir(feeder_path / f)
            ]

            dependencies = []
            for feeder in sorted(feeder_ids):

                yield optimization_task(
                    mvgd=mvgd,
                    feeder=feeder,
                    objective=objective,
                    directory=directory,
                    run_id=run_id,
                    version_db=version_db,
                    dep=[f"ref:reference_feeder_{mvgd}"]
                )

                # generate dependency list for concatenation task
                dependencies += [f"min_exp:{objective}_{mvgd}/{int(feeder):02}"]

            yield result_concatenation_task(
                mvgd=mvgd,
                objective=objective,
                directory=Path(""),
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
                source=Path("minimize_loading") / "reinforced",
                run_id=run_id,
                version_db=version_db,
                dep=[f"min_exp:reinforce_{mvgd}"],
            )


@create_after(executed="min_exp")
def task_min_pot():
    """Generator for minimal load balancing potential tasks"""

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o["mvgds"])
    objectives = ["maximize_grid_power",
                  "minimize_grid_power",
                  "maximize_energy_level",
                  "minimize_energy_level"]
    directory = Path("minimize_loading") / "feeder"
    # TODO add pathways

    # Versioning
    version_db, run_id = init_versioning()

    # create opt task only for existing grid folders
    for mvgd in mvgds:
        mvgd_path = results_dir / run_id / str(mvgd)
        if os.path.isdir(mvgd_path):

            feeder_path = mvgd_path / directory
            os.makedirs(feeder_path, exist_ok=True)

            # Get all existing feeder_ids is in directory
            feeder_ids = [
                f for f in os.listdir(feeder_path)
                if os.path.isdir(feeder_path / f)
            ]
            for objective in objectives:

                dependencies = []
                for feeder in sorted(feeder_ids):

                    yield optimization_task(
                        mvgd=mvgd,
                        feeder=feeder,
                        objective=objective,
                        directory=directory,
                        run_id=run_id,
                        version_db=version_db,
                        dep=[f"ref:reference_feeder_{mvgd}"]
                    )

                    dependencies += [f"min_pot:{objective}_{mvgd}"
                                     f"/{int(feeder):02}"]

                yield result_concatenation_task(
                    mvgd=mvgd,
                    objective=objective,
                    directory=Path("potential") / "minimize_loading",
                    run_id=run_id,
                    version_db=version_db,
                    dep=dependencies
                )


@create_after(executed="min_exp")
def task_exp_scn():
    """Generator for expansion scenario tasks"""

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o["mvgds"])

    scenarios = [20, 40, 60]

    # Versioning
    version_db, run_id = init_versioning()

    # create opt task only for existing grid folders
    for mvgd in mvgds:
        mvgd_path = results_dir / run_id / str(mvgd)
        if os.path.isdir(mvgd_path):

            for scenario in scenarios:

                yield expansion_scenario_task(
                    mvgd=mvgd,
                    percentage=scenario,
                    run_id=run_id,
                    version_db=version_db,
                    dep=[f"min_exp:reinforce_{mvgd}"]
                                              )

                source = (
                        Path("scenarios")
                        / f"{scenario}_pct_reinforced"
                        / "mvgd"
                )
                yield feeder_extraction_task(
                    mvgd=mvgd,
                    objective=f"{scenario}_pct_reinforced",
                    source=source,
                    run_id=run_id,
                    version_db=version_db,
                    dep=[f"exp_scn:{scenario}_pct_reinforced_{mvgd}"],
                                             )


@create_after(executed="exp_scn")
def task_scn_pot():
    """Generator for expansion scenarios load balancing potential tasks"""

    cfg_o = get_config(path=config_dir / ".opt.yaml")
    mvgds = sorted(cfg_o["mvgds"])
    objectives = ["maximize_grid_power",
                  "minimize_grid_power",
                  "maximize_energy_level",
                  "minimize_energy_level"]

    # TODO add pathways

    version_db, run_id = init_versioning()

    for mvgd in mvgds:
        mvgd_path = results_dir / run_id / str(mvgd)
        if os.path.isdir(mvgd_path):

            scenario_path = mvgd_path / "scenarios"
            scenarios = sorted([i for i in os.listdir(scenario_path)])
            for scenario in scenarios:

                feeder_path = scenario_path / scenario / "feeder"
                os.makedirs(feeder_path, exist_ok=True)
                feeder_ids = [
                    f for f in os.listdir(feeder_path)
                    if os.path.isdir(feeder_path / f)
                ]
                for objective in objectives:

                    dependencies = []
                    for feeder in sorted(feeder_ids):

                        yield optimization_task(
                            mvgd=mvgd,
                            feeder=feeder,
                            objective=objective,
                            directory=Path("scenarios") / scenario / "feeder",
                            run_id=run_id,
                            version_db=version_db,
                            dep=[f"exp_scn:{scenario}_feeder_{mvgd}"]
                        )

                        dependencies += [f"scn_pot:{scenario}_{objective}"
                                         f"_{mvgd}/{int(feeder):02}"]

                    yield result_concatenation_task(
                        mvgd=mvgd,
                        objective=objective,
                        directory=Path("potential") / scenario,
                        run_id=run_id,
                        version_db=version_db,
                        dep=dependencies
                    )


if __name__ == "__main__":

    doit.run(globals())
