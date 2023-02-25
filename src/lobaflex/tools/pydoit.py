import logging
import os

from datetime import datetime

import doit

from lobaflex import config_dir, results_dir
from lobaflex.tools.tools import (
    dump_yaml,
    get_config,
    log_errors,
    split_model_config_in_subconfig,
)

logger = logging.getLogger(__name__)


def task__split_model_config_in_subconfig():
    """This body is always executed to keep the respective configs uptodate"""
    split_model_config_in_subconfig()


def task__set_grids_version():
    """This tasks sets the version number of the dataset"""

    def version():
        cfg = get_config(path=config_dir / ".grids.yaml")
        version = cfg["version"]
        dep_manager = doit.Globals.dep_manager
        results = dep_manager.get_result("_set_grids_version")
        old_version = results.get("version", None)
        if version < old_version:
            logger.warning("New version number is lower then old version.")
        elif version > old_version:
            pass
        elif version == old_version:
            logger.warning("Version number didn't change.")

        print(f"Grids dataset version set to: {version}")
        return {"version": version}

    return {
        "actions": [version],
    }


@log_errors
def task__set_opt_version():
    """This tasks sets the version number of the dataset"""

    def set_version():
        cfg_o = get_config(path=config_dir / ".opt.yaml")

        version = cfg_o["version"]
        run_id = cfg_o["run_id"]

        dep_manager = doit.Globals.dep_manager
        version_db = dep_manager.get_result("_set_opt_version")
        version_db = version_db if isinstance(version_db, dict) else {"db": {}}

        old_version = version_db["db"].get(run_id, None)

        if old_version is None:
            logger.warning(f"New run_id: {run_id}. Set version number to 0.")

            version_db["db"].update({run_id: 0})
            version_db = {
                "current": {"run_id": run_id, "version": 0},
                "db": version_db["db"],
            }

        elif version < old_version:
            diff = version - old_version
            logger.warning(
                f"New version number {version} is {diff} lower "
                "than the previous one."
            )
            logger.warning(
                "Change values for 'run_id' or 'version' in config."
            )

        elif version > old_version + 1:
            diff = version - old_version
            logger.warning(
                f"New version number {version} is {diff} higher "
                "than the previous one."
            )
            logger.warning(
                "Change values for 'run_id' or 'version' in config."
            )

        else:
            logger.warning(f"Version set to: {version} for run: {run_id}")
            version_db["db"].update({run_id: version})
            version_db = {
                "current": {"run_id": run_id, "version": version},
                "db": version_db["db"],
            }

        # save config for run_id and version
        run_id_path = results_dir / run_id
        os.makedirs(run_id_path, exist_ok=True)

        filename = datetime.now().strftime(
            f"config_version_{version}_%Y-%m-%d-%H-%M-%S"
        )
        dump_yaml(yaml_file=cfg_o, save_to=run_id_path, filename=filename)
        logger.debug("Config saved.")

        return version_db

    return {
        "actions": [set_version],
    }


def opt_uptodate(task):
    """This function compares the version number of each task with the
    dataset version if the run_id is the same. If it's not equal the task is
    not uptodate."""
    dep_manager = doit.Globals.dep_manager
    version_db = dep_manager.get_result("_set_opt_version")
    if version_db is None:
        logger.warning(
            "Versioning not initialized.\n" "Run 'doit _set_opt_version' !"
        )
        raise ValueError("Run 'doit _set_opt_version' !")
    current_version = version_db["current"]

    task_db = dep_manager.get_result(task.name)
    if task_db is None:
        return False
    task_version = task_db.get(current_version["run_id"], "new")
    if task_version == "new":
        return False
    elif task_version == current_version["version"]:
        return True
    elif task_version + 1 == current_version["version"]:
        return False
    else:
        diff = current_version["version"] - task_version
        logger.warning(
            f"New version number {current_version['version']} is"
            f" {diff} versions higher than the previous one."
        )
        return False


def grids_uptodate(task):
    """This function compares the version number of each task with the
    dataset version. If it's smaller, the task is not uptodate"""
    dep_manager = doit.Globals.dep_manager
    dataset_results = dep_manager.get_result("_set_grids_version")
    if dataset_results is None:
        raise ValueError("Run 'doit _set_grids_version' first!")
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


def task__get_opt_version():
    """This tasks gets the version number of the dataset"""

    def get_dataset_version():
        dep_manager = doit.Globals.dep_manager
        dataset_results = dep_manager.get_result("_set_opt_version")
        if dataset_results is None:
            raise ValueError("Run '_doit _set_opt_version' first!")

        print(10 * "-----------")
        print("version: run_id")
        print(10 * "-----------")
        for run_id, version in dataset_results["db"].items():
            print(f"{version: 6d}: {run_id}")
        print(10 * "-----------")

        return None

    return {
        "actions": [get_dataset_version],
    }


def task__get_grids_version():
    """This tasks gets the version number of the dataset"""

    def get_dataset_version():
        dep_manager = doit.Globals.dep_manager
        dataset_results = dep_manager.get_result("_set_grids_version")
        if dataset_results is None:
            raise ValueError("Run '_doit _set_grids_version' first!")
        return {"version": dataset_results["version"]}

    return {
        "actions": [get_dataset_version],
    }
