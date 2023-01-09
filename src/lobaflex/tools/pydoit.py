import logging

import doit

from lobaflex import config_dir
from lobaflex.tools.tools import get_config, split_model_config_in_subconfig

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


def task__set_opt_version():
    """This tasks sets the version number of the dataset"""

    def version():
        cfg_o = get_config(path=config_dir / ".opt.yaml")
        version = cfg_o["version"]
        run_id = cfg_o["run_id"]

        dep_manager = doit.Globals.dep_manager
        results = dep_manager.get_result("_set_opt_version")

        old_version = results.get("version", None)
        old_run_id = results.get("run_id", None)

        if version < old_version:
            logger.warning("New version number is lower then old version.")
            if run_id == old_run_id:
                logger.warning("'run_id' is the same.")
                raise ValueError("Change 'run_id' in config.")

        elif version > old_version:
            pass
        elif version == old_version:
            logger.warning("Version number didn't change.")
            if run_id == old_run_id:
                logger.warning("'run_id' didn't change.")
            else:
                logger.warning("Only 'run_id' changed.")

        logger.warning(
            f"Grids dataset version set to: {version} for run:" f" {run_id}"
        )

        return {"version": version, "run_id": run_id}

    return {
        "actions": [version],
    }


def opt_uptodate(task):
    """This function compares the version number of each task with the
    dataset version. If it's not equal the task is not uptodate."""
    dep_manager = doit.Globals.dep_manager
    dataset_results = dep_manager.get_result("_set_opt_version")
    if dataset_results is None:
        raise ValueError("Run 'doit _set_opt_version' first!")
    task_results = dep_manager.get_result(task.name)
    if task_results is None:
        task_results = {"version": -1}
    return (
        True
        if task_results["version"] == dataset_results["version"]
        else False
    )


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
        return {"version": dataset_results["version"]}

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
