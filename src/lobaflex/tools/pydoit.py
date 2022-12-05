import doit

from lobaflex import config_dir
from lobaflex.tools.tools import get_config, split_model_config_in_subconfig


def task__split_model_config_in_subconfig():
    """This body is always executed to keep the respective configs uptodate"""
    split_model_config_in_subconfig()


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
