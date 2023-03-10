""""""
import csv
import logging
import os
import sys
import time

from datetime import date, datetime
from functools import wraps
from glob import glob
from pathlib import Path

import doit
import psutil
import requests
import yaml

from doit.exceptions import BaseFail
from dotenv import dotenv_values

from lobaflex import config_dir, results_dir

logger = logging.getLogger(__name__)


def log_errors(func):
    """
    Decorator object that logs every exception into the defined logger object.
    """

    @wraps(func)
    def exception_wrapper(*args, **kwargs):

        try:
            return func(*args, **kwargs)

        except Exception as e:
            issue = "ERROR in " + func.__name__ + "()\n"
            issue += 10 * "--------------------------" + "\n"
            logger.exception(issue)
            raise e

    return exception_wrapper


def get_files_in_subdirs(path, pattern):
    """Creates a list of all files with pattern in its subdirectories"""
    list_files = []
    for dir, _, _ in os.walk(path):
        list_files.extend(glob(os.path.join(dir, pattern)))

    return list_files


def get_config(path):
    """
    Returns the config.
    """
    with open(path, encoding="utf8") as f:
        return yaml.safe_load(f)


def split_model_config_in_subconfig():
    """This body is always executed to keep the respective configs uptodate"""
    cfg = get_config(path=config_dir / "model_config.yaml")
    dump_yaml(yaml_file=cfg, save_to=config_dir, split=True)


def setup_logfile(path):

    os.makedirs(path, exist_ok=True)
    # logger.remove()
    logfile = path / Path(f"{date.isoformat(date.today())}.log")
    logger.add(
        sink=logfile,
        format="{time} {level} {message}",
        level="TRACE",
        backtrace=True,
        diagnose=True,
    )

    logger.info("Start")


def create_dir_or_variant(path):
    """Creates the directory if doesn't exist or creates variant of it"""
    if os.path.isdir(path):
        run_id = datetime.now().strftime("run_%Y-%m-%d-%H-%M-%S")
        path = os.path.join(path, f"{run_id}")
        os.makedirs(path, exist_ok=True)
        logger.info(f"Created variant: {path}")
    else:
        os.makedirs(path, exist_ok=True)
        logger.info(f"Created directory: {path}")


def log_memory_usage():
    """Get memory usage and write to log

    Returns
    -------
    :obj:`int`
        Memory in MB
    """

    process = psutil.Process(os.getpid())
    mem = round(process.memory_info().rss / 1024**2)
    logger = logging.getLogger("windnode_abw")
    logger.info(f"[Memory used (w/o solver): {mem} MB]")

    return mem


def show_logger():
    """Show all logger"""
    existing_loggers = [logging.getLogger()]  # get the root logger
    existing_loggers = existing_loggers + [
        logging.getLogger(name) for name in logging.root.manager.loggerDict
    ]

    for indiv_logger in existing_loggers:
        print(indiv_logger)
        print(indiv_logger.handlers)
        print("-" * 20)


def timeit(func):
    """
    Decorator for measuring function's running time.
    """

    def measure_time(*args, **kw):
        start_time = time.perf_counter()
        result = func(*args, **kw)
        exec_time = time.perf_counter() - start_time
        if exec_time > 2:
            exec_time = time.gmtime(exec_time)
            exec_time = time.strftime("%Hh:%Mm:%Ss", exec_time)
        else:
            exec_time = str(int(exec_time * 100)) + "ms"
        logger.info(f"Processing time of {func.__qualname__}(): {exec_time}.")
        return result

    return measure_time


def write_metadata(path, edisgo_obj, text=False):
    """"""

    # TODO generate metadat from edisgo_obj
    # copy from  check_integrity
    # edisgo_obj
    metadata = ["This is Delhi \n", "This is Paris \n", "This is London \n"]
    grid_id = edisgo_obj.topology.mv_grid
    # Writing to file
    with open(Path(f"{path}/metadata.md"), "w") as file:
        # Writing data to a file
        file.write(f"METADATA for Grid {grid_id} \n {'*'*20} \n \n")
        file.writelines(metadata)
        if text:
            file.writelines(f"\n {'*'*20} \n {text}")


def dump_yaml(yaml_file, save_to, split=False, **kwargs):
    """Split yaml file into multiple yamls by sections"""
    if split:
        for subconfig in yaml_file:
            content = yaml.dump(
                yaml_file[subconfig], default_flow_style=False, sort_keys=False
            )
            path = save_to / f".{subconfig}.yaml"
            with open(path, "w") as file:
                file.write(content)
    else:
        content = yaml.dump(
            yaml_file, default_flow_style=False, sort_keys=False
        )
        filename = kwargs.get("filename", "config")
        path = save_to / f"{filename}.yaml"
        with open(path, "w") as file:
            file.write(content)


def init_versioning():
    """Initialize task versioning"""
    # Versioning
    dep_manager = doit.Globals.dep_manager
    version_db = dep_manager.get_result("_set_opt_version")
    version_db = version_db if isinstance(version_db, dict) else {"db": {}}
    task_version = version_db.get("current", {"run_id": "None"})
    run_id = task_version.get("run_id", "None")

    return version_db, run_id


def telegram_bot_sendtext(text):
    """"""
    cfg_telegram = dotenv_values(dotenv_path=config_dir / "telegram.env")
    token = cfg_telegram.get("TOKEN")
    chat_id = cfg_telegram.get("CHAT_ID")
    params = {"chat_id": chat_id, "text": text}
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    message = requests.post(url, params=params)
    return message


class TelegramReporter(object):
    """ """

    # short description, used by the help system
    desc = "telegram, csv and console output"

    def __init__(self, outstream, options):
        # save non-successful result information (include task errors)
        self.csv_file = datetime.now().strftime("run_%Y-%m-%d-%H-%M-%S.csv")
        self.failures = []
        self.runtime_errors = []
        self.failure_verbosity = options.get("failure_verbosity", 0)
        self.outstream = outstream
        self.telegram = telegram_bot_sendtext
        self.start_time = {}
        self.status = {}
        self.run = set()
        self.run_id = str()

    def write(self, text):
        self.outstream.write(text)
        # self.telegram(text)

    def initialize(self, tasks, selected_tasks):
        """called just after tasks have been loaded before execution starts"""

        _, self.run_id = init_versioning()

        pipeline = str()
        # [task for task in tasks.get(group) for group in selected_tasks]
        for group in selected_tasks:
            self.status[group] = None
            pipeline += pipeline.join([f"- {group}\n"])
            for task in tasks.get(group).task_dep:
                self.status[task] = None
                pipeline += pipeline.join([f"- {task}\n"])
        self.start_time["total"] = time.perf_counter()
        current_time = datetime.now().strftime("%A %d-%m-%Y, %H:%M:%S")
        if "_set_" not in pipeline and "_get_" not in pipeline:
            self.telegram(
                text="Pipeline started\n"
                + "-" * 28
                + "\n"
                + current_time
                + "\n"
                + "-" * 28
            )
            # self.telegram(text=f"Pipeline:\n{pipeline}")

    def get_status(self, task):
        """called when task is selected (check if up-to-date)"""
        pass

    def execute_task(self, task):
        """called when execution starts"""
        # ignore tasks that do not define actions
        # ignore private/hidden tasks (tasks that start with an underscore)
        if task.actions and (task.name[0] != "_"):
            self.write(".  %s\n" % task.title())
            self.start_time[task.name] = time.perf_counter()
            # self.telegram(text=f"Task {task.title()} is executed.")

    def add_failure(self, task, fail: BaseFail):
        """called when execution finishes with a failure"""
        result = {"task": task, "exception": fail}
        if fail.report:
            try:
                exec_time = time.perf_counter() - self.start_time[task.name]
                exec_time = time.gmtime(exec_time)
                exec_time = time.strftime("%Hh:%Mm:%Ss", exec_time)
                self.telegram(text=f"Failed: {task.name} after {exec_time}")
                self.status[task.name] = "fail"

            except KeyError:
                self.telegram(text=f"Unmet dependency: {task.name}")
                self.status[task.name] = "dependency"
            self.failures.append(result)
            self._write_failure(result)

    def add_success(self, task):
        """called when execution finishes successfully"""

        self.status[task.name] = "success"
        # if task successful and not private
        if task.actions and (task.name[0] != "_"):
            try:
                # not sure what this is for?!
                self.run.update({task.result.get("run")})
            except AttributeError:
                # I guess only error detection
                self.telegram(text=task.name)
            exec_time = time.perf_counter() - self.start_time[task.name]
            exec_time = time.gmtime(exec_time)
            exec_time = time.strftime("%Hh:%Mm:%Ss", exec_time)
            # self.telegram(text=f"Success: {task.title()}\n in {exec_time}")
        if "_set" in task.name:
            group = task.name.split("_")[2]
            version = task.result["current"]["version"]
            run_id = task.result["current"]["run_id"]
            if run_id is None:
                message = f"Version of {group} set to {version}."
            else:
                message = (
                    f"Version of {group} set to {version} for run "
                    f"{run_id}."
                )
            self.telegram(text=message)

    def skip_uptodate(self, task):
        """skipped up-to-date task"""
        if task.name[0] != "_":
            self.write("-- %s\n" % task.title())
            self.status[task.name] = "uptodate"
            # self.telegram(text=f"Skip: {task.title()}.")

    def skip_ignore(self, task):
        """skipped ignored task"""
        self.write("!! %s\n" % task.title())

    def cleanup_error(self, exception):
        """error during cleanup"""
        sys.stderr.write(exception.get_msg())

    def runtime_error(self, msg):
        """error from doit (not from a task execution)"""
        # saved so they are displayed after task failures messages
        self.runtime_errors.append(msg)

    def teardown_task(self, task):
        """called when starts the execution of teardown action"""
        pass

    def _write_failure(self, result, write_exception=True):
        msg = "{} - taskid:{}\n".format(
            result["exception"].get_name(),
            result["task"].name,
        )
        self.write(msg)
        if write_exception:
            self.write(result["exception"].get_msg())
            self.write("\n")

    def complete_run(self):
        """called when finished running all tasks"""

        # if _set_opt_version task is run, no logfile needs to be printed
        if (
            self.status.get("_set_opt_version", None) == "success"
            or self.status.get("_get_opt_version", None) == "success"
        ):
            pass
        else:
            # write csv logs for failed task incl traceback
            csv_file = results_dir / self.run_id / self.csv_file
            with open(csv_file, "w", newline="") as csvfile:
                # Create a CSV writer object
                writer = csv.writer(csvfile)
                writer.writerow(["task", "error-msg"])
                for fail in self.failures:
                    # Write the new line to the file
                    if fail["task"].executed:
                        writer.writerow(
                            [fail["task"].name, str(fail["exception"])]
                        )

        # if test fails print output from failed task
        for result in self.failures:

            task = result["task"]
            # makes no sense to print output if task was not executed
            if not task.executed:
                continue
            show_err = task.verbosity < 1 or self.failure_verbosity > 0
            show_out = task.verbosity < 2 or self.failure_verbosity == 2
            if show_err or show_out:
                self.write("#" * 40 + "\n")

            if show_err:
                self._write_failure(
                    result, write_exception=self.failure_verbosity
                )
                err = "".join([a.err for a in task.actions if a.err])
                self.write("{} <stderr>:\n{}\n".format(task.name, err))
            if show_out:
                out = "".join([a.out for a in task.actions if a.out])
                self.write("{} <stdout>:\n{}\n".format(task.name, out))

        if self.runtime_errors:
            self.write("#" * 40 + "\n")
            self.write("Execution aborted.\n")
            self.telegram(text="Execution aborted.")
            self.write("\n".join(self.runtime_errors))
            self.write("\n")

        # Don't send if any task with _set_ or _get_ included
        # this should only happen if _set and _get is executed individually
        tasklist = str().join(self.status.keys())
        if "_set_" not in tasklist and "_get_" not in tasklist:

            exec_time = time.perf_counter() - self.start_time["total"]
            exec_time = time.gmtime(exec_time)
            exec_time = time.strftime("%Hh:%Mm:%Ss", exec_time)
            current_time = datetime.now().strftime("%A %d-%m-%Y, %H:%M:%S")

            # Count task but do not include _get_*_version and _set_*_version
            success = [
                key
                for key, value in self.status.items()
                if value == "success" and "_version" not in key
            ]

            failed = [
                key
                for key, value in self.status.items()
                if value == "fail" and "_version" not in key
            ]

            uptodate = [
                key
                for key, value in self.status.items()
                if value == "uptodate" and "_version" not in key
            ]

            dependency = [
                key
                for key, value in self.status.items()
                if value == "dependency" and "_version" not in key
            ]

            total = (
                len(success) + len(failed) + len(uptodate) + len(dependency)
            )
            statistic = "Statistic:\n"
            statistic += f"Total of {total} tasks.\n"
            statistic += f"{len(uptodate)} uptodate.\n"
            statistic += f"{len(success)} succeeded.\n"
            statistic += f"{len(failed)} failed.\n"
            statistic += f"{len(dependency)} unmet dependencies.\n"

            # summary = "Summary:\n"
            # summary += str().join([f"-- {i}\n" for i in uptodate])
            # summary += str().join([f".. {i}\n" for i in success])
            # summary += str().join([f"!! {i}\n" for i in failed])
            # summary += str().join([f"-! {i}\n" for i in dependency])

            self.telegram(
                text=f"Run finished after {exec_time}. \n"
                + "#" * 28
                + "\n"
                + current_time
            )
            self.telegram(text=statistic)
            # Deactivated as messages became to big
            # self.telegram(text=summary)
