""""""
import logging
import os
import time

from datetime import date, datetime
from glob import glob
from pathlib import Path

import psutil
import yaml

from logger import logger


def get_dir(key):
    """Get directories parallel to src level"""
    src_dir = Path(".").absolute()
    repo_dir = src_dir.parent
    key_dir = repo_dir / key
    return key_dir


def get_csv_in_subdirs(path):
    """Creates a list of all csv files in its subdirectories"""
    list_files = []
    pattern = "*.csv"
    for dir, _, _ in os.walk(path):
        list_files.extend(glob(os.path.join(dir, pattern)))

    return list_files


def get_config(path=f".{get_dir(key='config')}/model_config.yaml"):
    """
    Returns the config.
    """
    with open(path, encoding="utf8") as f:
        return yaml.safe_load(f)


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


def setup_logger(name=None, loglevel=logging.DEBUG):
    """
    Instantiate logger

    Parameters
    ----------

    name : str
        Directory to save log, default: <package_dir>/../../logs/
    loglevel : ?
        Loglevel

    Returns
    -------
    instance of logger
    """
    logs_dir = get_dir(key="logs")
    os.makedirs(logs_dir, exist_ok=True)

    logger = logging.getLogger(name)  # use filename as name in log
    logger.setLevel(loglevel)

    if name:
        # create a file handler
        logfile = os.path.join(logs_dir, f"{name}.log")
        logfile = Path(logfile)

        # logfile.touch(exist_ok=True)
        handler = logging.FileHandler(logfile)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s-%(levelname)s-%(funcName)s: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logging.info("Path for logging file: %s" % logs_dir)

    # create a stream handler (print to prompt)
    stream = logging.StreamHandler()
    stream.setLevel(logging.INFO)
    stream_formatter = logging.Formatter(
        "%(asctime)s-%(levelname)s: %(message)s", "%H:%M:%S"
    )
    stream.setFormatter(stream_formatter)

    # add the handlers to the logger

    logger.addHandler(stream)

    logger.propagate = False

    logger.debug("*********************************************************")

    return logger


# def create_data_dir_tree():
#     """Create data root path, if necessary"""
#     root_path = get_data_root_dir()
#
#     # root dir does not exist
#     if not os.path.isdir(root_path):
#         # create it
#         logger.warning(f'WindNODE_ABW data root path {root_path} not found, '
#                        f'I will create it including subdirectories.')
#         os.mkdir(root_path)
#
#         # create subdirs
#         subdirs = ['config_dir', 'log_dir', 'data_dir', 'results_dir']
#         for subdir in subdirs:
#             path = os.path.join(root_path, get('user_dirs', subdir))
#             os.mkdir(path)
#
#         # copy default config files
#         config_path = os.path.join(root_path, get('user_dirs', 'config_dir'))
#         logger.info(f'I will create a default set of config files in {config_path}')
#         internal_config_dir = os.path.join(package_path, 'config')
#         for file in glob(os.path.join(internal_config_dir, '*.cfg')):
#             shutil.copy(file,
#                         os.path.join(config_path,
#                                      os.path.basename(file)
#                                      .replace('_default', '')))

# create_data_dirtree()
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
        start_time = time.time()
        result = func(*args, **kw)
        logger.info(
            "Processing time of %s(): %.2f seconds."
            % (func.__qualname__, time.time() - start_time)
        )
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


def split_yaml(yaml_file, save_to):
    """Split yaml file into multiple yamls by sections"""
    for subconfig in yaml_file:
        content = yaml.dump(
            yaml_file[subconfig], default_flow_style=False, sort_keys=False
        )
        path = save_to / f".{subconfig}.yaml"
        with open(path, "w") as file:
            file.write(content)
