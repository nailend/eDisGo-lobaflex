from doit.tools import check_timestamp_unchanged, result_dep

from dnm_generation import run_dnm_generation
from emob_integration import run_emob_integration
from feeder_extraction import run_feeder_extraction
from hp_integration import run_hp_integration
from load_integration import run_load_integration
from logger import logger
from tools import get_config, get_csv_in_subdirs, get_dir, split_yaml

src_dir = get_dir(key="src")
logs_dir = get_dir(key="logs")
data_dir = get_dir(key="data")
config_dir = get_dir(key="config")

# TODO
#   1. split config file and set file dependency on dir and grids only
#   2. file dependency with timestamp otherwise to expensive (big files)
#   3. add task dependency (only if not uptodate)
#   4. alternative uptodate function: version,
#   5. add groups
#   6. clean
#   7. callback telegram bot
#   8. watch param
#   9. subclass grids


def task_split_model_config_in_subconfig():
    config_dir = get_dir(key="config")
    cfg = get_config(path=config_dir / "model_config.yaml")
    split_yaml(yaml_file=cfg, save_to=config_dir)


def task_get_config_global():
    global cfg
    cfg = get_config(path=config_dir / ".grids.yaml")


def load_integration_task(mvgd):

    import_dir = cfg["load_integration"].get("import")
    export_dir = cfg["load_integration"].get("export")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    list_files_dep = get_csv_in_subdirs(path=data_dir / import_dir / str(mvgd))
    # List of files to check if timestamp uptodate
    list_uptodate = list_files_dep
    list_uptodate += [str(config_dir / ".grids.yaml")]
    list_uptodate += [str(src_dir / "load_integration.py")]

    yield {
        "name": f"{mvgd}_load_integration",
        "actions": [
            (
                run_load_integration,
                [],  # args
                {  # kwargs
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # 'title': "title",
        # "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / ".grids.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ]
        + list_files_dep,
        "uptodate": [check_timestamp_unchanged(i) for i in list_uptodate],
        "verbosity": 2,
    }


def emob_integration_task(mvgd):
    """Import emob"""

    import_dir = cfg["emob_integration"].get("import")
    export_dir = cfg["emob_integration"].get("export")
    to_freq = cfg["emob_integration"].get("to_freq")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    list_files_dep = get_csv_in_subdirs(path=data_dir / import_dir / str(mvgd))
    # List of files to check if timestamp uptodate
    list_uptodate = list_files_dep
    list_uptodate += [str(config_dir / ".grids.yaml")]
    list_uptodate += [str(src_dir / "emob_integration.py")]
    yield {
        "name": f"{mvgd}_emob_integration",
        "actions": [
            (
                run_emob_integration,
                [],  # args
                {  # kwargs
                    "grid_id": mvgd,
                    "to_freq": to_freq,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # 'title': "title",
        # "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / ".grids.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ]
        + list_files_dep,
        "uptodate": [check_timestamp_unchanged(i) for i in list_uptodate],
        "task_dep": [f"grids:{mvgd}_load_integration"],
        "verbosity": 2,
    }


def hp_integration_task(mvgd):

    import_dir = cfg["hp_integration"].get("import")
    export_dir = cfg["hp_integration"].get("export")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    list_files_dep = get_csv_in_subdirs(path=data_dir / import_dir / str(mvgd))
    # List of files to check if timestamp uptodate
    list_uptodate = list_files_dep
    list_uptodate += [str(config_dir / ".grids.yaml")]
    list_uptodate += [str(src_dir / "hp_integration.py")]

    yield {
        "name": f"{mvgd}_hp_integration",
        "actions": [
            (
                run_hp_integration,
                [],  # args
                {  # kwargs
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # 'title': "title",
        # "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / ".grids.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ]
        + list_files_dep,
        "uptodate": [check_timestamp_unchanged(i) for i in list_uptodate],
        "task_dep": [f"grids:{mvgd}_emob_integration"],
        "verbosity": 2,
    }


def feeder_extraction_task(mvgd):

    import_dir = cfg["feeder_extraction"].get("import")
    export_dir = cfg["feeder_extraction"].get("export")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    list_files_dep = get_csv_in_subdirs(path=data_dir / import_dir / str(mvgd))
    # List of files to check if timestamp uptodate
    list_uptodate = list_files_dep
    list_uptodate += [str(config_dir / ".grids.yaml")]
    list_uptodate += [str(src_dir / "feeder_extraction.py")]

    yield {
        "name": f"{mvgd}_feeder_extraction",
        "actions": [
            (
                run_feeder_extraction,
                [],  # args
                {  # kwargs
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # 'title': "title",
        # "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / "grids.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ]
        + list_files_dep,
        "uptodate": [check_timestamp_unchanged(i) for i in list_uptodate],
        "task_dep": [f"grids:{mvgd}_hp_integration"],
        "verbosity": 2,
    }


def dnm_generation_task(mvgd):

    import_dir = cfg["dnm_generation"].get("import")
    export_dir = cfg["dnm_generation"].get("export")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    list_files_dep = get_csv_in_subdirs(path=data_dir / import_dir / str(mvgd) / "feeder")
    # List of files to check if timestamp uptodate
    list_uptodate = list_files_dep
    list_uptodate = [i for i in list_uptodate if not i.startswith("downstream")]
    list_uptodate += [str(config_dir / ".grids.yaml")]
    list_uptodate += [str(src_dir / "dnm_generation.py")]

    yield {
        "name": f"{mvgd}_dnm_generation",
        "actions": [
            (
                run_dnm_generation,
                [],
                {
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # 'title': "title",
        # "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / ".grids.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ]
        + files_dep,
        "uptodate": [check_timestamp_unchanged(i) for i in list_uptodate],
        "task_dep": [f"grids:{mvgd}_feeder_extraction"],
        "verbosity": 2,
    }




def task_grids():
    """Generate all task for Grid generation"""

    mvgds = sorted(cfg.get("mvgd"))
    logger.info(f"{len(mvgds)} MVGD's in the pipeline")

    for mvgd in mvgds:
        yield load_integration_task(mvgd)
        yield emob_integration_task(mvgd)
        yield hp_integration_task(mvgd)
        yield feeder_extraction_task(mvgd)
        yield dnm_generation_task(mvgd)


if __name__ == "__main__":
    import doit

    doit.run(globals())
