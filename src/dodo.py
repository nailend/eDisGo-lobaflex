from dnm_generation import run_dnm_generation
from emob_integration import run_emob_integration
from feeder_extraction import run_feeder_extraction
from hp_integration import run_hp_integration
from load_integration import run_load_integration
from logger import logger
from tools import get_config, get_dir, split_yaml

logs_dir = get_dir(key="logs")
data_dir = get_dir(key="data")
config_dir = get_dir(key="config")
cfg = get_config(path=config_dir / "model_config.yaml")

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


def set_target(task, values):
    version = task.options.get('version', '0')
    task.targets.append(f'targetfile{version}')


def load_integration_task(mvgd):

    import_dir = cfg["grid_generation"]["load_integration"].get("import")
    export_dir = cfg["grid_generation"]["load_integration"].get("export")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    files_list = list((data_dir / import_dir / str(mvgd)).glob("*.csv"))

    yield {
        "basename": f"load_integration_mvgd-{mvgd}",
        "actions": [
            (
                run_load_integration,
                [],
                {
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # 'title': "title",
        "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / "model_config.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ] + files_list,
        # 'uptodate': [set_target],
        "verbosity": 2,
    }


def emob_integration_task(mvgd):

    import_dir = cfg["grid_generation"]["emob_integration"].get("import")
    export_dir = cfg["grid_generation"]["emob_integration"].get("export")
    to_freq = cfg["grid_generation"]["emob_integration"].get("to_freq")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    files_list = list((data_dir / import_dir / str(mvgd)).glob("*.csv"))

    yield {
        "basename": f"emob_integration_mvgd-{mvgd}",
        "actions": [
            (
                run_emob_integration,
                [],
                {
                    "grid_id": mvgd,
                    "to_freq": to_freq,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # 'title': "title",
        "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / "model_config.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ]+files_list,
        "verbosity": 2,
    }


def hp_integration_task(mvgd):

    import_dir = cfg["grid_generation"]["hp_integration"].get("import")
    export_dir = cfg["grid_generation"]["hp_integration"].get("export")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    files_list = list((data_dir / import_dir / str(mvgd)).glob("*.csv"))

    yield {
        "basename": f"hp_integration_mvgd-{mvgd}",
        "actions": [
            (
                run_hp_integration,
                [],
                {
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # 'title': "title",
        "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / "model_config.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ]+files_list,
        "verbosity": 2,
    }


def feeder_extraction_task(mvgd):

    import_dir = cfg["grid_generation"]["feeder_extraction"].get("import")
    export_dir = cfg["grid_generation"]["feeder_extraction"].get("export")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    files_list = list((data_dir / import_dir / str(mvgd)).glob("*.csv"))

    yield {
        "basename": f"feeder_extraction_mvgd-{mvgd}",
        "actions": [
            (
                run_feeder_extraction,
                [],
                {
                    "grid_id": mvgd,
                    "doit": True,
                    "save": True,
                },
            )
        ],
        # 'title': "title",
        "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / "model_config.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ] + files_list,
        "verbosity": 2,
    }


def dnm_generation_task(mvgd):

    import_dir = cfg["grid_generation"]["dnm_generation"].get("import")
    export_dir = cfg["grid_generation"]["dnm_generation"].get("export")

    # Metadata describing the generated data.
    target_path = data_dir / export_dir / str(mvgd) / "metadata.md"
    # List of files which the task depends on
    files_list = list((data_dir / import_dir / str(mvgd) / "feeder").glob(
                                                           "*.csv"))

    yield {
        "basename": f"dnm_generation_mvgd-{mvgd}",
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
        "doc": "docs for X",
        "targets": [target_path],
        "file_dep": [
            config_dir / "model_config.yaml",
            data_dir / import_dir / str(mvgd) / "metadata.md",
        ] + files_list,
        "verbosity": 2,
    }


def task_grid_generation():

    multi_grid_ids = sorted(cfg["model"].get("multi_grid_ids"))
    logger.info(f"{len(multi_grid_ids)} MVGD's in the pipeline")

    for mvgd in multi_grid_ids:
        yield load_integration_task(mvgd)
        yield emob_integration_task(mvgd)
        yield hp_integration_task(mvgd)
        yield feeder_extraction_task(mvgd)
        yield dnm_generation_task(mvgd)


if __name__ == "__main__":
    import doit

    doit.run(globals())
