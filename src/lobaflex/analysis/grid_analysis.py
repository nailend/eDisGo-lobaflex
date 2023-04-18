import logging
import os

from datetime import datetime

import papermill as pm
import nbformat as nb

from lobaflex import config_dir, logs_dir, results_dir
from lobaflex.analysis import __path__ as analysis_path
from lobaflex.tools.logger import setup_logging
from lobaflex.tools.tools import get_config, log_errors

if __name__ == "__main__":
    logger = logging.getLogger("lobaflex.analysis." + __name__)
else:
    logger = logging.getLogger(__name__)


def remove_taged_cell(path, tag):
    """Remove cell with specific tag from notebook"""

    with open(path, "r") as f:
        noteboob = nb.read(f, nb.NO_CONVERT)

    # Iterate over the cells of the notebook
    for cell in noteboob.cells:
        # Check if the cell has the specified tag
        if tag in cell.metadata.get("tags", []):
            # Delete the cell
            noteboob.cells.remove(cell)
            logger.info(f"Cell {cell['execution_count']} with tag "
                        "'delete_me' removed.")
    # Write the executed notebook without the deleted cell
    with open(path, "w") as f:
        nb.write(noteboob, f)
    logger.info("Notebook saved without deleted cell.")


@log_errors
def create_grids_notebook(
    grid_id,
    run_id,
    template="grid_analysis_template.ipynb",
    period=None,
    name=None,
    import_dir=None,
    export_dir=None,
    kernel_name=None,
    version_db=None,
):

    cfg_o = get_config(path=config_dir / ".opt.yaml")

    date = datetime.now().date().isoformat()
    logfile = logs_dir / f"{run_id}_analysis_grids_ynb_{grid_id}_{date}.log"
    setup_logging(file_name=logfile)

    # define data and paths
    input_template = os.path.join(analysis_path[0], "templates", template)

    if import_dir is None:
        grid_path = results_dir / run_id / str(grid_id)
    if export_dir is None:
        export_dir = run_id
    export_path = results_dir / export_dir / str(grid_id) / "analysis"
    os.makedirs(export_path, exist_ok=True)

    if name is None:
        name = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    # else:
    #     name = name + "_" + datetime.now().strftime("%Y-%m-%d_%H%M%S")
    export_name = f"{template.strip('.ipynb')}_{name}.ipynb"
    export_notebook = export_path / export_name

    parameters = {
        "run_id": run_id,
        "grid_id": grid_id,
        "period": period,
        "grid_path": str(grid_path),
    }

    # execute notebook with specific parameter
    logger.info(f"Execute notebook {template} for grid {grid_id}...")
    try:
        pm.execute_notebook(
            input_template,
            export_notebook,
            parameters=parameters,
            request_save_on_cell_execute=False,
            progress_bar=False,
            kernel_name=kernel_name,
        )
    # except FileNotFoundError:
    #     logger.warning(f"Template or output path not found, skipping...")
    except Exception as ex:
        logger.warning(f"An exception of type {type(ex).__name__} occurred:")
        logger.warning(ex)
        raise Exception(
            f"Notebook creation for {grid_id} of run {run_id} " "failed."
        )
    else:
        logger.info(f"Notebook created for Grid {grid_id} of run {run_id}.")

        remove_taged_cell(export_notebook, tag="delete_me")

        if version_db is not None:
            return version_db["db"]


if __name__ == "__main__":

    from lobaflex.tools.tools import split_model_config_in_subconfig

    split_model_config_in_subconfig()

    logger = logging.getLogger("lobaflex.__main__")
    date = datetime.now().date().isoformat()
    cfg_o = get_config(path=config_dir / ".opt.yaml")
    logfile = logs_dir / f"analysis_grids_ynb_{date}_local.log"
    setup_logging(file_name=logfile)

    grid_id = 1056
    run_id = "rolling_horizon_only_load_unbound_2weeks"
    create_grids_notebook(
        grid_id=grid_id,
        run_id=run_id,
        # template="grid_analysis_template.ipynb",
        template="analyse_potential.ipynb",
        period="potential",
        import_dir=None,
        export_dir=None,
        name="local",
        # kernel_name="lobaflex",
        # kernel_name="d_py3.8_edisgo-lobaflex",
        kernel_name=os.path.basename(os.environ.get("VIRTUAL_ENV")),
    )
