import logging
import multiprocessing as mp
import os

import pandas as pd
import papermill as pm

from numpy import inf, nan

# from edisgo import __path__ as wn_path
from windnode_abw.tools import config

logger = logging.getLogger("papermill")


def create_scenario_notebook(
    scenario,
    run_id,
    template="scenario_analysis_template.ipynb",
    output_path=os.path.join(wn_path[0], "jupy"),
    kernel_name=None,
    force_new_results=False,
):

    # define data and paths
    input_template = os.path.join(wn_path[0], "jupy", "templates", template)
    output_name = "scenario_analysis_{scenario}.ipynb".format(
        scenario=scenario
    )
    output_notebook = os.path.join(output_path, output_name)

    # execute notebook with specific parameter
    try:
        pm.execute_notebook(
            input_template,
            output_notebook,
            parameters={
                "scenario": scenario,
                "run_timestamp": run_id,
                "force_new_results": force_new_results,
            },
            request_save_on_cell_execute=True,
            kernel_name=kernel_name,
        )
    except FileNotFoundError:
        logger.warning(f"Template or output path not found, skipping...")
        return scenario
    except Exception as ex:
        logger.warning(
            f"Scenario {scenario}: An exception of type {type(ex).__name__} occurred:"
        )
        logger.warning(ex)
        logger.warning(f"Scenario {scenario} skipped...")
        return scenario
    else:
        logger.info(f"Notebook created for scenario: {scenario}...")


def create_multiple_scenario_notebooks(
    scenarios,
    run_id,
    template="scenario_analysis_template.ipynb",
    output_path=os.path.join(wn_path[0], "jupy"),
    num_processes=None,
    kernel_name=None,
    force_new_results=False,
):

    if isinstance(scenarios, str):
        scenarios = [scenarios]

    # get list of available scenarios in run id folder
    result_base_path = os.path.join(
        config.get_data_root_dir(), config.get("user_dirs", "results_dir")
    )
    avail_scenarios = [
        file.split(".")[0]
        for file in os.listdir(os.path.join(result_base_path, run_id))
        if not file.startswith(".")
    ]
    # get list of available scenarios for comparison
    all_scenarios = [
        file.split(".")[0]
        for file in os.listdir(os.path.join(wn_path[0], "scenarios"))
        if file.endswith(".scn")
    ]

    if len(all_scenarios) > len(avail_scenarios):
        logger.info(
            f"Available scenarios ({len(avail_scenarios)}) in run "
            f"{run_id} differ from the total number of scenarios "
            f"({len(all_scenarios)})."
        )

    # create scenario list
    if scenarios == ["all"]:
        scenarios = avail_scenarios

    logger.info(
        f"Creating notebooks for {len(scenarios)} scenarios in {output_path} ..."
    )

    pool = mp.Pool(processes=num_processes)

    errors = None
    for scen in scenarios:
        pool.apply_async(
            create_scenario_notebook,
            args=(
                scen,
                run_id,
                template,
            ),
            kwds={
                "output_path": output_path,
                "kernel_name": kernel_name,
                "force_new_results": force_new_results,
            },
        )
    pool.close()
    pool.join()

    if errors is not None:
        logger.warning(f"Errors occured during creation of notebooks.")
    else:
        logger.info(
            f"Notebooks for {len(scenarios)} scenarios created without errors."
        )


if __name__ == "__main__":

    run_timestamp = "2020-08-18_082418"

    create_scenario_notebook(
        scenario="StatusQuo_DSMno",
        run_id=run_timestamp,
        kernel_name="python3",
        # force_new_results=False
    )

    # scenarios = ['StatusQuo_DSMno', 'ISE2050', 'NEP2035']

    # create_multiple_scenario_notebooks(
    #     scenarios=scenarios,
    #     run_id=run_timestamp,
    #     template="scenario_analysis_template.ipynb",
    #     output_path='/home/local/RL-INSTITUT/julian.endres/Projekte/WindNODE_ABW/windnode_abw/jupy/executed_notebooks',
    #     num_processes=None,
    #     kernel_name='python3',
    #     # kernel_name='conda-env-windnode_abw-py',
    #     force_new_results=False
    # )

    print("DONE")
