import os
import re

import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio

from edisgo.edisgo import import_edisgo_from_files
from plotly.subplots import make_subplots

from lobaflex.tools.tools import get_files_in_subdirs

# comment to make plots interactive
# pio.renderers.default = "svg"


colors_dict = {
    "load": "#db3b3c",  # red
    "reactive_load": "#951b1c",  # darkred
    "feedin": "#4bce4b",  # green
    "reactive_feedin": "#1c641c",  # darkgreen
    "hp": "#8c564b",  # brown
    "ev": "#1f77b4",  # blue
    "tes": "#d6aa27",  # orange
    "hp_el": "#8c564b",  # brown
    "p_cum_neg": "#ffc0cb",  # pink
    "v_neg": "#41ffde",  # lightcyan
    "hp_opt": "#8B4513",
    "hp_reference": "#D2691E",
    "ev_opt": "#0071C5",
    "ev_reference": "#1E90FF",
    "residual_load": "grey",
    "PV": "#e2c319",
    "electric vehicles": "#1f77b4",
    "conventional load": "#8c564b",
    "heat pump": "#ff0800",
    "biomass": "#2ca02c",
    "hydropower": "#4a19e5",
    "wind": "#73baec",
}


def custom_round(x, base=0.001):
    """

    Parameters
    ----------
    x :
    base :

    Returns
    -------

    """
    return base * np.ceil(float(x) / base)


def get_all_attribute_values_for_keyword(results_path, keyword):
    """

    Parameters
    ----------
    results_path :
    keyword :

    Returns
    -------

    """
    objectives = [
        "maximize_grid_power",
        "minimize_grid_power",
        "maximize_energy_level",
        "minimize_energy_level",
    ]

    # colors_dict = {
    #     "load": "#db3b3c",  # red
    #     "reactive_load": "#951b1c",  # darkred
    #     "feedin": "#4bce4b",  # green
    #     "reactive_feedin": "#1c641c",  # darkgreen
    #     "hp": "#8c564b",  # brown
    #     "ev": "#1f77b4",  # blue
    #     "tes": "#d6aa27",  # orange
    #     "hp_el": "#8c564b",  # brown
    #     "p_cum_neg": "#ffc0cb",  # pink
    #     "v_neg": "#41ffde",  # lightcyan
    # }

    data = dict().fromkeys(objectives)

    list_of_results = get_files_in_subdirs(path=results_path, pattern="*.csv")
    selected_list = [i for i in list_of_results if "concat" in i]

    if "initial" in keyword:
        keyword_files = [i for i in selected_list if keyword in i]
    else:
        keyword_files = [
            i
            for i in selected_list
            if keyword in i and not "slack_initial" in i
        ]

    for obj in objectives:
        obj_files = [i for i in keyword_files if obj in i]
        files = [i for i in obj_files if keyword in i]

        obj_dict = {}
        for i, file in enumerate(files):
            attr = re.findall(rf"{keyword}_(.*).csv", file)[0]
            df = pd.read_csv(file, index_col=0, parse_dates=True)
            df = df.sum(axis=1).rename(f"{keyword} {attr} [MW]")
            obj_dict[attr] = df
        data[obj] = obj_dict

    return data


def plot_all_attributes_for_keyword(
    results_path,
    keyword,
    edisgo_obj=None,
    timeframe=None,
    objectives=None,
    title=None,
):
    """

    Parameters
    ----------
    results_path :
    keyword :
    edisgo_obj :
    timeframe :
    objectives :
    title :

    Returns
    -------

    """
    list_of_results = get_files_in_subdirs(path=results_path, pattern="*.csv")
    selected_list = [i for i in list_of_results if "concat" in i]

    if edisgo_obj is not None:

        if timeframe is None:
            timeframe = edisgo_obj.timeseries.timeindex

        p_set = (
            edisgo_obj.topology.loads_df.loc[
                edisgo_obj.topology.loads_df["opt"]
            ]
            .groupby("type")["p_set"]
            .sum()
        )
        upper_power = (
            edisgo_obj.electromobility.flexibility_bands["upper_power"]
            .loc[timeframe]
            .sum(axis=1)
        )

    # objectives = [
    #     "maximize_grid_power",
    #     "minimize_grid_power",
    #     "maximize_energy_level",
    #     "minimize_energy_level",
    # ]
    if objectives is None:

        if "charging" in keyword:
            objectives = [
                "maximize_grid_power",
                "minimize_grid_power",
            ]
        elif "energy" in keyword:
            objectives = [
                "maximize_energy_level",
                "minimize_energy_level",
            ]
        else:
            objectives = [
                "maximize_grid_power",
                "minimize_grid_power",
                "maximize_energy_level",
                "minimize_energy_level",
            ]

    # colors_dict = {
    #     "load": "#db3b3c",  # red
    #     "reactive_load": "#951b1c",  # darkred
    #     "feedin": "#4bce4b",  # green
    #     "reactive_feedin": "#1c641c",  # darkgreen
    #     "hp": "#8c564b",  # brown
    #     "ev": "#1f77b4",  # blue
    #     "tes": "#d6aa27",  # orange
    #     "hp_el": "#8c564b",  # brown
    #     "p_cum_neg": "#ffc0cb",  # pink
    #     "v_neg": "#41ffde",  # lightcyan
    # }

    keyword_files = [
        i for i in selected_list if keyword in i and "slack_initial" not in i
    ]

    if len(keyword_files) == 0:
        print(f"No {keyword} values.")

    else:

        fig = make_subplots(
            rows=len(objectives),
            subplot_titles=objectives,
            vertical_spacing=0.05,
        )

        for subplot, obj in enumerate(objectives):
            obj_files = [i for i in keyword_files if obj in i]
            files = [i for i in obj_files if keyword in i]

            if len(files) == 0:
                fig.add_annotation(
                    x=2,
                    y=5,
                    text=f"No values",
                    showarrow=False,
                    font=dict(size=16),
                    row=subplot + 1,
                    col=1,
                )
                continue

            for i, file in enumerate(files):
                attr = re.findall(rf"{keyword}_(.*).csv", file)[0]
                df = pd.read_csv(file, index_col=0, parse_dates=True)
                df = df.sum(axis=1).rename(f"{keyword} {attr} [MW]")

                if timeframe is None:
                    # dont ues timesteps if timeframe not given
                    # probably split timeseries
                    dates = list(range(len(df.index)))
                else:
                    #
                    dates = timeframe
                    df = df.loc[timeframe]

                # correct values to first timestep of lower band
                if "energy_level" in keyword and edisgo_obj is not None:
                    if attr == "ev":
                        df = (
                            df
                            - edisgo_obj.electromobility.flexibility_bands[
                                "lower_energy"
                            ]
                            .sum(axis=1)
                            .loc[timeframe]
                            .iloc[0]
                        )

                trace = go.Scatter(
                    x=dates,
                    y=df,
                    mode="lines",
                    name=attr,
                    line_color=colors_dict.get(attr, None),
                    # showlegend=True if not subplot else False,
                    # legendgroup=attr,
                    legendgroup=obj,
                    # legendgrouptitle_text=attr,
                    legendgrouptitle_text=obj,
                )
                fig.add_trace(
                    trace,
                    row=subplot + 1,
                    col=1,
                )

            if keyword == "charging" and edisgo_obj is not None:
                # draw p_set line
                trace = go.Scatter(
                    x=dates,
                    y=len(dates) * [p_set["heat_pump"]],
                    mode="lines",
                    name="hp p_set",
                    line_color=colors_dict.get("hp_el", None),
                    line=dict(dash="dot"),
                    # showlegend=True if not subplot else False,
                    legendgroup="hp p_set",
                    # legendgroup=obj,
                    legendgrouptitle_text=obj,
                )
                fig.add_trace(
                    trace,
                    row=subplot + 1,
                    col=1,
                )

                # draw ev upper power line
                trace = go.Scatter(
                    x=dates,
                    y=upper_power,
                    mode="lines",
                    name="ev upper_power",
                    line_color=colors_dict.get("ev", None),
                    line=dict(dash="dot"),
                    # showlegend=True if not subplot else False,
                    legendgroup="ev upper power",
                    # legendgroup=obj,
                    legendgrouptitle_text=obj,
                )
                fig.add_trace(
                    trace,
                    row=subplot + 1,
                    col=1,
                )

        fig.update_layout(
            # title=f"{keyword}: {run_id} - {grid_id}",
            title=f"{keyword}: " + "" if title is None else title,
            width=1000,
            height=len(objectives) * 500,
            # height=2000,
            margin=dict(t=30, b=30, l=30, r=30),
            showlegend=True,
        )
        if "energy" in keyword:
            ylabel = "Energy in MWh"
        else:
            ylabel = "Power in MWh"
        fig.update_yaxes(title_text=ylabel)

        fig.show()


def heatmap_energy_level(results_path, keyword, edisgo_obj=None, title=None):
    """

    Parameters
    ----------
    results_path :
    keyword :
    edisgo_obj :
    title :

    Returns
    -------

    """
    list_of_results = get_files_in_subdirs(path=results_path, pattern="*.csv")
    selected_list = [i for i in list_of_results if "concat" in i]
    keyword_files = [
        i for i in selected_list if keyword in i and "slack_initial" not in i
    ]

    if len(keyword_files) == 0:
        print(f"No {keyword} values.")

    else:

        pattern = rf"potential/\w+/(.*)/concat/\d+_energy_level_(.*).csv"
        keys = [re.search(pattern, string=i).groups() for i in keyword_files]

        files = pd.DataFrame(
            keys, index=keyword_files, columns=["objective", "attribute"]
        ).sort_values(["attribute", "objective"])

        subplot_titles = (
            files["objective"].values + "_" + files["attribute"].values
        )

        fig = make_subplots(
            rows=len(files),
            subplot_titles=subplot_titles,
            vertical_spacing=0.05,
        )

        traces = []
        for i, file in enumerate(files.index):
            attr = re.findall(rf"{keyword}_(.*).csv", file)[0]
            df = pd.read_csv(file, index_col=0, parse_dates=True)
            df = df.sum(axis=1).rename(f"{keyword} {attr} [MW]")

            # correct values to first timestep of lower band
            if "energy_level" in keyword and edisgo_obj is not None:
                if attr == "ev":
                    df = (
                        df
                        - edisgo_obj.electromobility.flexibility_bands[
                            "lower_energy"
                        ]
                        .sum(axis=1)
                        .iloc[0]
                    )

            data = [df.iloc[i:].loc[::24] for i in range(24)]
            days = df.index.day.unique()
            days = [dt.strftime("%d.%m") for dt in np.unique(df.index.date)]
            trace = go.Heatmap(
                z=data, x=days, colorscale="Viridis", showscale=False
            )
            fig.add_trace(trace, row=i + 1, col=1)

        fig.update_layout(
            # title=f"{keyword}: {run_id} - {grid_id}",
            title=f"{keyword}: " + "" if title is None else title,
            width=1000,
            height=2000,
            margin=dict(t=30, b=30, l=30, r=30),
            showlegend=True,
            coloraxis_showscale=True,
        )
        fig.update_yaxes(title_text="hour of day", tickmode="linear", dtick=4)
        fig.update_xaxes(title_text="day of month", tickmode="linear", dtick=1)

        fig.show()


def plot_optimized_dispatch(edisgo_obj, timeframe, title=None):
    """

    Parameters
    ----------
    edisgo_obj :
    timeframe :
    title :

    Returns
    -------

    """
    # define components
    # res_generators = edisgo_obj.topology.generators_df[
    #     edisgo_obj.topology.generators_df["type"].isin(["solar", "wind"])
    # ].index

    flexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["opt"] == True
    ].index

    # inflexible_loads = edisgo_obj.topology.loads_df.loc[
    #     edisgo_obj.topology.loads_df["opt"] == False
    # ].index

    flexible_hp = [i for i in flexible_loads if "HP" in i]
    flexible_cp = [i for i in flexible_loads if "Charging_Point" in i]

    fig = go.Figure()

    df_hp = edisgo_obj.timeseries.loads_active_power.loc[
        timeframe, flexible_hp
    ].sum(axis=1)

    df_ev = edisgo_obj.timeseries.loads_active_power.loc[
        timeframe, flexible_cp
    ].sum(axis=1)

    df_residual = edisgo_obj.timeseries.residual_load.loc[timeframe]

    fig.add_trace(
        go.Scatter(
            mode="lines",
            name="Electric Vehicles",
            x=timeframe,
            y=df_ev,
            marker=dict(color=colors_dict["ev_reference"]),
            stackgroup="one",
            yaxis="y1",
        )
    )

    fig.add_trace(
        go.Scatter(
            mode="lines",
            name="Heat pumps",
            x=timeframe,
            y=df_hp,
            marker=dict(color=colors_dict["hp_reference"]),
            stackgroup="one",
            yaxis="y1",
        )
    )

    fig.add_trace(
        go.Scatter(
            mode="lines",
            # name="Feedin - fix_load",
            name="Residual load",
            x=timeframe,
            y=df_residual,
            marker=dict(color=colors_dict["residual_load"]),
            #     stackgroup='two',
            yaxis="y2",
        )
    )

    df_load = df_hp + df_ev

    range_load = np.ceil(df_load.max().max()) - np.floor(df_load.min().min())

    range_residual = np.ceil(df_residual.max()) - np.floor(df_residual.min())
    scaleratio = custom_round(x=range_residual / range_load, base=5)

    all_max = max(
        df_load.max().max(),
        df_residual.max() / scaleratio,
    )
    all_min = min(
        df_load.min().min(),
        df_residual.min() / scaleratio,
    )

    fig.update_layout(
        # title=f"Charging: {run_id} - {grid_id}",
        title=f"Optimized dispatch: " + "" if title is None else title,
        width=1000,
        height=600,
        yaxis_title="MW",
        xaxis_title="timesteps",
        showlegend=True,
        # yaxis=dict(title="Load Power in MW"),
        # yaxis2=dict(title="Feedin Power in MW", overlaying="y", side="right"),
        yaxis=dict(
            title="Load Power in MW",
            zeroline=True,
            zerolinecolor="grey",
            zerolinewidth=1,
            scaleanchor="y2",
            range=[all_min, all_max],
            scaleratio=scaleratio,
        ),
        yaxis2=dict(
            title="Residual load in MW",
            overlaying="y1",
            side="right",
            zeroline=True,
            zerolinecolor="grey",
            zerolinewidth=1,
            range=[all_min * scaleratio, all_max * scaleratio],
            scaleanchor="y2",
        ),
    )

    fig.show()
    return fig


def plot_scenario_potential(
    potential_path, objectives, keyword, technology, timeframe, title=None
):
    """

    Parameters
    ----------
    potential_path :
    objectives :
    keyword :
    technology :
    timeframe :
    title :

    Returns
    -------

    """
    scenarios = [
        "100_pct_reinforced",
        "80_pct_reinforced",
        "60_pct_reinforced",
        "40_pct_reinforced",
        "20_pct_reinforced",
        "minimize_loading",
    ]
    scenarios.reverse()

    # Define the base color and the number of traces
    base_color = "rgb(0, 0, 255)"
    num_traces = len(scenarios) + 1

    # Create a list of colors with decreasing brightness
    # colors = [f'rgb({int(255 - i*50)}, {int(255 - i*50)}, {255})' for i in range(num_traces)]
    colors = [f"rgb({i}, {i}, {255})" for i in np.linspace(0, 255, num_traces)]

    fig = go.Figure()

    # #     # add opt disptach
    # # import opimized grid
    edisgo_obj = import_edisgo_from_files(
        potential_path.parent / "minimize_loading" / "mvgd",
        import_topology=True,
        import_timeseries=True,
        import_heat_pump=True,
        import_electromobility=True,
    )
    flexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["opt"] == True
    ].index

    flexible_hp = [i for i in flexible_loads if "HP" in i]
    flexible_cp = [i for i in flexible_loads if "Charging_Point" in i]
    #
    # heta_demand = (
    #     edisgo_obj.heat_pump.heat_demand_df.loc[timeframe, flexible_hp]
    #     .sum(axis=1)
    #     .rename("heat_demand")
    # )
    # residual_load = (
    #     edisgo_obj.timeseries.residual_load.loc[timeframe]
    #     .rename("residual_load")
    # )
    #
    # fig.add_trace(
    #     go.Scatter(
    #         mode="lines",
    #         name="heat_demand",
    #         x=timeframe,
    #         # y=edisgo_obj.timeseries.loads_active_power.loc[timeframe, flexible_cp].sum(axis=1),
    #         # y=heta_demand,
    #         y=residual_load,
    #         line=dict(color="black"),
    #     )
    # )

    for legend, objective in enumerate(objectives):

        # add max power per scenario
        for i, scenario in enumerate(scenarios):
            scenario_path = potential_path / scenario / objective / "concat"
            if os.path.isdir(scenario_path):
                files_in_path = get_files_in_subdirs(
                    path=scenario_path, pattern="*.csv"
                )

                keyword_files = [
                    i
                    for i in files_in_path
                    if keyword in i and "slack_initial" not in i
                ]

                file = [i for i in keyword_files if technology in i]
                if len(file) == 0:
                    continue
                file = file[0]
                attr = re.findall(rf"{keyword}_(.*).csv", file)[0]
                df = pd.read_csv(file, index_col=0, parse_dates=True)
                df = df.sum(axis=1).rename(f"{keyword} {attr} [MW]")
                df = df.loc[timeframe]

                # if "energy" in keyword and technology == "ev":
                #     df = df - df.iloc[0]

                fig.add_trace(
                    go.Scatter(
                        mode="lines",
                        #             opacity=0.3,
                        #             fill='tozeroy',
                        #             fill="tonexty",
                        fill=None if i == 0 else "tonexty",
                        name=scenario,
                        x=timeframe,
                        y=df,
                        line=dict(color=colors[i + 1]),
                        # showlegend=True if not subplot else False,
                        showlegend=True if legend == 0 else False,
                        legendgroup=scenario,
                    )
                )
            else:
                continue

    upper_limit = None
    lower_limit = None
    if technology == "ev":
        if "charging" in keyword:
            upper_limit = (
                edisgo_obj.electromobility.flexibility_bands["upper_power"]
                .loc[timeframe]
                .sum(axis=1)
            )
        elif "energy" in keyword:
            lower_limit = (
                edisgo_obj.electromobility.flexibility_bands["lower_energy"]
                .loc[timeframe]
                .sum(axis=1)
            )

            upper_limit = (
                edisgo_obj.electromobility.flexibility_bands["upper_energy"]
                .loc[timeframe]
                .sum(axis=1)
            )

    elif technology == "hp_el":
        upper_limit = edisgo_obj.topology.loads_df.groupby("type")[
            "p_set"
        ].sum()["heat_pump"]
        upper_limit = len(timeframe) * [upper_limit]

    elif technology == "tes":
        upper_limit = edisgo_obj.heat_pump.thermal_storage_units_df.loc[
            :, "capacity"
        ].sum()
        upper_limit = len(timeframe) * [upper_limit]

    if upper_limit is not None:
        fig.add_trace(
            go.Scatter(
                mode="lines",
                name="upper limit",
                x=timeframe,
                y=upper_limit,
            )
        )
    if lower_limit is not None:
        fig.add_trace(
            go.Scatter(
                mode="lines",
                name="lower limit",
                x=timeframe,
                y=lower_limit,
            )
        )

    fig.update_layout(
        # title=f"{keyword}: {run_id} - {grid_id}",
        title=f"{str(' + ').join(objectives)}: {keyword} - {technology}" + ""
        if title is None
        else title,
        width=1000,
        height=500,
        # height=2000,
        margin=dict(t=30, b=30, l=30, r=30),
        showlegend=True,
    )
    if "energy" in keyword:
        ylabel = "Energy in MWh"
    else:
        ylabel = "Power in MWh"
    fig.update_yaxes(title_text=ylabel)

    fig.show()


def plot_compare_optimization_to_reference(grid_path, timeframe):
    """

    Parameters
    ----------
    grid_path :
    timeframe :

    Returns
    -------

    """
    # import opimized grid
    edisgo_obj = import_edisgo_from_files(
        grid_path / "minimize_loading" / "mvgd",
        import_topology=True,
        import_timeseries=True,
        import_heat_pump=True,
        import_electromobility=True,
    )

    # define components
    # res_generators = edisgo_obj.topology.generators_df[
    #     edisgo_obj.topology.generators_df["type"].isin(["solar", "wind"])
    # ].index

    flexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["opt"] == True
    ].index

    # inflexible_loads = edisgo_obj.topology.loads_df.loc[
    #     edisgo_obj.topology.loads_df["opt"] == False
    # ].index

    flexible_hp = [i for i in flexible_loads if "HP" in i]
    flexible_cp = [i for i in flexible_loads if "Charging_Point" in i]

    hp_optimized = (
        edisgo_obj.timeseries.loads_active_power.loc[timeframe, flexible_hp]
        .sum(axis=1)
        .rename("hp_opt")
    )
    ev_optimized = (
        edisgo_obj.timeseries.loads_active_power.loc[timeframe, flexible_cp]
        .sum(axis=1)
        .rename("ev_opt")
    )
    #    ev_optimized = ev_optimized + hp_optimized.values
    hp_optimized = hp_optimized + ev_optimized.values
    # import reference grid
    edisgo_obj = import_edisgo_from_files(
        grid_path / "initial" / "mvgd",
        import_topology=False,
        import_timeseries=True,
        import_heat_pump=False,
        import_electromobility=False,
    )

    hp_reference = (
        edisgo_obj.timeseries.loads_active_power.loc[timeframe, flexible_hp]
        .sum(axis=1)
        .rename("hp_reference")
    )
    ev_reference = (
        edisgo_obj.timeseries.loads_active_power.loc[timeframe, flexible_cp]
        .sum(axis=1)
        .rename("ev_reference")
    )
    #    ev_reference = ev_reference + hp_reference.values
    hp_reference = hp_reference + ev_reference.values
    residual_load = edisgo_obj.timeseries.residual_load.loc[timeframe].rename(
        "residual_load"
    )
    df = pd.concat(
        [
            residual_load,
            ev_optimized,
            hp_optimized,
            ev_reference,
            hp_reference,
        ],
        axis=1,
    )

    # colordict = {
    #     "hp_opt": "#8B4513",
    #     "hp_reference": "#D2691E",
    #     "ev_opt": "#0071C5",
    #     "ev_reference": "#1E90FF",
    #     "residual_load": "grey",
    # }

    filldict = {
        #        "hp_opt": "tozeroy",
        "hp_opt": "tonexty",
        #        "hp_reference": "tonexty",
        #        "ev_opt": "tozeroy",
        "ev_opt": "tonexty",
        #        "ev_reference": "tonexty",
        #        "residual_load": "tozeroy",
    }
    patterndict = {
        "hp_opt": None,
        "hp_reference": "/",
        "ev_opt": None,
        "ev_reference": "/",
        "residual_load": None,
    }
    stacked_dict = {
        "hp_opt": "opt",
        "hp_reference": "ref",
        "ev_opt": "opt",
        "ev_reference": "ref",
        "residual_load": "res",
    }
    dash_dict = {
        "hp_opt": None,
        #        "hp_reference": "dot",
        "ev_opt": None,
        #        "ev_reference": "dot",
        "residual_load": None,
    }

    dashdict = {
        "hp_opt": None,
        "hp_reference": "dash",
        "ev_opt": None,
        "ev_reference": ".",
        "residual_load": "dash",
    }

    fig = go.Figure()

    for name, ts in df.items():

        fig.add_trace(
            go.Scatter(
                mode="lines",
                line=dict(
                    color=colors_dict.get(name, None),
                    dash=dash_dict.get(name, None),
                ),
                # fill= "tozeroy" if name == "residual_load" else None,
                fill=filldict.get(name, None),
                #                opacity=0.3,
                fillpattern=dict(shape=patterndict.get(name, None)),
                #                stackgroup=stacked_dict.get(name, None),
                name=name,
                x=timeframe,
                y=ts,
                yaxis="y2" if name == "residual_load" else "y1",
            )
        )

    # define scaleratio & axis range
    range_load = np.ceil(
        df.drop(columns="residual_load").max().max()
    ) - np.floor(df.drop(columns="residual_load").min().min())

    range_residual = np.ceil(df["residual_load"].max()) - np.floor(
        df["residual_load"].min()
    )
    scaleratio = custom_round(x=range_residual / range_load, base=5)

    all_max = max(
        df.drop(columns="residual_load").max().max(),
        df["residual_load"].max() / scaleratio,
    )
    all_min = min(
        df.drop(columns="residual_load").min().min(),
        df["residual_load"].min() / scaleratio,
    )

    fig.update_layout(
        width=1000,
        height=600,
        #        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="MW",
        xaxis_title="timesteps",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=1.1,
            xanchor="center",
            x=0.5,
            itemsizing="constant",
        ),
        yaxis=dict(
            title="Load Power in MW",
            zeroline=True,
            zerolinecolor="grey",
            zerolinewidth=1,
            scaleanchor="y2",
            range=[all_min, all_max],
            scaleratio=scaleratio,
        ),
        yaxis2=dict(
            title="Residual load in MW",
            overlaying="y1",
            side="right",
            zeroline=True,
            zerolinecolor="grey",
            zerolinewidth=1,
            range=[all_min * scaleratio, all_max * scaleratio],
            # scaleanchor='y2',
        ),
    )

    fig.update_yaxes(
        title_font=dict(size=16),
        titlefont=dict(size=18),
        tickfont=dict(size=16),
    )
    fig.update_xaxes(
        title_font=dict(size=16),
        titlefont=dict(size=18),
        tickfont=dict(size=16),
    )
    fig.show()
    return fig


def plot_flex_capacities(edisgo_obj):
    """

    Parameters
    ----------
    edisgo_obj :

    Returns
    -------

    """
    flexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["opt"] == True
    ].index
    inflexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["opt"] == False
    ].index

    flexible_hp = [i for i in flexible_loads if "HP" in i]
    flexible_cp = [i for i in flexible_loads if "Charging_Point" in i]
    inflexible_cp = [i for i in inflexible_loads if "Charging_Point" in i]

    df_cap_hp = (
        edisgo_obj.topology.loads_df.loc[
            edisgo_obj.topology.loads_df["type"] == "heat_pump"
        ]["p_set"]
        * 1e3
    )

    df_cap_tes = (
        edisgo_obj.heat_pump.thermal_storage_units_df["capacity"] * 1e3
    )

    df_cap_ev = (
        edisgo_obj.topology.loads_df.loc[
            edisgo_obj.topology.loads_df["type"] == "charging_point"
        ]["p_set"]
        * 1e3
    )

    subplot_titles = ["HP Capacity", "TES Capacity", "EV Capacity"]
    fig = make_subplots(
        cols=3,
        subplot_titles=subplot_titles,
        vertical_spacing=0.2,
        specs=[[{}, {}, {"secondary_y": True}]],
    )

    fig.add_trace(
        go.Violin(
            y=df_cap_hp,
            name="",
            fillcolor=colors_dict["hp"],
            line_color="black",
            box_visible=True,
        ),
        col=1,
        row=1,
    )
    fig.add_trace(
        go.Violin(
            y=df_cap_tes,
            name="",
            hoverinfo="y+name",
            fillcolor=colors_dict["tes"],
            line_color="black",
            box_visible=True,
        ),
        col=2,
        row=1,
    )
    fig.add_trace(
        go.Violin(
            y=df_cap_ev.loc[flexible_cp],
            name="home+work",
            hoverinfo="y+name",
            fillcolor=colors_dict["ev"],
            line_color="black",
            box_visible=True,
        ),
        col=3,
        row=1,
    )
    fig.add_trace(
        go.Violin(
            y=df_cap_ev.loc[inflexible_cp],
            name="public+hpc",
            hoverinfo="y+name",
            fillcolor=colors_dict["ev"],
            line_color="black",
            box_visible=True,
        ),
        col=3,
        row=1,
        secondary_y=True,
    )

    fig.update_layout(
        width=1000,
        height=500,
        showlegend=False,
        yaxis=dict(title="kW"),
        yaxis2=dict(title="kWh"),
        yaxis3=dict(title="kWh"),
        yaxis4=dict(title="kWh", overlaying="y3", side="right"),
    )
    fig.show()
    return fig


def identify_cop_bug(results_path, edisgo_obj, timeframe):
    """

    Parameters
    ----------
    results_path :
    edisgo_obj :
    timeframe :

    Returns
    -------

    """
    grid_id = edisgo_obj.topology.mv_grid.id

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            mode="lines",
            y=edisgo_obj.heat_pump.cop_df.mean(axis=1).loc[timeframe],
            x=timeframe,
            yaxis="y1",
            name="COP",
        )
    )
    fig.add_trace(
        go.Scatter(
            mode="lines",
            y=edisgo_obj.timeseries.residual_load.loc[timeframe],
            x=timeframe,
            yaxis="y2",
            name="residual load",
        )
    )

    df = get_all_attribute_values_for_keyword(
        results_path=results_path,
        keyword=f"{grid_id}_energy_level",
    )

    fig.add_trace(
        go.Scatter(
            mode="lines",
            y=df["maximize_energy_level"]["tes"].loc[timeframe],
            x=timeframe,
            yaxis="y2",
            name="tes energy level",
        )
    )

    df = get_all_attribute_values_for_keyword(
        results_path=results_path,
        keyword=f"{grid_id}_grid_power",
    )

    fig.add_trace(
        go.Scatter(
            mode="lines",
            y=df["maximize_energy_level"]["flexible"].loc[timeframe],
            x=timeframe,
            yaxis="y2",
            name="grid_power_flexible",
        )
    )

    df = get_all_attribute_values_for_keyword(
        results_path=results_path,
        keyword=f"{grid_id}_grid_cumulated",
    )

    fig.add_trace(
        go.Scatter(
            mode="lines",
            y=df["maximize_energy_level"]["energy"].loc[timeframe],
            x=timeframe,
            yaxis="y2",
            name="grid_cumulated_energy",
        )
    )

    fig.update_layout(
        title=f"maximize energy level - period: {timeframe.name}",
        width=1000,
        height=600,
        yaxis1=dict(title="COP"),
        yaxis2=dict(
            zeroline=True,
            zerolinecolor="black",
            zerolinewidth=1,
            title="Alles andere in MW MWH",
            overlaying="y1",
            side="right",
        ),
    )
    fig.show()


def get_all_reinforcement_measures(grid_path):
    """

    Parameters
    ----------
    grid_path :

    Returns
    -------

    """

    edisgo_paths = [grid_path / "reference" / "reinforced"]
    edisgo_paths += [grid_path / "minimize_loading" / "reinforced"]
    scenarios = [f"{i}_pct_reinforced" for i in [20, 40, 60, 80, 100]]
    edisgo_paths += [
        grid_path / "scenarios" / scenario / "mvgd" for scenario in scenarios
    ]

    df_costs = []
    scenarios = []
    for edisgo_path in edisgo_paths:
        try:
            edisgo_obj = import_edisgo_from_files(
                edisgo_path,
                import_topology=False,
                import_timeseries=False,
                import_heat_pump=False,
                import_electromobility=False,
                import_results=True,
            )
        except:
            continue

        #     costs += edisgo_obj.results.grid_expansion_costs['total_costs'].sum()
        costs = pd.concat(
            [
                edisgo_obj.results.grid_expansion_costs.groupby(
                    "voltage_level"
                )["quantity"]
                .sum()
                .to_frame(),
                edisgo_obj.results.grid_expansion_costs.groupby(
                    "voltage_level"
                )["total_costs"]
                .sum()
                .to_frame(),
            ],
            axis=1,
        )  # .T.stack().round()

        df_costs += [
            pd.concat(
                [
                    costs,
                    pd.Series(
                        [
                            edisgo_obj.results.grid_expansion_costs[
                                "quantity"
                            ].sum(),
                            edisgo_obj.results.grid_expansion_costs[
                                "total_costs"
                            ].sum(),
                        ],
                        index=["quantity", "total_costs"],
                        name="all",
                    )
                    .to_frame()
                    .T,
                ]
            )
            .T.astype(int)
            .T
        ]
        scenarios += [edisgo_path.parent.name]
    df = pd.concat(df_costs, keys=scenarios).sort_index().T
    #     print(f"{edisgo_path.parent.name}: {costs/1e3:.2f} Mio €")

    order = [
        "minimize_loading",
        "reference",
        "20_pct_reinforced",
        "40_pct_reinforced",
        "60_pct_reinforced",
        "80_pct_reinforced",
        "100_pct_reinforced",
    ]

    return df.T.loc[order]


def get_power_diff(grid_path, timeframe, technology=["hp", "ev"]):

    keyword = "charging"
    if type(technology) is not list():
        technology = list(technology)

    df_all = pd.DataFrame()
    for i, scenario in enumerate(os.listdir(grid_path / "potential")):
        scenario_path = grid_path / "potential" / scenario
        df_diff = pd.DataFrame()
        for objective in os.listdir(scenario_path):
            if "power" not in objective:
                continue
            file_path = scenario_path / objective / "concat"
            if len(os.listdir(file_path)) == 0:
                continue
            files_in_path = get_files_in_subdirs(
                path=file_path, pattern="*.csv"
            )

            # filter for keyword
            keyword_files = [
                i
                for i in files_in_path
                if keyword in i and "slack_initial" not in i
            ]
            ts = pd.Series(index=timeframe, data=0, dtype=float)
            for tech in technology:

                # filter for technology
                file = [i for i in keyword_files if tech in i]
                df = pd.read_csv(file[0], index_col=0, parse_dates=True)

                if "ev" in tech:
                    ts = ts + df.loc[timeframe].sum(axis=1)
                elif "hp" in tech:
                    ts = ts + df.loc[timeframe].sum(axis=1)
                else:
                    raise KeyError

            df_diff = pd.concat([df_diff, ts], axis=1)
        df_diff = df_diff.diff(axis=1)
        df_diff = df_diff.iloc[:, -1].rename(scenario)
        df_all = pd.concat([df_all, df_diff], axis=1)

    df_all = df_all.loc[:, df_all.mean().sort_values().index]
    return df_all


def plot_power_potenital(grid_path, timeframe, technology=["hp", "ev"]):
    df_diff = get_power_diff(grid_path, timeframe, technology)

    # Create a list of colors with decreasing brightness
    num_traces = len(df_diff.columns) + 1
    colors = [f"rgb({i}, {i}, {255})" for i in np.linspace(0, 200, num_traces)]
    # colors.reverse()

    fig = go.Figure()

    # import opimized grid
    edisgo_obj = import_edisgo_from_files(
        grid_path / "minimize_loading" / "mvgd",
        import_topology=True,
        import_timeseries=False,
        import_heat_pump=True,
        import_electromobility=True,
    )

    for i, (scn, data) in enumerate(df_diff.items()):
        fig.add_trace(
            go.Scatter(
                mode="lines",
                #             opacity=0.3,
                #             fill='tozeroy',
                fill="tonexty",
                #             fill=None if i == 0 else "tonexty",
                name=scn,
                x=timeframe,
                y=data,
                line=dict(color=colors[i + 1]),
                #             showlegend=True if not subplot else False,
                #             showlegend=True if legend == 0 else False,
                #             legendgroup=scenario,
            )
        )

    upper_limit = pd.Series(index=timeframe, data=0, dtype=float)
    if "ev" in technology:
        upper_limit += (
            edisgo_obj.electromobility.flexibility_bands["upper_power"]
            .loc[timeframe]
            .sum(axis=1)
        )

    if "hp" in technology:
        hp_p_set = edisgo_obj.topology.loads_df.groupby("type")["p_set"].sum()[
            "heat_pump"
        ]
        hp_p_set = len(timeframe) * [hp_p_set]
        upper_limit += hp_p_set

    fig.add_trace(
        go.Scatter(
            mode="lines",
            name="upper limit",
            x=timeframe,
            y=upper_limit,
            line=dict(color="#16BDBF"),
        )
    )

    fig.update_layout(
        width=1000,
        height=500,
        # height=2000,
        margin=dict(t=30, b=30, l=30, r=30),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=1.1,
            xanchor="center",
            x=0.5,
            itemsizing="constant",
        ),
    )

    fig.update_yaxes(title_text="Power in MW")
    fig.update_yaxes(
        title_font=dict(size=16),
        titlefont=dict(size=18),
        tickfont=dict(size=16),
    )
    fig.update_xaxes(
        title_font=dict(size=16),
        titlefont=dict(size=18),
        tickfont=dict(size=16),
    )
    fig.show()
    return fig
