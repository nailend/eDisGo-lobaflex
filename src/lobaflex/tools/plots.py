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
    }

    data = dict().fromkeys(objectives)

    list_of_results = get_files_in_subdirs(path=results_path, pattern="*.csv")
    selected_list = [i for i in list_of_results if "concat" in i]

    keyword_files = [
        i for i in selected_list if keyword in i and not "slack_initial" in i
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
    }

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
                    line_color=colors_dict[attr],
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
                    line_color=colors_dict["hp_el"],
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
                    line_color=colors_dict["ev"],
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
            name="Heat pumps",
            x=timeframe,
            y=df_hp,
            stackgroup="one",
            yaxis="y1",
        )
    )

    fig.add_trace(
        go.Scatter(
            mode="lines",
            name="Electric Vehicles",
            x=timeframe,
            y=df_ev,
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


def plot_scenario_potential(
    potential_path, objectives, keyword, technology, timeframe,
    title=None
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
    # Define the base color and the number of traces
    base_color = "rgb(0, 0, 255)"
    num_traces = 5

    # Create a list of colors with decreasing brightness
    # colors = [f'rgb({int(255 - i*50)}, {int(255 - i*50)}, {255})' for i in range(num_traces)]
    colors = [f"rgb({i}, {i}, {255})" for i in np.linspace(0, 255, num_traces)]

    scenarios = [
        "60_pct_reinforced",
        "40_pct_reinforced",
        "20_pct_reinforced",
        "minimize_loading",
    ]
    scenarios.reverse()

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
    # flexible_loads = edisgo_obj.topology.loads_df.loc[
    # edisgo_obj.topology.loads_df["opt"] == True
    # ].index
    #
    # flexible_hp = [i for i in flexible_loads if "HP" in i]
    # flexible_cp = [i for i in flexible_loads if "Charging_Point" in i]
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
            files_in_path = get_files_in_subdirs(
                path=scenario_path, pattern="*.csv"
            )

            keyword_files = [
                i
                for i in files_in_path
                if keyword in i and "slack_initial" not in i
            ]

            file = [i for i in keyword_files if technology in i]
            file = file[0]
            attr = re.findall(rf"{keyword}_(.*).csv", file)[0]
            df = pd.read_csv(file, index_col=0, parse_dates=True)
            df = df.sum(axis=1).rename(f"{keyword} {attr} [MW]")
            df = df.loc[timeframe]

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

    if technology == "ev":
        upper_power = (
            edisgo_obj.electromobility.flexibility_bands["upper_power"]
            .loc[timeframe]
            .sum(axis=1)
        )
    elif technology == "hp_el":
        upper_power = (
            edisgo_obj.topology.loads_df.loc[
            edisgo_obj.topology.loads_df["type"] == "heat_pump"
        ]
        .groupby("type")["p_set"]
        .sum()
        )
        upper_power = len(timeframe) * [upper_power["heat_pump"]]
        # print(upper_power)
    fig.add_trace(
        go.Scatter(
            mode="lines",
            #             opacity=0.3,
            #             fill='tozeroy',
            #             fill="tonexty",
            # fill=None if i == 0 else "tonexty",
            name="upper_power",
            x=timeframe,
            y=upper_power,
            # line=dict(color=colors[i + 1]),
            # showlegend=True if not subplot else False,
            # showlegend=True if legend == 0 else False,
            # legendgroup=scenario,
        ))


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

    # import reference grid
    edisgo_obj = import_edisgo_from_files(
        grid_path / "reference" / "mvgd",
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

    residual_load = edisgo_obj.timeseries.residual_load.loc[timeframe].rename(
        "residual_load"
    )
    df = pd.concat(
        [
            residual_load,
            hp_optimized,
            hp_reference,
            ev_optimized,
            ev_reference,
        ],
        axis=1,
    )

    colordict = {
        "hp_opt": "#8B4513",
        "hp_reference": "#D2691E",
        "ev_opt": "#0071C5",
        "ev_reference": "#1E90FF",
        "residual_load": "grey",
    }

    filldict = {
        "hp_opt": "tozeroy",
        "hp_reference": "tonexty",
        "ev_opt": "tozeroy",
        "ev_reference": "tonexty",
        "residual_load": "tozeroy",
    }

    patterndict = {
        "hp_opt": None,
        "hp_reference": "/",
        "ev_opt": None,
        "ev_reference": "/",
        "residual_load": None,
    }

    fig = go.Figure()

    for name, ts in df.items():

        fig.add_trace(
            go.Scatter(
                mode="lines",
                line=dict(color=colordict[name]),
                # fill= "tozeroy" if name == "residual_load" else None,
                fill=filldict[name],
                #             opacity=0.3,
                fillpattern=dict(shape=patterndict[name]),
                # stackgroup=patterndict[name],
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
        yaxis_title="MW",
        xaxis_title="timesteps",
        showlegend=True,
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

    fig.show()


def plot_flex_capacities(edisgo_obj):
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
    )

    fig.add_trace(
        go.Violin(y=df_cap_hp, name=""),
        col=1,
        row=1,
    )
    fig.add_trace(go.Violin(y=df_cap_tes, name=""), col=2, row=1)
    fig.add_trace(go.Violin(y=df_cap_ev, name=""), col=3, row=1)

    fig.update_layout(
        width=1000,
        height=500,
        showlegend=False,
        yaxis=dict(title="kW"),
        yaxis2=dict(title="kWh"),
        yaxis3=dict(title="kWh"),
    )
    fig.show()