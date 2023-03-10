import re

import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio

from plotly.subplots import make_subplots

from lobaflex.tools.tools import get_files_in_subdirs

# comment to make plots interactive
# pio.renderers.default = "svg"


def get_all_attribute_values_for_keyword(results_path, keyword):
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
    title=None,
):
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
        i for i in selected_list if keyword in i and not "slack_initial" in i
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
            traces = []
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
                    # legendgroup=attr,
                    legendgroup=obj,
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
                    # legendgroup=attr,
                    legendgroup=obj,
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
            height=2000,
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
    # define components
    res_generators = edisgo_obj.topology.generators_df[
        edisgo_obj.topology.generators_df["type"].isin(["solar", "wind"])
    ].index

    flexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["opt"] == True
    ].index

    inflexible_loads = edisgo_obj.topology.loads_df.loc[
        edisgo_obj.topology.loads_df["opt"] == False
    ].index

    flexible_hp = [i for i in flexible_loads if "HP" in i]
    flexible_cp = [i for i in flexible_loads if "Charging_Point" in i]

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            mode="lines",
            name="Heat pumps",
            x=timeframe,
            y=edisgo_obj.timeseries.loads_active_power.loc[
                timeframe, flexible_hp
            ].sum(axis=1),
            #     stackgroup='one'
            yaxis="y1",
        )
    )

    fig.add_trace(
        go.Scatter(
            mode="lines",
            name="Charging points",
            x=timeframe,
            y=edisgo_obj.timeseries.loads_active_power.loc[
                timeframe, flexible_cp
            ].sum(axis=1),
            #     stackgroup='one',
            yaxis="y1",
        )
    )

    fig.add_trace(
        go.Scatter(
            mode="lines",
            name="Feedin-fix_load",
            x=timeframe,
            y=edisgo_obj.timeseries.generators_active_power.loc[
                timeframe, res_generators
            ].sum(axis=1)
            - edisgo_obj.timeseries.loads_active_power.loc[
                timeframe, inflexible_loads
            ].sum(axis=1),
            #     stackgroup='two',
            yaxis="y2",
        )
    )

    fig.update_layout(
        # title=f"Charging: {run_id} - {grid_id}",
        title=f"Optimized dispatch: " + "" if title is None else title,
        width=1000,
        height=600,
        yaxis_title="MW",
        xaxis_title="timesteps",
        showlegend=True,
        yaxis=dict(title="Power in MW"),
        yaxis2=dict(title="Power in MW", overlaying="y", side="right"),
    )

    fig.show()
