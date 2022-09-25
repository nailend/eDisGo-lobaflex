import datetime

import pandas as pd
import saio

from sqlalchemy import func
from sqlalchemy.types import Integer

import egon_db as db

from tools import timeit

saio.register_schema("demand", engine=db.engine())
saio.register_schema("boundaries", engine=db.engine())
saio.register_schema("supply", engine=db.engine())
saio.register_schema("openstreetmap", engine=db.engine())


def get_profile_ids_residential_heat_demand(building_id=None, mv_grid_id=None):
    """
    Returns a list of profile ids for residential heat demand.
    """

    from saio.boundaries import egon_map_zensus_grid_districts
    from saio.demand import heat_timeseries_selected_profiles

    # if building_id:
    #     building = db.select_dataframe(
    #         f"""
    #         SELECT * FROM
    #         demand.heat_timeseries_selected_profiles
    #         WHERE building_id  = {building_id}
    #         """,
    #         index_col='ID')

    if mv_grid_id:
        with db.session_scope() as session:
            cells_query = (
                session.query(heat_timeseries_selected_profiles)
                .filter(
                    egon_map_zensus_grid_districts.zensus_population_id
                    == heat_timeseries_selected_profiles.zensus_population_id
                )
                .filter(egon_map_zensus_grid_districts.bus_id == mv_grid_id)
            )

        df_profiles = pd.read_sql(
            cells_query.statement,
            cells_query.session.bind,
            index_col="ID",
        )

    # TODO rework
    df_profiles.selected_idp_profiles = df_profiles.selected_idp_profiles.str.replace(
        "[", ""
    )

    df_profiles.selected_idp_profiles = df_profiles.selected_idp_profiles.str.replace(
        "]", ""
    )

    df_profiles.selected_idp_profiles = df_profiles.selected_idp_profiles.str.split(
        ", "
    )

    return df_profiles


def create_timeseries_residential_heat_demand(df_profiles, idp_data):
    """"""

    # TODO rework all
    selected_idp = pd.DataFrame(
        index=df_profiles.index,
        columns=range(365),
        data=df_profiles.selected_idp_profiles,
    )

    for i in range(365):
        selected_idp[i] = df_profiles.selected_idp_profiles.str[i]

    df_timeseries = pd.DataFrame(columns=selected_idp.index, index=range(8760))

    for i in range(365):
        df_timeseries[i * 24 : (i + 1) * 24] = idp_data.loc[
            selected_idp[i].astype(int)
        ].transpose()

    df_timeseries.index = pd.date_range(datetime(2011, 1, 1, 0), periods=8760, freq="H")

    return df_timeseries


def get_random_residential_buildings(scenario, limit):
    """"""
    # residential
    from saio.demand import egon_building_electricity_peak_loads

    with db.session_scope() as session:

        cells_query = (
            session.query(egon_building_electricity_peak_loads.building_id)
            .filter(egon_building_electricity_peak_loads.scenario == scenario)
            .filter(egon_building_electricity_peak_loads.sector == "residential")
            .limit(limit)
        )

    df_building_id = pd.read_sql(
        cells_query.statement,
        cells_query.session.bind,
        index_col=None,
    )
    return df_building_id


@timeit
def get_cop(building_ids):

    from saio.boundaries import (
        egon_map_zensus_buildings_residential,
        egon_map_zensus_weather_cell,
    )
    from saio.openstreetmap import osm_buildings_synthetic
    from saio.supply import egon_era5_renewable_feedin

    with db.session_scope() as session:
        cells_query = (
            session.query(
                egon_map_zensus_buildings_residential.id.label("egon_building_id"),
                egon_era5_renewable_feedin.feedin,
            )
            .filter(egon_map_zensus_buildings_residential.id.in_(building_ids))
            .filter(
                egon_map_zensus_buildings_residential.cell_id
                == egon_map_zensus_weather_cell.zensus_population_id,
            )
            .filter(
                egon_map_zensus_weather_cell.w_id == egon_era5_renewable_feedin.w_id,
            )
            .filter(egon_era5_renewable_feedin.carrier == "heat_pump_cop")
        )

    df_cop_osm = pd.read_sql(
        cells_query.statement,
        cells_query.session.bind,
        index_col=None,
    )
    synt_building_id = set(building_ids).difference(set(df_cop_osm["egon_building_id"]))

    with db.session_scope() as session:
        cells_query = (
            session.query(
                osm_buildings_synthetic.id.label("egon_building_id"),
                egon_era5_renewable_feedin.feedin,
            )
            .filter(
                func.cast(osm_buildings_synthetic.id, Integer).in_(synt_building_id)
            )
            .filter(
                func.cast(osm_buildings_synthetic.cell_id, Integer)
                == egon_map_zensus_weather_cell.zensus_population_id,
            )
            .filter(
                egon_map_zensus_weather_cell.w_id == egon_era5_renewable_feedin.w_id,
            )
            .filter(egon_era5_renewable_feedin.carrier == "heat_pump_cop")
        )

    df_cop_synth = pd.read_sql(
        cells_query.statement,
        cells_query.session.bind,
        index_col=None,
    )
    df_cop_synth["egon_building_id"] = df_cop_synth["egon_building_id"].astype(int)
    df_cop = pd.concat([df_cop_osm, df_cop_synth], axis=0, ignore_index=True)

    df_cop = pd.DataFrame.from_dict(
        df_cop.set_index("egon_building_id")["feedin"].to_dict(), orient="columns"
    )

    return df_cop


def create_timeseries_for_building(building_id, scenario):
    """Generates final heat demand timeseries for a specific building

    Parameters
    ----------
    building_id : int
        Index of the selected building
    scenario : str
        Name of the selected scenario.

    Returns
    -------
    pandas.DataFrame
        Hourly heat demand timeseries in MW for the selected building

    """
    sql = f"""
        SELECT building_demand * UNNEST(idp) as demand
        FROM
        (
        SELECT demand.demand / building.count * daily_demand.daily_demand_share as building_demand, daily_demand.day_of_year
        FROM

        (SELECT demand FROM
        demand.egon_peta_heat
        WHERE scenario = '{scenario}'
        AND sector = 'residential'
        AND zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
        WHERE building_id  = {building_id})) as demand,

        (SELECT COUNT(building_id)
        FROM demand.egon_heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
        WHERE building_id  = {building_id})) as building,

        (SELECT daily_demand_share, day_of_year FROM
        demand.egon_daily_heat_demand_per_climate_zone
        WHERE climate_zone = (
            SELECT climate_zone FROM boundaries.egon_map_zensus_climate_zones
            WHERE zensus_population_id =
            (SELECT zensus_population_id FROM demand.egon_heat_timeseries_selected_profiles
             WHERE building_id = {building_id}))) as daily_demand) as daily_demand

        JOIN (SELECT b.idp, ordinality as day
        FROM demand.egon_heat_timeseries_selected_profiles a,
        UNNEST (a.selected_idp_profiles) WITH ORDINALITY as selected_idp
        JOIN demand.egon_heat_idp_pool b
        ON selected_idp = b.index
        WHERE a.building_id = {building_id}) as demand_profile
        ON demand_profile.day = daily_demand.day_of_year
        """

    return pd.read_sql(sql, db.engine(), index_col=None).rename(
        columns={"demand": building_id}
    )
