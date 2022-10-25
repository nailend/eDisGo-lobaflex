import numpy as np
import pandas as pd
import saio

from loguru import logger
from sqlalchemy import func
from sqlalchemy.types import Integer

import egon_db as db

from tools import timeit

saio.register_schema("demand", engine=db.engine())
saio.register_schema("boundaries", engine=db.engine())
saio.register_schema("supply", engine=db.engine())
saio.register_schema("openstreetmap", engine=db.engine())

from saio.boundaries import (
    egon_map_zensus_buildings_residential,
    egon_map_zensus_climate_zones,
    egon_map_zensus_grid_districts,
    egon_map_zensus_mvgd_buildings,
    egon_map_zensus_weather_cell,
)
from saio.demand import (
    egon_building_electricity_peak_loads,
    egon_cts_electricity_demand_building_share,
    egon_cts_heat_demand_building_share,
    egon_daily_heat_demand_per_climate_zone,
    egon_etrago_electricity_cts,
    egon_etrago_heat_cts,
    egon_heat_idp_pool,
    egon_heat_timeseries_selected_profiles,
    egon_peta_heat,
)
from saio.openstreetmap import osm_buildings_synthetic
from saio.supply import egon_era5_renewable_feedin




@timeit
def get_random_residential_buildings(scenario, limit):
    """"""
    # residential

    with db.session_scope() as session:

        # TODO change to heat peak load as there are residentials without heat
        #  but electricity
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


@timeit
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


def determine_minimum_hp_capacity_per_building(
    peak_heat_demand, flexibility_factor=24 / 18, cop=1.7
):
    """
    Determines minimum required heat pump capacity.

    Parameters
    ----------
    peak_heat_demand : pd.Series
        Series with peak heat demand per building in MW. Index contains the
        building ID.
    flexibility_factor : float
        Factor to overdimension the heat pump to allow for some flexible
        dispatch in times of high heat demand. Per default, a factor of 24/18
        is used, to take into account

    Returns
    -------
    pd.Series
        Pandas series with minimum required heat pump capacity per building in
        MW.

    """
    return peak_heat_demand * flexibility_factor / cop


def get_peta_demand(mvgd, scenario):
    """
    Retrieve annual peta heat demand for residential buildings for either
    eGon2035 or eGon100RE scenario.

    Parameters
    ----------
    mvgd : int
        MV grid ID.
    scenario : str
        Possible options are eGon2035 or eGon100RE

    Returns
    -------
    df_peta_demand : pd.DataFrame
        Annual residential heat demand per building and scenario. Columns of
        the dataframe are zensus_population_id and demand.

    """

    with db.session_scope() as session:
        query = (
            session.query(
                egon_map_zensus_grid_districts.zensus_population_id,
                egon_peta_heat.demand,
            )
            .filter(egon_map_zensus_grid_districts.bus_id == mvgd)
            .filter(
                egon_map_zensus_grid_districts.zensus_population_id
                == egon_peta_heat.zensus_population_id
            )
            .filter(
                egon_peta_heat.sector == "residential",
                egon_peta_heat.scenario == scenario,
            )
        )

        df_peta_demand = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )

    return df_peta_demand


def get_residential_heat_profile_ids(mvgd):
    """
    Retrieve 365 daily heat profiles ids per residential building and selected
    mvgd.

    Parameters
    ----------
    mvgd : int
        ID of MVGD

    Returns
    -------
    df_profiles_ids : pd.DataFrame
        Residential daily heat profile ID's per building. Columns of the
        dataframe are zensus_population_id, building_id,
        selected_idp_profiles, buildings and day_of_year.

    """
    with db.session_scope() as session:
        query = (
            session.query(
                egon_map_zensus_grid_districts.zensus_population_id,
                egon_heat_timeseries_selected_profiles.building_id,
                egon_heat_timeseries_selected_profiles.selected_idp_profiles,
            )
            .filter(egon_map_zensus_grid_districts.bus_id == mvgd)
            .filter(
                egon_map_zensus_grid_districts.zensus_population_id
                == egon_heat_timeseries_selected_profiles.zensus_population_id
            )
        )

        df_profiles_ids = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )
    # Add building count per cell
    df_profiles_ids = pd.merge(
        left=df_profiles_ids,
        right=df_profiles_ids.groupby("zensus_population_id")["building_id"]
        .count()
        .rename("buildings"),
        left_on="zensus_population_id",
        right_index=True,
    )

    # unnest array of ids per building
    df_profiles_ids = df_profiles_ids.explode("selected_idp_profiles")
    # add day of year column by order of list
    df_profiles_ids["day_of_year"] = (
        df_profiles_ids.groupby("building_id").cumcount() + 1
    )
    return df_profiles_ids


def get_daily_profiles(profile_ids):
    """
    Parameters
    ----------
    profile_ids : list(int)
        daily heat profile ID's

    Returns
    -------
    df_profiles : pd.DataFrame
        Residential daily heat profiles. Columns of the dataframe are idp,
        house, temperature_class and hour.

    """

    with db.session_scope() as session:
        query = session.query(egon_heat_idp_pool).filter(
            egon_heat_idp_pool.index.in_(profile_ids)
        )

        df_profiles = pd.read_sql(
            query.statement, query.session.bind, index_col="index"
        )

    # unnest array of profile values per id
    df_profiles = df_profiles.explode("idp")
    # Add column for hour of day
    df_profiles["hour"] = df_profiles.groupby(axis=0, level=0).cumcount() + 1

    return df_profiles


def get_daily_demand_share(mvgd):
    """per census cell
    Parameters
    ----------
    mvgd : int
        MVGD id

    Returns
    -------
    df_daily_demand_share : pd.DataFrame
        Daily annual demand share per cencus cell. Columns of the dataframe
        are zensus_population_id, day_of_year and daily_demand_share.

    """

    with db.session_scope() as session:
        query = session.query(
            egon_map_zensus_grid_districts.zensus_population_id,
            egon_daily_heat_demand_per_climate_zone.day_of_year,
            egon_daily_heat_demand_per_climate_zone.daily_demand_share,
        ).filter(
            egon_map_zensus_climate_zones.climate_zone
            == egon_daily_heat_demand_per_climate_zone.climate_zone,
            egon_map_zensus_grid_districts.zensus_population_id
            == egon_map_zensus_climate_zones.zensus_population_id,
            egon_map_zensus_grid_districts.bus_id == mvgd,
        )

        df_daily_demand_share = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )
    return df_daily_demand_share


def calc_cts_building_profiles(
    bus_ids,
    scenario,
    sector,
):
    """
    Calculate the cts demand profile for each building. The profile is
    calculated by the demand share of the building per substation bus.

    Parameters
    ----------
    bus_ids: list of int
        Ids of the substation for which selected building profiles are
        calculated.
    scenario: str
        Scenario for which the share is calculated: "eGon2035" or "eGon100RE"
    sector: str
        Sector for which the share is calculated: "electricity" or "heat"

    Returns
    -------
    df_building_profiles: pd.DataFrame
        Table of demand profile per building. Column names are building IDs and index
        is hour of the year as int (0-8759).

    """
    if sector == "electricity":
        # Get cts building electricity demand share of selected buildings
        with db.session_scope() as session:
            cells_query = (
                session.query(
                    egon_cts_electricity_demand_building_share,
                )
                .filter(egon_cts_electricity_demand_building_share.scenario == scenario)
                .filter(egon_cts_electricity_demand_building_share.bus_id.in_(bus_ids))
            )

        df_demand_share = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col=None
        )

        # Get substation cts electricity load profiles of selected bus_ids
        with db.session_scope() as session:
            cells_query = (
                session.query(egon_etrago_electricity_cts).filter(
                    egon_etrago_electricity_cts.scn_name == scenario
                )
            ).filter(egon_etrago_electricity_cts.bus_id.in_(bus_ids))

        df_cts_substation_profiles = pd.read_sql(
            cells_query.statement,
            cells_query.session.bind,
        )
        df_cts_substation_profiles = pd.DataFrame.from_dict(
            df_cts_substation_profiles.set_index("bus_id")["p_set"].to_dict(),
            orient="index",
        )
        # df_cts_profiles = calc_load_curves_cts(scenario)

    elif sector == "heat":
        # Get cts building heat demand share of selected buildings
        with db.session_scope() as session:
            cells_query = (
                session.query(
                    egon_cts_heat_demand_building_share,
                )
                .filter(egon_cts_heat_demand_building_share.scenario == scenario)
                .filter(egon_cts_heat_demand_building_share.bus_id.in_(bus_ids))
            )

        df_demand_share = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col=None
        )

        # Get substation cts heat load profiles of selected bus_ids
        with db.session_scope() as session:
            cells_query = (
                session.query(egon_etrago_heat_cts).filter(
                    egon_etrago_heat_cts.scn_name == scenario
                )
            ).filter(egon_etrago_heat_cts.bus_id.in_(bus_ids))

        df_cts_substation_profiles = pd.read_sql(
            cells_query.statement,
            cells_query.session.bind,
        )
        df_cts_substation_profiles = pd.DataFrame.from_dict(
            df_cts_substation_profiles.set_index("bus_id")["p_set"].to_dict(),
            orient="index",
        )

    else:
        raise KeyError("Sector needs to be either 'electricity' or 'heat'")

    # TODO remove after #722
    df_demand_share.rename(columns={"id": "building_id"}, inplace=True)

    # get demand profile for all buildings for selected demand share
    df_building_profiles = pd.DataFrame()
    for bus_id, df in df_demand_share.groupby("bus_id"):
        shares = df.set_index("building_id", drop=True)["profile_share"]
        try:
            profile_ts = df_cts_substation_profiles.loc[bus_id]
        except KeyError:
            # This should only happen within the SH cutout
            logger.info(
                f"No CTS profile found for substation with bus_id:" f" {bus_id}"
            )
            continue

        building_profiles = np.outer(profile_ts, shares)
        building_profiles = pd.DataFrame(
            building_profiles, index=profile_ts.index, columns=shares.index
        )
        df_building_profiles = pd.concat(
            [df_building_profiles, building_profiles], axis=1
        )

    return df_building_profiles


def identify_similar_mvgd(number_of_residentials):

    logger.info(
        f"Looking for mvgd with more then {number_of_residentials} " f"residentials."
    )
    with db.session_scope() as session:
        cells_query = (
            session.query(
                egon_map_zensus_mvgd_buildings.bus_id,
                func.count(egon_map_zensus_mvgd_buildings.building_id).label("count"),
            )
            .filter(egon_map_zensus_mvgd_buildings.sector == "residential")
            .group_by(egon_map_zensus_mvgd_buildings.bus_id)
        )

    df = pd.read_sql(cells_query.statement, session.connection(), index_col=None)

    df = df.loc[df["count"] > number_of_residentials]
    mvgd = df.nsmallest(1, columns="count")
    logger.info(
        f"{mvgd['bus_id'].values} with {mvgd['count'].values} "
        f"residentials "
        f"found."
    )

    return mvgd["bus_id"].values[0]


@timeit
def calc_residential_heat_profiles_per_mvgd(mvgd, scenario):
    """
    Gets residential heat profiles per building in MV grid for either eGon2035
    or eGon100RE scenario.

    Parameters
    ----------
    mvgd : int
        MV grid ID.
    scenario : str
        Possible options are eGon2035 or eGon100RE.

    Returns
    --------
    pd.DataFrame
        Heat demand profiles of buildings. Columns are:
            * zensus_population_id : int
                Zensus cell ID building is in.
            * building_id : int
                ID of building.
            * day_of_year : int
                Day of the year (1 - 365).
            * hour : int
                Hour of the day (1 - 24).
            * demand_ts : float
                Building's residential heat demand in MW, for specified hour
                of the year (specified through columns `day_of_year` and
                `hour`).
    """

    columns = [
        "zensus_population_id",
        "building_id",
        "day_of_year",
        "hour",
        "demand_ts",
    ]

    df_peta_demand = get_peta_demand(mvgd, scenario)

    # TODO maybe return empty dataframe
    if df_peta_demand.empty:
        logger.info(f"No demand for MVGD: {mvgd}")
        return pd.DataFrame(columns=columns)

    df_profiles_ids = get_residential_heat_profile_ids(mvgd)

    if df_profiles_ids.empty:
        logger.info(f"No profiles for MVGD: {mvgd}")
        return pd.DataFrame(columns=columns)

    df_profiles = get_daily_profiles(df_profiles_ids["selected_idp_profiles"].unique())

    df_daily_demand_share = get_daily_demand_share(mvgd)

    # Merge profile ids to peta demand by zensus_population_id
    df_profile_merge = pd.merge(
        left=df_peta_demand, right=df_profiles_ids, on="zensus_population_id"
    )

    # Merge daily demand to daily profile ids by zensus_population_id and day
    df_profile_merge = pd.merge(
        left=df_profile_merge,
        right=df_daily_demand_share,
        on=["zensus_population_id", "day_of_year"],
    )

    # Merge daily profiles by profile id
    df_profile_merge = pd.merge(
        left=df_profile_merge,
        right=df_profiles[["idp", "hour"]],
        left_on="selected_idp_profiles",
        right_index=True,
    )

    # Scale profiles
    df_profile_merge["demand_ts"] = (
        df_profile_merge["idp"]
        .mul(df_profile_merge["daily_demand_share"])
        .mul(df_profile_merge["demand"])
        .div(df_profile_merge["buildings"])
    )

    return df_profile_merge.loc[:, columns]


def aggregate_residential_and_cts_profiles(mvgd, scenario):
    """
    Gets residential and CTS heat demand profiles per building and aggregates
    them.

    Parameters
    ----------
    mvgd : int
        MV grid ID.
    scenario : str
        Possible options are eGon2035 or eGon100RE.

    Returns
    --------
    pd.DataFrame
        Table of demand profile per building. Column names are building IDs and
        index is hour of the year as int (0-8759).

    """
    # ############### get residential heat demand profiles ###############
    df_heat_ts = calc_residential_heat_profiles_per_mvgd(mvgd=mvgd, scenario=scenario)

    # pivot to allow aggregation with CTS profiles
    df_heat_ts = df_heat_ts.pivot(
        index=["day_of_year", "hour"],
        columns="building_id",
        values="demand_ts",
    )
    df_heat_ts = df_heat_ts.sort_index().reset_index(drop=True)

    # ############### get CTS heat demand profiles ###############
    heat_demand_cts_ts = calc_cts_building_profiles(
        bus_ids=[mvgd],
        scenario=scenario,
        sector="heat",
    )

    # ############# aggregate residential and CTS demand profiles #############
    df_heat_ts = pd.concat([df_heat_ts, heat_demand_cts_ts], axis=1)

    df_heat_ts = df_heat_ts.groupby(axis=1, level=0).sum()

    return df_heat_ts
