"""Tests for the analysis views in ``analysis/marts.sql``."""

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from tests.analysis.world import A, B, C, build_inputs

MARTS = Path(__file__).parents[2] / 'analysis' / 'marts.sql'
TUESDAY = "service_date = '2026-09-22'"


@pytest.fixture(name='con', scope='module')
def _con() -> duckdb.DuckDBPyConnection:
    """Run the views over the real seed and a small curated layer."""
    con = duckdb.connect()
    con.execute('INSTALL spatial; LOAD spatial; LOAD icu;')
    build_inputs(con=con)
    con.execute(MARTS.read_text(encoding='utf-8'))
    return con


def test_reference_views_take_the_latest_vintage(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select distinct vintage from stop_geography',
    ).fetchall()
    assert rows == [(date(2026, 9, 22),)]


def test_reference_views_keep_every_row_of_that_vintage(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute('select count(*) from stop_geography').fetchone()
    assert rows == (4,)


def test_an_unmoved_stop_is_not_stale(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        "select stop_id from stop_geography_stale where stop_id = 's1'",
    ).fetchall()
    assert rows == []


def test_a_moved_stop_is_flagged_stale(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select stop_id, round(moved_m) from stop_geography_stale '
        "where stop_id = 's2'",
    ).fetchone()
    assert row is not None
    assert row[0] == 's2'
    assert row[1] > 25


def test_calendar_exclusion_expands_ranges(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        "select count(*) from calendar_exclusion "
        "where reason = 'Spring holidays'",
    ).fetchone()
    assert rows is not None
    assert rows[0] == 12


def test_calendar_exclusion_keeps_both_reasons_for_a_date(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select exclusion_type from calendar_exclusion '
        "where day = '2026-10-05' order by exclusion_type",
    ).fetchall()
    assert rows == [('public_holiday',), ('school_holiday',)]


def test_term_weekday_excludes_the_spring_holidays(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select day from term_weekday '
        "where day between '2026-09-24' and '2026-10-14' order by day",
    ).fetchall()
    assert [row[0] for row in rows] == [
        date(2026, 9, 24),
        date(2026, 9, 25),
        date(2026, 10, 13),
        date(2026, 10, 14),
    ]


def test_term_weekday_counts_the_collection_window(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select count(*) from term_weekday '
        "where day between '2026-09-21' and '2026-11-06'",
    ).fetchone()
    assert rows is not None
    assert rows[0] == 24


def test_school_route_uses_the_declared_type(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute('select route_id from school_route').fetchall()
    assert rows == [('r_school',)]


def test_school_trip_follows_its_route(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute('select trip_id from school_trip').fetchall()
    assert rows == [('t1',)]


def test_seed_holds_every_nsw_public_holiday(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select day from calendar_exclusion '
        "where exclusion_type = 'public_holiday' order by day",
    ).fetchall()
    assert [row[0] for row in rows] == [
        date(2026, 1, 1),
        date(2026, 1, 26),
        date(2026, 4, 3),
        date(2026, 4, 4),
        date(2026, 4, 5),
        date(2026, 4, 6),
        date(2026, 4, 25),
        date(2026, 6, 8),
        date(2026, 10, 5),
        date(2026, 12, 25),
        date(2026, 12, 26),
        date(2026, 12, 28),
    ]


def test_exclusions_stay_within_the_year(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select min(day), max(day) from calendar_exclusion',
    ).fetchone()
    assert rows == (date(2026, 1, 1), date(2026, 12, 31))


def test_every_exclusion_type_is_recognised(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select distinct exclusion_type from calendar_exclusion '
        'order by exclusion_type',
    ).fetchall()
    assert [row[0] for row in rows] == [
        'public_holiday',
        'school_development_day',
        'school_holiday',
    ]


def rows_of(
    con: duckdb.DuckDBPyConnection, sql: str,
) -> list[tuple[object, ...]]:
    """Run a query and fetch every row.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection holding the views.
    sql : str
        Query.

    Returns
    -------
    list[tuple[object, ...]]
        The rows.
    """
    return con.execute(sql).fetchall()


def call_statuses(
    con: duckdb.DuckDBPyConnection, *, sequence: int,
) -> dict[object, object]:
    """Map each trip to its call status at one stop on 22 September.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection holding the views.
    sequence : int
        Stop sequence.

    Returns
    -------
    dict[object, object]
        Trip to status.
    """
    rows = rows_of(con, (
        'select trip_id, call_status from call_observation '
        f'where {TUESDAY} and stop_sequence = {sequence}'
    ))
    return {row[0]: row[1] for row in rows}


def test_snapshot_dates_the_check_in_sydney(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A check at 15:00 UTC on the 22nd is the 23rd in Sydney."""
    assert rows_of(
        con, 'select valid_from, check_date, has_trips from snapshot '
        'order by valid_from',
    ) == [
        (A, date(2026, 9, 20), True),
        (B, date(2026, 9, 22), True),
        (C, date(2026, 9, 23), False),
    ]


def test_service_day_picks_the_snapshot_the_merger_used(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Stop times follow the merger; trips fall back past stop-times-only."""
    assert rows_of(
        con, 'select service_date, stop_time_valid_from, trip_valid_from, '
        'timetable_borrowed, day_type from service_day order by 1',
    ) == [
        (date(2026, 9, 17), A, A, True, 'term_weekday'),
        (date(2026, 9, 20), A, A, False, 'sunday'),
        (date(2026, 9, 22), B, B, False, 'term_weekday'),
        (date(2026, 9, 23), C, B, False, 'term_weekday'),
    ]


def test_ordinary_route_drops_school_and_rail_replacement(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Type 700 only, less agencies 7083 and 7084; withdrawn routes stay."""
    assert rows_of(
        con, 'select route_id from ordinary_route order by 1',
    ) == [('r_bus',), ('r_named',), ('r_old',)]


def test_period_labels_hours_past_midnight_as_night(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Hour 25 is night on the previous day's timetable."""
    assert rows_of(
        con, 'select service_hour, period from period '
        'where service_hour in (5, 8, 14, 17, 21, 25) order by 1',
    ) == [
        (5, 'night'), (8, 'am_peak'), (14, 'inter_peak'),
        (17, 'pm_peak'), (21, 'evening'), (25, 'night'),
    ]


def test_scheduled_trip_reads_the_calendar_within_its_snapshot(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """``x`` means weekends in snapshot A, so it runs on Sunday 20th."""
    assert rows_of(
        con, 'select trip_id, is_timetabled from scheduled_trip '
        "where service_date = '2026-09-20' order by 1",
    ) == [('a2', True), ('a3', True), ('a4', True), ('a5', True)]


def test_scheduled_trip_keeps_reported_trips_the_calendar_misses(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A reported trip with stop times joins the spine; ADDED does not."""
    assert rows_of(
        con, 'select trip_id, is_timetabled from scheduled_trip '
        f'where {TUESDAY} order by 1',
    ) == [
        ('k1', True), ('k2', True), ('k3', True), ('k4', True),
        ('k5', True), ('kd', True), ('kn', True), ('ko', False),
    ]


def test_scheduled_trip_applies_calendar_exceptions(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """On the 23rd ``x`` is removed and ``wk`` added."""
    assert rows_of(
        con, 'select trip_id, is_timetabled from scheduled_trip '
        "where service_date = '2026-09-23'",
    ) == [('ko', True)]


def test_scheduled_call_counts_hours_past_midnight(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """25:10 on the 22nd is 01:10 on the 23rd, in service hour 25."""
    assert rows_of(
        con, "select timezone('UTC', scheduled_departure_utc), "
        'service_hour '
        f"from scheduled_call where {TUESDAY} and trip_id = 'kn' "
        'and stop_sequence = 1',
    ) == [(datetime(2026, 9, 22, 15, 10), 25)]


def test_scheduled_call_does_not_board_at_set_down_or_last_stops(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """pickup_type 1 and the last stop are judged on arrival."""
    assert rows_of(
        con, 'select stop_sequence, is_boarding from scheduled_call '
        f"where {TUESDAY} and trip_id = 'k3' order by 1",
    ) == [(1, True), (2, False), (3, False)]


def test_call_observation_gives_every_call_one_status(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Cancelled, absent, day-late and partial runs are told apart."""
    assert call_statuses(con, sequence=1) == {
        'k1': 'observed', 'k2': 'cancelled', 'k3': 'observed',
        'k4': 'absent', 'k5': 'observed', 'ko': 'observed',
        'kd': 'no_prediction', 'kn': 'observed',
    }


def test_call_observation_drops_predictions_after_a_cancellation(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A partly run trip's later calls are cancelled; SKIPPED is kept."""
    statuses = call_statuses(con, sequence=2)
    assert (statuses['k5'], statuses['ko']) == ('cancelled', 'skipped')


def test_call_observation_measures_delay_from_the_timetable(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Delays are observed minus scheduled, in seconds."""
    assert rows_of(
        con, 'select trip_id, stop_sequence, delay_s '
        f"from call_observation where {TUESDAY} and trip_id in ('k1', 'k3') "
        'order by 1, 2',
    ) == [
        ('k1', 1, -59), ('k1', 2, 360), ('k1', 3, -120),
        ('k3', 1, -61), ('k3', 2, 359), ('k3', 3, 30),
    ]


def test_call_observation_judges_a_first_stop_on_its_departure(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A fresh first-stop departure counts; lost tracking does not."""
    assert rows_of(
        con, 'select trip_id, stop_sequence, is_judged '
        f"from call_observation where {TUESDAY} and ("
        "(trip_id = 'k5' and stop_sequence = 1) "
        "or (trip_id = 'kn' and stop_sequence = 3)) order by 1",
    ) == [('k5', 1, True), ('kn', 3, False)]


def stop_hour(
    con: duckdb.DuckDBPyConnection, *, stop_id: str, columns: str,
) -> tuple[object, ...] | None:
    """Fetch one cell of ``mart_stop_hour`` at 08:00 on 22 September.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection holding the views.
    stop_id : str
        The stop.
    columns : str
        Columns to select.

    Returns
    -------
    tuple[object, ...] | None
        The cell.
    """
    return con.execute(
        f'select {columns} from mart_stop_hour where {TUESDAY} '
        f"and stop_id = '{stop_id}' and service_hour = 8",
    ).fetchone()


def test_mart_stop_hour_counts_every_call_status(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Seven scheduled calls at the first stop in the 08:00 hour."""
    assert stop_hour(con, stop_id='s1', columns=(
        'n_scheduled_calls, n_observed, n_cancelled, n_absent, '
        'n_no_prediction, n_skipped, n_judged'
    )) == (7, 4, 1, 1, 1, 0, 4)


def test_mart_stop_hour_applies_the_contract_window(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """59 s early is on time, 61 s early is early."""
    assert stop_hour(
        con, stop_id='s1', columns='n_on_time, n_early, n_late',
    ) == (2, 2, 0)


def test_mart_stop_hour_judges_set_down_calls_on_arrival(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """5:59 late on arrival is on time, 6:00 late departing is late."""
    assert stop_hour(
        con, stop_id='s2', columns='n_judged, n_on_time, n_late',
    ) == (2, 1, 1)


def test_mart_stop_hour_has_no_early_limit_at_the_last_stop(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Two minutes early at the terminus is on time."""
    assert stop_hour(
        con, stop_id='s3', columns='n_judged, n_on_time, n_early',
    ) == (2, 2, 0)


def test_mart_stop_hour_stores_headway_sums(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Scheduled and observed n, sum and sum of squares, in seconds."""
    assert stop_hour(con, stop_id='s1', columns=(
        'scheduled_headway_n, scheduled_headway_sum_s, '
        'scheduled_headway_sq_s, observed_headway_n, '
        'observed_headway_sum_s, observed_headway_sq_s'
    )) == (6, 3000, 1_620_000, 3, 2519, 3_028_925)


def test_mart_stop_hour_bands_and_bunches_by_the_cell_schedule(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A 60 s gap under an 8:20 mean headway is bunched."""
    assert stop_hour(con, stop_id='s1', columns=(
        'frequency_band, n_bunched, bunching_below_precision'
    )) == ('<=10', 1, False)


def test_mart_stop_hour_gives_set_down_calls_no_headway(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Four scheduled headways at s2: k3 only sets down there."""
    assert stop_hour(
        con, stop_id='s2', columns='scheduled_headway_n, n_boarding_calls',
    ) == (4, 5)


def test_mart_trip_classifies_every_trip(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Reinstated runs, partial runs and lost tracking each land once."""
    rows = rows_of(
        con, f'select trip_id, status from mart_trip where {TUESDAY}',
    )
    assert {row[0]: row[1] for row in rows} == {
        'k1': 'ran', 'k2': 'cancelled', 'k3': 'ran', 'k4': 'unknown',
        'k5': 'incomplete', 'kd': 'unknown', 'kn': 'incomplete',
        'ko': 'ran',
    }


def test_mart_trip_measures_the_unjudged_tail(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Scheduled seconds from the last judged call to the last stop."""
    assert rows_of(
        con, 'select trip_id, unjudged_tail_s from mart_trip '
        f"where {TUESDAY} and trip_id in ('k1', 'kn') order by 1",
    ) == [('k1', 0), ('kn', 1500)]


def test_mart_trip_keeps_the_cancellation_evidence(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A reinstated trip keeps ever_canceled; a missing one is unreported."""
    assert rows_of(
        con, 'select trip_id, ever_canceled, reported, first_stop_delay_s '
        f"from mart_trip where {TUESDAY} and trip_id in ('k1', 'k3', 'k4') "
        'order by 1',
    ) == [('k1', False, True, -59), ('k3', True, True, -61),
          ('k4', False, False, None)]


def test_stop_context_counts_walking_catchment(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Routed pairs only, with the unroutable one counted."""
    assert rows_of(
        con, 'select walk_population_400m, walk_population_800m, '
        'straight_population_800m, n_unrouted_meshblocks_800m, sa2_code '
        "from stop_context where stop_id = 's1'",
    ) == [(50, 70, 80, 1, 'A1')]


def test_stop_frequency_combines_routes(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Seven departures from s1 in the 08:00 hour."""
    assert rows_of(
        con, 'select n_departures, n_routes from stop_frequency '
        f"where {TUESDAY} and stop_id = 's1' and service_hour = 8",
    ) == [(7, 1)]


def test_route_context_weights_stops_by_calls(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """14 calls each at s1 and s2, 9 at the unscored s3."""
    assert rows_of(
        con, 'select n_stops, n_calls, n_calls_null_seifa, '
        'irsd_score_call_weighted_mean, irsd_score_call_weighted_median, '
        'irsd_state_decile_min, irsd_state_decile_max, '
        'irsd_quintile_1_call_share, irsd_quintile_3_call_share, '
        'walk_population_800m_call_weighted_mean '
        "from route_context where route_id = 'r_bus'",
    ) == [(3, 37, 9, 950.0, 900.0, 2, 5, 0.5, 0.5, 70.0)]


def test_route_clockface_allows_a_minute_of_drift(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """10:30, 11:00, 11:31, 12:00 is every 30 minutes."""
    assert rows_of(
        con, 'select period, n_departures, headway_min, is_clockface '
        "from route_clockface where route_id = 'r_bus' "
        "and service_date in ('2026-09-20', '2026-09-22') "
        "and period in ('inter_peak', 'am_peak') order by service_date",
    ) == [('inter_peak', 4, 30, True), ('am_peak', 7, 10, False)]


def test_route_league_counts_trips_on_term_weekdays(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Eight AM peak trips on the 22nd and 23rd; 17th's run is later."""
    assert rows_of(
        con, 'select n_trips, n_ran, n_cancelled, n_incomplete, '
        'n_unknown_trips, sd7_rate, sd1_on_time_share, on_time_share '
        "from route_league where route_id = 'r_bus' and period = 'am_peak'",
    ) == [(8, 3, 1, 1, 3, 0.25, 0.5, 0.625)]


def test_route_league_takes_excess_wait_as_a_ratio_of_sums(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Sums over both stops' cells, not an average of cell figures."""
    row = con.execute(
        "select excess_wait_s from route_league where route_id = 'r_bus' "
        "and period = 'am_peak'",
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(
        3_028_925 / (2 * 2519) - 3_870_000 / (2 * 5700),
    )


def test_coverage_reports_no_service_as_zero(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """An SA2 with a stop but no bus reads zero, not missing."""
    assert rows_of(
        con, 'select sa2_code, n_days, n_calls, calls_per_day '
        "from coverage where period = 'am_peak' "
        "and day_type = 'term_weekday' order by 1",
    ) == [('A1', 3, 14, 14 / 3), ('B2', 3, 0, 0.0), ('C3', 3, 0, 0.0)]
