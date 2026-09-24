"""A small curated layer for testing the analysis views.

Three timetable snapshots: ``A`` checked on 20 September in Sydney, ``B``
on 22 September, and ``C`` early on 23 September in Sydney but still 22
September in UTC, holding stop times alone. ``service_id`` means
weekdays in one snapshot and weekends in the other. Facts cover 16, 17,
20, 22 and 23 September 2026, with the trips on 22 September built to
give one of every call and trip status.
"""

from datetime import date
from pathlib import Path
from typing import Final

import duckdb

SEED: Final[Path] = (
    Path(__file__).parents[2] / 'analysis' / 'calendar_exclusions_2026.csv'
)
A: Final[str] = '2026-09-19T173455Z'
B: Final[str] = '2026-09-22T020011Z'
C: Final[str] = '2026-09-22T150000Z'
WEEKDAYS: Final[tuple[str, ...]] = ('1', '1', '1', '1', '1', '0', '0')
WEEKENDS: Final[tuple[str, ...]] = ('0', '0', '0', '0', '0', '1', '1')

Row = tuple[object, ...]


def table(
    *,
    con: duckdb.DuckDBPyConnection,
    name: str,
    columns: str,
    rows: list[Row],
) -> None:
    """Create a table and fill it.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to create it in.
    name : str
        Table name.
    columns : str
        Column definitions, comma separated, with no commas inside types.
    rows : list[Row]
        Values, in column order.
    """
    marks = ', '.join('?' for _ in columns.split(','))
    con.execute(f'create table {name} ({columns})')
    con.executemany(f'insert into {name} values ({marks})', rows)


def at(clock: str, *, day: str = '2026-09-22') -> str:
    """Name a Sydney wall-clock instant in September 2026.

    Parameters
    ----------
    clock : str
        ``HH:MM:SS``, Sydney standard time.
    day : str
        ``YYYY-MM-DD``.

    Returns
    -------
    str
        A timestamp literal with its offset.
    """
    return f'{day} {clock}+10'


def stop_times(
    *,
    trip_id: str,
    times: tuple[str, ...],
    valid_from: str,
) -> list[Row]:
    """Schedule one trip over stops ``s1``, ``s2``, ``s3`` in order.

    The first and last calls are timepoints. Trip ``k3`` only sets down
    at its second stop.

    Parameters
    ----------
    trip_id : str
        The trip.
    times : tuple[str, ...]
        One ``HH:MM:SS`` per stop, used for arrival and departure.
    valid_from : str
        Snapshot label.

    Returns
    -------
    list[Row]
        ``dim_scheduled_stop_time`` rows.
    """
    last = len(times)
    return [
        (
            trip_id, f's{sequence}', sequence, time, time,
            '1' if sequence in (1, last) else '0',
            '1' if (trip_id, sequence) == ('k3', 2) else '0',
            valid_from,
        )
        for sequence, time in enumerate(times, start=1)
    ]


def build_timetable(*, con: duckdb.DuckDBPyConnection) -> None:
    """Create every ``dim_*_all`` input.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to create them in.
    """
    table(con=con, name='dim_route_all', columns=(
        'route_id varchar, agency_id varchar, route_short_name varchar, '
        'route_long_name varchar, route_type varchar, valid_from varchar'
    ), rows=[
        ('r_bus', '2459', '400', 'Bondi to Burwood', '700', A),
        ('r_old', '2459', '401', 'Withdrawn route', '700', A),
        ('r_school', '2459', 'S265', 'Kirrawee PS to Kirrawee HS', '712',
         B),
        ('r_bus', '2459', '400', 'Bondi to Burwood', '700', B),
        ('r_named', '2459', '753', 'Warabrook to Corpus Christi School',
         '700', B),
        ('r_sw', '7083', 'SW1', 'Bankstown to Sydenham', '700', B),
        ('r_rr', '7051', 'BMT1', 'Trackwork buses', '714', B),
    ])
    trips: list[Row] = [
        ('a1', 'r_bus', 'wk', '0', A),
        *((f'a{n}', 'r_bus', 'x', '0', A) for n in range(2, 6)),
        ('t1', 'r_school', 'none', '0', B),
        ('t2', 'r_bus', 'none', '0', B),
        ('t3', 'r_named', 'none', '0', B),
        *((trip, 'r_bus', 'x', '0', B)
          for trip in ('k1', 'k2', 'k3', 'k4', 'k5', 'kd', 'kn')),
        ('ko', 'r_bus', 'wk', '0', B),
    ]
    table(con=con, name='dim_trip_all', columns=(
        'trip_id varchar, route_id varchar, service_id varchar, '
        'direction_id varchar, valid_from varchar'
    ), rows=trips)
    days = ', '.join(
        f'{day} varchar' for day in (
            'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
            'saturday', 'sunday',
        )
    )
    table(con=con, name='dim_calendar_all', columns=(
        f'service_id varchar, {days}, start_date varchar, '
        'end_date varchar, valid_from varchar'
    ), rows=[
        ('wk', *WEEKDAYS, '20260918', '20261231', A),
        ('x', *WEEKENDS, '20260918', '20261231', A),
        ('x', *WEEKDAYS, '20260920', '20261231', B),
        ('wk', *WEEKENDS, '20260920', '20261231', B),
    ])
    table(con=con, name='dim_calendar_dates_all', columns=(
        'service_id varchar, date varchar, exception_type varchar, '
        'valid_from varchar'
    ), rows=[
        ('x', '20260923', '2', B),
        ('wk', '20260923', '1', B),
    ])
    build_stop_times(con=con)
    table(con=con, name='dim_stop_all', columns=(
        'stop_id varchar, stop_lat double, stop_lon double, '
        'valid_from varchar'
    ), rows=[
        ('s1', -33.87, 151.20, B),
        ('s2', -33.95, 151.10, B),
        ('s3', -33.80, 151.00, B),
    ])


def build_stop_times(*, con: duckdb.DuckDBPyConnection) -> None:
    """Create ``dim_scheduled_stop_time_all``; ``C`` repeats ``B``.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to create it in.
    """
    earlier = {
        'a1': ('10:00:00', '10:10:00'),
        'a2': ('10:30:00', '10:40:00'),
        'a3': ('11:00:00', '11:10:00'),
        'a4': ('11:31:00', '11:41:00'),
        'a5': ('12:00:00', '12:10:00'),
    }
    later = {
        'k1': ('08:00:00', '08:10:00', '08:20:00'),
        'k2': ('08:10:00', '08:20:00', '08:30:00'),
        'k3': ('08:20:00', '08:30:00', '08:40:00'),
        'k4': ('08:30:00', '08:40:00', '08:50:00'),
        'k5': ('08:40:00', '08:50:00', '09:00:00'),
        'ko': ('08:45:00', '08:55:00', '09:05:00'),
        'kd': ('08:50:00', '09:00:00', '09:10:00'),
        'kn': ('25:10:00', '25:20:00', '25:45:00'),
    }
    rows = [
        row
        for times, valid_from in (
            (earlier, A), (later, B), (later, C),
        )
        for trip_id, schedule in times.items()
        for row in stop_times(
            trip_id=trip_id, times=schedule, valid_from=valid_from,
        )
    ]
    table(con=con, name='dim_scheduled_stop_time_all', columns=(
        'trip_id varchar, stop_id varchar, stop_sequence integer, '
        'arrival_time varchar, departure_time varchar, timepoint varchar, '
        'pickup_type varchar, valid_from varchar'
    ), rows=rows)


def stop_row(
    *,
    trip_id: str,
    sequence: int,
    clock: str | None,
    status: str = 'SCHEDULED',
    reliable: bool = True,
    **overrides: object,
) -> Row:
    """One ``fact_trip_stop`` row, before its update time is added.

    Parameters
    ----------
    trip_id : str
        The trip.
    sequence : int
        Stop sequence.
    clock : str | None
        Predicted arrival and departure, Sydney time, or None.
    status : str
        ``schedule_relationship``.
    reliable : bool
        ``is_reliable``.
    **overrides : object
        ``service_date`` (default 22 September), ``day`` of the
        prediction, ``arrival``, ``departure``, ``lost_tracking``.

    Returns
    -------
    Row
        Service date, trip, sequence, arrival, departure, status,
        lost tracking, reliable.
    """
    service_date = overrides.get('service_date', date(2026, 9, 22))
    day = str(overrides.get('day', service_date))
    predicted = at(clock, day=day) if clock else None
    return (
        service_date, trip_id, sequence,
        overrides.get('arrival', predicted),
        overrides.get('departure', predicted),
        status, overrides.get('lost_tracking', False), reliable,
    )


def build_facts(*, con: duckdb.DuckDBPyConnection) -> None:
    """Create ``fact_trip`` and ``fact_trip_stop``.

    Every stop row was last updated 10 seconds before its predicted
    departure.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to create them in.
    """
    sept = {day: date(2026, 9, day) for day in (16, 17, 20, 22, 23)}
    table(con=con, name='fact_trip', columns=(
        'service_date date, trip_id varchar, route_id varchar, '
        'final_status varchar, canceled_polls integer, '
        'first_canceled_at_utc timestamptz, had_vehicle boolean'
    ), rows=[
        (sept[16], 'a1', 'r_bus', 'SCHEDULED', 0, None, True),
        (sept[17], 'a1', 'r_bus', 'SCHEDULED', 0, None, True),
        (sept[20], 'a2', 'r_bus', 'SCHEDULED', 0, None, True),
        (sept[22], 'k1', 'r_bus', 'SCHEDULED', 0, None, True),
        (sept[22], 'k2', 'r_bus', 'CANCELED', 50, at('07:00:00'), False),
        (sept[22], 'k3', 'r_bus', 'SCHEDULED', 5, at('07:30:00'), True),
        (sept[22], 'k5', 'r_bus', 'CANCELED', 30, at('08:45:00'), True),
        (sept[22], 'kn', 'r_bus', 'SCHEDULED', 0, None, True),
        (sept[22], 'ko', 'r_bus', 'SCHEDULED', 0, None, True),
        (sept[22], 'kd', 'r_bus', 'SCHEDULED', 0, None, True),
        (sept[22], 'kadd', 'r_bus', 'ADDED', 0, None, True),
        (sept[23], 'ko', 'r_bus', 'SCHEDULED', 0, None, True),
    ])
    table(con=con, name='stop_row', columns=(
        'service_date date, trip_id varchar, stop_sequence integer, '
        'final_predicted_arrival_utc timestamptz, '
        'final_predicted_departure_utc timestamptz, '
        'schedule_relationship varchar, lost_tracking boolean, '
        'is_reliable boolean'
    ), rows=stop_rows())
    con.execute(
        'create table fact_trip_stop as select *, '
        'final_predicted_departure_utc - interval 10 second '
        'as last_update_at_utc from stop_row',
    )
    con.execute('drop table stop_row')


def stop_rows() -> list[Row]:
    """Every ``fact_trip_stop`` row.

    Returns
    -------
    list[Row]
        Rows in ``stop_row`` form.
    """
    sunday = date(2026, 9, 20)
    return [
        stop_row(trip_id='k1', sequence=1, clock='07:59:01'),
        stop_row(trip_id='k1', sequence=2, clock='08:16:00'),
        stop_row(trip_id='k1', sequence=3, clock='08:18:00'),
        stop_row(trip_id='k3', sequence=1, clock='08:18:59'),
        stop_row(trip_id='k3', sequence=2, clock='08:35:59'),
        stop_row(trip_id='k3', sequence=3, clock='08:40:30'),
        stop_row(trip_id='k5', sequence=1, clock='08:40:00',
                 reliable=False, arrival=None),
        stop_row(trip_id='ko', sequence=1, clock='08:41:00'),
        stop_row(trip_id='ko', sequence=2, clock=None, status='SKIPPED',
                 reliable=False),
        stop_row(trip_id='ko', sequence=3, clock='09:05:00'),
        stop_row(trip_id='kd', sequence=1, clock='08:50:00',
                 day='2026-09-23'),
        stop_row(trip_id='kn', sequence=1, clock='01:10:00',
                 day='2026-09-23'),
        stop_row(trip_id='kn', sequence=2, clock='01:20:00',
                 day='2026-09-23'),
        stop_row(trip_id='kn', sequence=3, clock='01:45:00',
                 day='2026-09-23', reliable=False, lost_tracking=True),
        stop_row(trip_id='a2', sequence=1, clock='10:30:00',
                 service_date=sunday),
        stop_row(trip_id='a2', sequence=2, clock='10:40:00',
                 service_date=sunday),
    ]


def build_reference(*, con: duckdb.DuckDBPyConnection) -> None:
    """Create ``stop_geography_all`` and ``stop_meshblock_all``.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to create them in.
    """
    old, new = date(2026, 1, 1), date(2026, 9, 22)
    table(con=con, name='stop_geography_all', columns=(
        'stop_id varchar, stop_lat double, stop_lon double, '
        'sa2_code varchar, sa2_name varchar, gccsa_name varchar, '
        'irsd_score double, irsd_state_decile smallint, '
        'distance_to_nearest_centre_m double, vintage date'
    ), rows=[
        ('s1', -33.87, 151.20, 'A1', 'Alpha', 'Greater Sydney', 900.0, 2,
         1000.0, old),
        ('s1', -33.87, 151.20, 'A1', 'Alpha', 'Greater Sydney', 900.0, 2,
         1000.0, new),
        ('s2', -33.90, 151.10, 'A1', 'Alpha', 'Greater Sydney', 1000.0, 5,
         2000.0, new),
        ('s3', -33.80, 151.00, 'B2', 'Beta', 'Greater Sydney', None, None,
         3000.0, new),
        ('s9', -33.70, 150.90, 'C3', 'Gamma', 'Greater Sydney', 1100.0, 8,
         9000.0, new),
    ])
    table(con=con, name='stop_meshblock_all', columns=(
        'stop_id varchar, mesh_block_code varchar, '
        'straight_line_distance_m double, network_distance_m double, '
        'routing_status varchar, person_count integer, vintage date'
    ), rows=[
        ('s1', '10001', 100.0, 300.0, 'routed', 50, old),
        ('s1', '10001', 100.0, 300.0, 'routed', 50, new),
        ('s1', '10002', 500.0, 700.0, 'routed', 20, new),
        ('s1', '10003', 700.0, None, 'unroutable', 10, new),
    ])


def build_inputs(*, con: duckdb.DuckDBPyConnection) -> None:
    """Create every input ``analysis/marts.sql`` reads.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to create them in.
    """
    con.execute(
        'create view calendar_exclusion_seed as '
        f"select * from read_csv('{SEED}')",
    )
    build_timetable(con=con)
    build_facts(con=con)
    build_reference(con=con)
