"""Tests for the service-day merge of trip-level status."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compactor.trips import TRIP_SCHEMA
from merger.merge_sql import build_trip_query
from tests.merger.test_merge_sql import write_schedule

HOUR: datetime = datetime(2026, 9, 16, 21, 0, tzinfo=UTC)


@pytest.fixture
def _connection() -> duckdb.DuckDBPyConnection:
    """Provide an in-memory DuckDB connection.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A fresh connection.
    """
    return duckdb.connect()


def status_row(*, hour: int, **overrides: Any) -> dict[str, Any]:
    """Build one partial trip row observed within one UTC hour.

    Parameters
    ----------
    hour : int
        Hours after ``HOUR`` the row's polls fall in.
    **overrides : Any
        Column values replacing the defaults.

    Returns
    -------
    dict[str, Any]
        One partial row.
    """
    start = HOUR + timedelta(hours=hour)
    return {
        'start_date': '20260917',
        'trip_id': '1012281',
        'route_id': '2447_160',
        'start_time': '07:30:00',
        'final_status': 'SCHEDULED',
        'final_status_at_utc': start + timedelta(minutes=59),
        'scheduled_polls': 60,
        'canceled_polls': 0,
        'added_polls': 0,
        'first_seen_at_utc': start,
        'last_seen_at_utc': start + timedelta(minutes=59),
        'first_canceled_at_utc': None,
        'last_canceled_at_utc': None,
        'had_vehicle': False,
        **overrides,
    }


def write_hours(*, tmp_path: Path, rows: list[dict[str, Any]]) -> str:
    """Write each row to the partial of the hour it was seen in.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    rows : list[dict[str, Any]]
        Partial rows, one hour each.

    Returns
    -------
    str
        A glob matching every partial written.
    """
    for row in rows:
        seen: datetime = row['first_seen_at_utc']
        partition = tmp_path / f'dt={seen:%Y-%m-%d}' / f'hour={seen:%H}'
        partition.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pylist([row], schema=TRIP_SCHEMA),
            partition / 'data.parquet',
        )
    return str(tmp_path / 'dt=*' / 'hour=*' / 'data.parquet')


def merge(
    *,
    connection: duckdb.DuckDBPyConnection,
    glob: str,
    service_date: str = '20260917',
    dim_source: str | None = None,
) -> list[dict[str, Any]]:
    """Run the trip merge and return its rows as dicts.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Open connection.
    glob : str
        Glob of partials to read.
    service_date : str
        Service date to assemble, as ``YYYYMMDD``.
    dim_source : str | None
        Timetable snapshot to consult, or None.

    Returns
    -------
    list[dict[str, Any]]
        Merged rows.
    """
    result = connection.execute(
        build_trip_query(dim_source=dim_source),
        {
            'partials': glob,
            'service_date': service_date,
            'dt_from': '2026-09-01',
            'dt_to': '2026-10-01',
        },
    ).fetchall()
    columns = [d[0] for d in connection.description]
    return [dict(zip(columns, row)) for row in result]


def test_polls_sum_across_hours(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """Each hour counts its own polls, so the day is their sum."""
    glob = write_hours(tmp_path=tmp_path, rows=[
        status_row(hour=0, scheduled_polls=50, canceled_polls=10),
        status_row(hour=1, scheduled_polls=5, canceled_polls=55),
    ])
    row = merge(connection=_connection, glob=glob)[0]
    assert row['scheduled_polls'] == 55
    assert row['canceled_polls'] == 65
    assert row['added_polls'] == 0


def test_final_status_comes_from_the_latest_hour(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A trip cancelled then reinstated ends SCHEDULED."""
    glob = write_hours(tmp_path=tmp_path, rows=[
        status_row(hour=1, final_status='SCHEDULED'),
        status_row(
            hour=0,
            final_status='CANCELED',
            first_canceled_at_utc=HOUR + timedelta(minutes=5),
            last_canceled_at_utc=HOUR + timedelta(minutes=40),
        ),
    ])
    row = merge(connection=_connection, glob=glob)[0]
    assert row['final_status'] == 'SCHEDULED'
    assert row['first_canceled_at_utc'] == HOUR + timedelta(minutes=5)
    assert row['last_canceled_at_utc'] == HOUR + timedelta(minutes=40)


def test_cancellation_span_widens_across_hours(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """The earliest and latest CANCELED polls come from any hour."""
    glob = write_hours(tmp_path=tmp_path, rows=[
        status_row(
            hour=hour,
            final_status='CANCELED',
            first_canceled_at_utc=HOUR + timedelta(hours=hour),
            last_canceled_at_utc=HOUR + timedelta(hours=hour, minutes=59),
        )
        for hour in (0, 1, 2)
    ])
    row = merge(connection=_connection, glob=glob)[0]
    assert row['final_status'] == 'CANCELED'
    assert row['first_canceled_at_utc'] == HOUR
    assert row['last_canceled_at_utc'] == (
        HOUR + timedelta(hours=2, minutes=59)
    )


def test_seen_span_and_vehicle_combine_across_hours(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """Seen times span every hour and one vehicle sighting is enough."""
    glob = write_hours(tmp_path=tmp_path, rows=[
        status_row(hour=0, had_vehicle=True, start_time='07:30:00'),
        status_row(hour=1, start_time='07:32:00'),
    ])
    row = merge(connection=_connection, glob=glob)[0]
    assert row['first_seen_at_utc'] == HOUR
    assert row['last_seen_at_utc'] == HOUR + timedelta(minutes=119)
    assert row['had_vehicle'] is True
    assert row['start_time'] == '07:32:00'


def test_only_the_requested_service_date_is_merged(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """Rows for another start date are left out."""
    glob = write_hours(tmp_path=tmp_path, rows=[
        status_row(hour=0),
        status_row(hour=1, start_date='20260918', trip_id='other'),
    ])
    rows = merge(connection=_connection, glob=glob)
    assert [row['trip_id'] for row in rows] == ['1012281']


def test_merged_columns_have_fact_types(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """Counts stay 32-bit and the service date becomes a date."""
    glob = write_hours(tmp_path=tmp_path, rows=[
        status_row(hour=0), status_row(hour=1),
    ])
    target = tmp_path / 'fact_trip.parquet'
    query = build_trip_query(dim_source=None)
    _connection.execute(
        f"COPY ({query}) TO '{target}' (FORMAT PARQUET)",
        {
            'partials': glob,
            'service_date': '20260917',
            'dt_from': '2026-09-01',
            'dt_to': '2026-10-01',
        },
    )
    schema = pq.read_schema(target)
    assert schema.field('service_date').type == pa.date32()
    for name in ('scheduled_polls', 'canceled_polls', 'added_polls'):
        assert schema.field(name).type == pa.int32(), name
    assert not {'dt', 'hour'} & set(schema.names)


def test_trip_timetabled_after_midnight_joins_the_previous_day(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A cancelled 24:30 trip is counted on the day it belongs to."""
    (tmp_path / 'dim').mkdir()
    (tmp_path / 'partials').mkdir()
    glob = write_hours(tmp_path=tmp_path / 'partials', rows=[
        status_row(hour=0, start_date='20260918', final_status='CANCELED'),
    ])
    schedule = write_schedule(
        tmp_path=tmp_path / 'dim', first_times={'1012281': '24:30:00'},
    )
    for service_date, expected in (('20260917', 1), ('20260918', 0)):
        rows = merge(
            connection=_connection, glob=glob,
            service_date=service_date, dim_source=schedule,
        )
        assert len(rows) == expected
    rows = merge(
        connection=_connection, glob=glob, dim_source=schedule,
    )
    assert str(rows[0]['service_date']) == '2026-09-17'
    assert rows[0]['start_date'] == '20260918'
