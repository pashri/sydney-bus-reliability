"""Tests for the service-day merge SQL."""

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.compactor.positions import POSITION_SCHEMA
from src.compactor.trip_updates import TRIP_STOP_SCHEMA
from src.merger.merge_sql import POSITION_MERGE, TRIP_STOP_MERGE


@pytest.fixture
def _connection() -> duckdb.DuckDBPyConnection:
    """Provide an in-memory DuckDB connection.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A fresh connection.
    """
    return duckdb.connect()


def write_partials(
    *,
    tmp_path: Path,
    rows: list[dict[str, object]],
    schema: pa.Schema,
) -> str:
    """Write partial rows to a Parquet file and return its glob.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    rows : list[dict[str, object]]
        Partial rows to write.
    schema : pyarrow.Schema
        Explicit schema. Required rather than inferred, because a
        column that is all-None in one fixture would otherwise infer
        as pyarrow's null type and break DuckDB's typed arithmetic
        the moment a real value is missing.

    Returns
    -------
    str
        A glob matching the written file.
    """
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, tmp_path / 'hour=21.parquet')
    return str(tmp_path / '*.parquet')


def trip_row(
    *,
    hour: int,
    n_updates: int,
    delay: int,
    last_update: datetime,
) -> dict[str, object]:
    """Build one partial trip-stop row.

    Parameters
    ----------
    hour : int
        Unused marker for readability.
    n_updates : int
        Real observations counted in this hour.
    delay : int
        Predicted arrival delay.
    last_update : datetime
        When the last real observation was made.

    Returns
    -------
    dict[str, object]
        One partial row.
    """
    del hour
    return {
        'service_date': '20260917',
        'trip_id': '1012281',
        'stop_id': '200013',
        'stop_sequence': 1,
        'route_id': '2447_160',
        'final_predicted_arrival_utc': datetime(
            2026, 9, 17, 0, 0, tzinfo=UTC,
        ),
        'delay_s': delay,
        'final_predicted_departure_utc': None,
        'departure_delay_s': None,
        'last_update_at_utc': last_update,
        'n_updates': n_updates,
        'schedule_relationship': 'SCHEDULED',
        'trip_schedule_relationship': 'SCHEDULED',
        'had_vehicle': True,
        'lost_tracking': False,
        'last_observed_at_utc': last_update,
    }


def position_row(
    *,
    vehicle_id: str,
    observed_at: datetime,
    lat: float,
    lon: float,
    fetched_at: datetime,
) -> dict[str, object]:
    """Build one partial position row.

    Parameters
    ----------
    vehicle_id : str
        Vehicle identifier.
    observed_at : datetime
        When the position was reported.
    lat : float
        Latitude.
    lon : float
        Longitude.
    fetched_at : datetime
        When this observation was fetched.

    Returns
    -------
    dict[str, object]
        One partial row.
    """
    return {
        'vehicle_id': vehicle_id,
        'observed_at_utc': observed_at,
        'lat': lat,
        'lon': lon,
        'fetched_at_utc': fetched_at,
    }


def test_n_updates_sums_across_partials(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """Three hourly partials of one trip merge to the summed count.

    This is the property most likely to break silently under a
    refactor, because taking the last value instead of the sum
    produces a plausible-looking number.
    """
    rows = [
        trip_row(
            hour=hour,
            n_updates=count,
            delay=60,
            last_update=datetime(2026, 9, 16, hour, 30, tzinfo=UTC),
        )
        for hour, count in ((20, 5), (21, 7), (22, 3))
    ]
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, {'partials': glob, 'service_date': '20260917'},
    ).fetchall()
    assert len(result) == 1
    columns = [d[0] for d in _connection.description]
    assert dict(zip(columns, result[0]))['n_updates'] == 15


def test_latest_real_prediction_wins(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """Across partials, the newest real observation is retained."""
    rows = [
        trip_row(
            hour=20,
            n_updates=1,
            delay=60,
            last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
        ),
        trip_row(
            hour=22,
            n_updates=1,
            delay=300,
            last_update=datetime(2026, 9, 16, 22, 30, tzinfo=UTC),
        ),
    ]
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, {'partials': glob, 'service_date': '20260917'},
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    assert dict(zip(columns, result[0]))['delay_s'] == 300


def test_no_join_fan_out(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """Merging must never multiply rows.

    The classic way a reliability number silently doubles.
    """
    rows = [
        trip_row(
            hour=hour,
            n_updates=1,
            delay=60,
            last_update=datetime(2026, 9, 16, hour, 30, tzinfo=UTC),
        )
        for hour in (20, 21, 22, 23)
    ]
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, {'partials': glob, 'service_date': '20260917'},
    ).fetchall()
    assert len(result) == 1


def test_lost_tracking_propagates(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A dropout in any partial marks the merged row."""
    rows = [
        trip_row(
            hour=20,
            n_updates=1,
            delay=60,
            last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
        ),
    ]
    rows[0]['lost_tracking'] = True
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, {'partials': glob, 'service_date': '20260917'},
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    merged = dict(zip(columns, result[0]))
    assert merged['lost_tracking']
    assert not merged['is_reliable']


def test_null_delay_is_not_reliable(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A NO_DATA-only row has a NULL delay and cannot be reliable."""
    row = trip_row(
        hour=20,
        n_updates=0,
        delay=60,
        last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
    )
    row['delay_s'] = None
    row['final_predicted_arrival_utc'] = None
    glob = write_partials(
        tmp_path=tmp_path, rows=[row], schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, {'partials': glob, 'service_date': '20260917'},
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    merged = dict(zip(columns, result[0]))
    assert not merged['is_reliable']


def test_stale_prediction_beyond_lead_is_not_reliable(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A prediction unchanged more than the lead window is a forecast."""
    row = trip_row(
        hour=20,
        n_updates=1,
        delay=60,
        last_update=datetime(2026, 9, 16, 23, 58, tzinfo=UTC),
    )
    glob = write_partials(
        tmp_path=tmp_path, rows=[row], schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, {'partials': glob, 'service_date': '20260917'},
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    merged = dict(zip(columns, result[0]))
    assert not merged['is_reliable']


def test_position_merge_dedupes_on_key(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A restated position appearing in two partials merges to one row.

    The key includes position, not just vehicle and timestamp, since
    many vehicles can share a timestamp while reporting a different
    location.
    """
    observed_at = datetime(2026, 9, 16, 20, 30, tzinfo=UTC)
    rows = [
        position_row(
            vehicle_id='v1',
            observed_at=observed_at,
            lat=-33.8,
            lon=151.2,
            fetched_at=datetime(2026, 9, 16, 20, 31, tzinfo=UTC),
        ),
        position_row(
            vehicle_id='v1',
            observed_at=observed_at,
            lat=-33.8,
            lon=151.2,
            fetched_at=datetime(2026, 9, 16, 21, 1, tzinfo=UTC),
        ),
        position_row(
            vehicle_id='v1',
            observed_at=observed_at,
            lat=-33.9,
            lon=151.3,
            fetched_at=datetime(2026, 9, 16, 21, 1, tzinfo=UTC),
        ),
    ]
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=POSITION_SCHEMA,
    )
    result = _connection.execute(
        POSITION_MERGE,
        {
            'partials': glob,
            'window_start': datetime(2026, 9, 16, 20, 0, tzinfo=UTC),
            'window_end': datetime(2026, 9, 16, 21, 0, tzinfo=UTC),
        },
    ).fetchall()
    assert len(result) == 2
