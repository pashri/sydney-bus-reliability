"""Tests for the service-day merge SQL."""

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compactor.positions import POSITION_SCHEMA
from compactor.trip_updates import TRIP_STOP_SCHEMA
from merger.merge_sql import (
    POSITION_MERGE,
    TRIP_STOP_MERGE,
    build_trip_stop_query,
)


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
    dt: str = '2026-09-16',
    hour: str = '21',
) -> str:
    """Write partial rows to a Parquet file and return its glob.

    The file goes under ``dt=<dt>/hour=<hour>/``, matching the layout
    the compactor writes, because the merge SQL reads those path
    segments as hive partition columns.

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
    dt : str
        UTC date partition to write into.
    hour : str
        UTC hour partition to write into.

    Returns
    -------
    str
        A glob matching every partition written under ``tmp_path``.
    """
    table = pa.Table.from_pylist(rows, schema=schema)
    partition = tmp_path / f'dt={dt}' / f'hour={hour}'
    partition.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, partition / 'data.parquet')
    return str(tmp_path / 'dt=*' / 'hour=*' / 'data.parquet')


def merge_params(
    *,
    glob: str,
    service_date: str = '20260917',
    dt_from: str = '2026-09-01',
    dt_to: str = '2026-10-01',
) -> dict[str, str]:
    """Build the parameters ``TRIP_STOP_MERGE`` binds.

    Parameters
    ----------
    glob : str
        Glob matching the partials to read.
    service_date : str
        Service date to assemble, as ``YYYYMMDD``.
    dt_from : str
        Earliest ``dt`` partition to scan, as ``YYYY-MM-DD``.
    dt_to : str
        Latest ``dt`` partition to scan, as ``YYYY-MM-DD``.

    Returns
    -------
    dict[str, str]
        Parameters for the merge query.
    """
    return {
        'partials': glob,
        'service_date': service_date,
        'dt_from': dt_from,
        'dt_to': dt_to,
    }


def trip_row(
    *,
    hour: int,
    n_updates: int,
    delay: int,
    last_update: datetime,
    last_observed: datetime | None = None,
    stop_sequence: int = 1,
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
    last_observed : datetime | None
        When the key was last observed at all, real or echo. Defaults
        to ``last_update`` when no dropout is being modelled.
    stop_sequence : int
        Position of this call in the trip.

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
        'stop_sequence': stop_sequence,
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
        'last_observed_at_utc': last_observed or last_update,
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
        TRIP_STOP_MERGE, merge_params(glob=glob),
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
        TRIP_STOP_MERGE, merge_params(glob=glob),
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
        TRIP_STOP_MERGE, merge_params(glob=glob),
    ).fetchall()
    assert len(result) == 1


def test_lost_tracking_reflects_the_final_dropout(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A key whose day ends on an echo after its last real call is lost."""
    rows = [
        trip_row(
            hour=20,
            n_updates=1,
            delay=60,
            last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
            last_observed=datetime(2026, 9, 16, 20, 45, tzinfo=UTC),
        ),
    ]
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, merge_params(glob=glob),
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    merged = dict(zip(columns, result[0]))
    assert merged['lost_tracking']
    assert merged['is_reliable'] is False


def test_never_reported_key_is_not_lost_tracking(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A key with no real observation at all is never-reported, not lost."""
    row = trip_row(
        hour=20,
        n_updates=0,
        delay=60,
        last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
        last_observed=datetime(2026, 9, 16, 20, 45, tzinfo=UTC),
    )
    row['delay_s'] = None
    row['final_predicted_arrival_utc'] = None
    row['last_update_at_utc'] = None
    row['had_vehicle'] = False
    glob = write_partials(
        tmp_path=tmp_path, rows=[row], schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, merge_params(glob=glob),
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    merged = dict(zip(columns, result[0]))
    assert not merged['lost_tracking']
    assert not merged['had_vehicle']


def test_a_later_real_hour_clears_an_earlier_dropout(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """NO_DATA late in hour 20 then real SCHEDULED in hour 21 must
    end reliable.

    Regression for BOOL_OR(lost_tracking) across per-hour partials:
    that formulation ORs the hour-20 dropout across the whole day and
    wrongly disqualifies the hour-21 arrival, even though hour 21 is a
    genuine, later real observation. Discrimination check performed
    manually against the pre-fix BOOL_OR SQL confirmed it returns
    ``lost_tracking = True`` / ``is_reliable = False`` for this
    fixture, while the MAX-based comparison here returns
    ``lost_tracking = False`` / ``is_reliable = True``.
    """
    rows = [
        trip_row(
            hour=20,
            n_updates=0,
            delay=0,
            last_update=datetime(2026, 9, 16, 20, 0, tzinfo=UTC),
            last_observed=datetime(2026, 9, 16, 20, 55, tzinfo=UTC),
        ),
        trip_row(
            hour=21,
            n_updates=1,
            delay=30,
            last_update=datetime(2026, 9, 16, 21, 59, 30, tzinfo=UTC),
            last_observed=datetime(2026, 9, 16, 21, 59, 30, tzinfo=UTC),
        ),
    ]
    rows[0]['delay_s'] = None
    rows[0]['final_predicted_arrival_utc'] = None
    rows[0]['last_update_at_utc'] = None
    rows[1]['final_predicted_arrival_utc'] = datetime(
        2026, 9, 16, 22, 0, tzinfo=UTC,
    )
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, merge_params(glob=glob),
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    merged = dict(zip(columns, result[0]))
    assert not merged['lost_tracking']
    assert merged['is_reliable'] is True


def test_loop_route_stop_sequence_keeps_both_calls(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """Same trip, same stop_id, two stop_sequences: two merged rows."""
    rows = [
        trip_row(
            hour=20,
            n_updates=1,
            delay=60,
            last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
            stop_sequence=3,
        ),
        trip_row(
            hour=20,
            n_updates=1,
            delay=300,
            last_update=datetime(2026, 9, 16, 20, 45, tzinfo=UTC),
            stop_sequence=17,
        ),
    ]
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, merge_params(glob=glob),
    ).fetchall()
    assert len(result) == 2
    columns = [d[0] for d in _connection.description]
    by_sequence = {
        row['stop_sequence']: row['delay_s']
        for row in (dict(zip(columns, r)) for r in result)
    }
    assert by_sequence == {3: 60, 17: 300}


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
        TRIP_STOP_MERGE, merge_params(glob=glob),
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    merged = dict(zip(columns, result[0]))
    assert merged['is_reliable'] is False


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
        TRIP_STOP_MERGE, merge_params(glob=glob),
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    merged = dict(zip(columns, result[0]))
    assert merged['is_reliable'] is False


def test_delay_without_predicted_arrival_is_not_reliable(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A delay with no arrival time to compare it against reads False.

    The feed can send a stop-time event carrying ``delay`` and no
    ``time``. Comparing the last update against a missing arrival
    yields NULL, so the flag is coalesced rather than left unknown.
    """
    row = trip_row(
        hour=20,
        n_updates=1,
        delay=60,
        last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
    )
    row['final_predicted_arrival_utc'] = None
    glob = write_partials(
        tmp_path=tmp_path, rows=[row], schema=TRIP_STOP_SCHEMA,
    )
    result = _connection.execute(
        TRIP_STOP_MERGE, merge_params(glob=glob),
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    merged = dict(zip(columns, result[0]))
    assert merged['delay_s'] == 60
    assert merged['final_predicted_arrival_utc'] is None
    assert merged['is_reliable'] is False


def test_merged_counter_and_flag_survive_a_parquet_round_trip(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """``n_updates`` stays a whole number and ``is_reliable`` a boolean.

    Asserted on the written file rather than on the query result.
    ``SUM`` widens the counter to a type Parquet cannot hold, so the
    narrowing is only observable once the file is read back.
    """
    rows = [
        trip_row(hour=20, n_updates=3, delay=60,
                 last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC)),
        trip_row(hour=21, n_updates=4, delay=60,
                 last_update=datetime(2026, 9, 16, 21, 30, tzinfo=UTC)),
    ]
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=TRIP_STOP_SCHEMA,
    )
    target = tmp_path / 'fact_trip_stop.parquet'
    _connection.execute(
        f"COPY ({TRIP_STOP_MERGE}) TO '{target}' (FORMAT PARQUET)",
        merge_params(glob=glob),
    )
    schema = pq.read_schema(target)
    assert schema.field('n_updates').type == pa.int32()
    assert schema.field('is_reliable').type == pa.bool_()


def test_merged_columns_keep_the_partial_types(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """Every column the merge carries through keeps its stored type.

    Read back from the written file, not from the query, and compared
    against the partial's own schema. A tool that reads a fact object
    by its full path picks up ``service_date`` from the
    ``service_date=`` path segment unless it opts out, and writing that
    back changes the column's type without changing a row count.
    """
    row = trip_row(
        hour=20,
        n_updates=1,
        delay=60,
        last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
    )
    glob = write_partials(
        tmp_path=tmp_path, rows=[row], schema=TRIP_STOP_SCHEMA,
    )
    target = tmp_path / 'fact.parquet'
    _connection.execute(
        f"COPY ({build_trip_stop_query(dim_source=None)}) TO '{target}' "
        '(FORMAT PARQUET)',
        merge_params(glob=glob),
    )
    written = pq.read_schema(target)
    # service_date is deliberately widened to a date, to agree with the
    # service_date= partition it is written under. Everything else must
    # arrive unchanged.
    assert written.field('service_date').type == pa.date32()
    carried = [
        field for field in TRIP_STOP_SCHEMA
        if field.name in written.names
        and field.name not in {'last_observed_at_utc', 'service_date'}
    ]
    assert carried
    for field in carried:
        stored = written.field(field.name).type
        if pa.types.is_timestamp(field.type):
            assert pa.types.is_timestamp(stored), field.name
        else:
            assert stored == field.type, field.name


def test_hive_partition_columns_are_not_merged_in(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """``dt`` and ``hour`` come from the path and must not be output.

    They exist only so whole objects can be pruned before their
    footers are read. Letting them through would change the merged
    schema away from the partial's own.
    """
    rows = [
        trip_row(
            hour=20,
            n_updates=1,
            delay=60,
            last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
        ),
    ]
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=TRIP_STOP_SCHEMA,
    )
    _connection.execute(TRIP_STOP_MERGE, merge_params(glob=glob))
    columns = {d[0] for d in _connection.description}
    assert not columns & {'dt', 'hour'}
    assert 'service_date' in columns


def test_position_merge_drops_hive_partition_columns(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """The position merge selects ``*``, so it must exclude them too."""
    rows = [
        position_row(
            vehicle_id='v1',
            observed_at=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
            lat=-33.8,
            lon=151.2,
            fetched_at=datetime(2026, 9, 16, 20, 31, tzinfo=UTC),
        ),
    ]
    glob = write_partials(
        tmp_path=tmp_path, rows=rows, schema=POSITION_SCHEMA,
    )
    _connection.execute(
        POSITION_MERGE,
        {
            'partials': glob,
            'window_start': datetime(2026, 9, 16, 20, 0, tzinfo=UTC),
            'window_end': datetime(2026, 9, 16, 21, 0, tzinfo=UTC),
            'dt_from': '2026-09-01',
            'dt_to': '2026-10-01',
        },
    )
    columns = {d[0] for d in _connection.description}
    assert not columns & {'dt', 'hour'}
    assert set(POSITION_SCHEMA.names) <= columns


def test_partitions_outside_the_bounds_are_not_read(
    _connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    """A partial outside the ``dt`` bounds does not reach the merge.

    Its rows carry the service date under assembly, so reading it
    would change the answer. Only the partition path excludes it.
    """
    write_partials(
        tmp_path=tmp_path,
        rows=[
            trip_row(
                hour=20,
                n_updates=1,
                delay=60,
                last_update=datetime(2026, 9, 16, 20, 30, tzinfo=UTC),
            ),
        ],
        schema=TRIP_STOP_SCHEMA,
    )
    glob = write_partials(
        tmp_path=tmp_path,
        rows=[
            trip_row(
                hour=20,
                n_updates=500,
                delay=60,
                last_update=datetime(2026, 8, 1, 20, 30, tzinfo=UTC),
            ),
        ],
        schema=TRIP_STOP_SCHEMA,
        dt='2026-08-01',
        hour='20',
    )
    result = _connection.execute(
        TRIP_STOP_MERGE,
        merge_params(glob=glob, dt_from='2026-09-15', dt_to='2026-09-18'),
    ).fetchall()
    columns = [d[0] for d in _connection.description]
    assert len(result) == 1
    assert dict(zip(columns, result[0]))['n_updates'] == 1

    widened = _connection.execute(
        TRIP_STOP_MERGE,
        merge_params(glob=glob, dt_from='2026-08-01', dt_to='2026-09-18'),
    ).fetchall()
    assert dict(zip(columns, widened[0]))['n_updates'] == 501


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
            'dt_from': '2026-09-01',
            'dt_to': '2026-10-01',
        },
    ).fetchall()
    assert len(result) == 2
