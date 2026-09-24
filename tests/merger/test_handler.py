"""Tests for the daily merger handler."""

import io
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from common.collector_run import COLLECTOR_RUN_COLUMNS
from common.connection import configure, create_s3_secret, load_extensions
from common.service_day import merge_window
from common.types_ import RunRecord
from compactor.positions import POSITION_SCHEMA
from compactor.trip_updates import TRIP_STOP_SCHEMA
from compactor.trips import TRIP_SCHEMA
from merger.handler import (
    DUCKDB_LIMITS,
    MergeTable,
    handler,
    merge_trip_stops,
    merge_trips,
    partial_hour_from_key,
    partial_paths,
    partition_bounds,
    target_service_date,
)
from merger.schedule import resolve_dim_source
from schedule_loader.dimensions import SCHEDULED_STOP_TIME_SCHEMA


class _Context:
    """Minimal Lambda context for Powertools."""

    function_name = 'sydney-bus-reliability-merger'
    memory_limit_in_mb = 2048
    invoked_function_arn = (
        'arn:aws:lambda:ap-southeast-2:000000000000:function:test'
    )
    aws_request_id = 'test-request-id'


def test_target_service_date_defaults_to_yesterday() -> None:
    """Running at 04:00 Sydney assembles the day that just ended."""
    now = datetime(2026, 9, 17, 18, 0, tzinfo=UTC)
    assert target_service_date(event={}, now=now) == date(2026, 9, 17)


def test_target_service_date_accepts_explicit_backfill() -> None:
    """An explicit date enables re-running after a failure."""
    assert target_service_date(
        event={'service_date': '2026-09-16'},
        now=datetime(2026, 9, 17, 18, 0, tzinfo=UTC),
    ) == date(2026, 9, 16)


def test_merge_collector_run_folds_jsonl_to_parquet(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """A day of JSONL audit objects becomes one Parquet table."""
    client = boto3.client('s3')
    for index in range(3):
        client.put_object(
            Bucket=_bucket,
            Key=f'curated/collector_run/dt=2026-09-17/{index}.jsonl',
            Body=json.dumps({
                'feed': 'vehiclepos',
                'fetched_at_utc': '2026-09-17T02:00:00+00:00',
                'received_at_utc': '2026-09-17T02:00:01+00:00',
                'rtt_s': 0.5,
                'server_date_utc': None,
                'skew_s': None,
                'status_code': 200,
                'body_bytes': 1234,
                'error': None,
            }).encode() + b'\n',
        )
    os.environ['BUCKET_NAME'] = _bucket
    from merger.handler import merge_collector_run

    assert merge_collector_run(
        bucket=_bucket, service_date=date(2026, 9, 17),
        endpoint=_s3_endpoint,
    ) == 3
    written = client.list_objects_v2(
        Bucket=_bucket, Prefix='curated/fact_collector_run/',
    )
    assert [item['Key'] for item in written['Contents']] == [
        'curated/fact_collector_run/collection_date=2026-09-17/data.parquet',
    ]


def collector_row(*, fetched_at: str) -> bytes:
    """Build one collector audit line.

    Parameters
    ----------
    fetched_at : str
        ISO 8601 UTC timestamp for the fetch.

    Returns
    -------
    bytes
        One JSON Lines record.
    """
    return json.dumps({
        'feed': 'vehiclepos',
        'fetched_at_utc': fetched_at,
        'received_at_utc': fetched_at,
        'rtt_s': 0.5,
        'server_date_utc': None,
        'skew_s': None,
        'status_code': 200,
        'body_bytes': 1234,
        'error': None,
    }).encode() + b'\n'


def put_collector_rows(
    *,
    bucket: str,
    stamps: dict[str, list[str]],
) -> None:
    """Write audit lines into their UTC date partitions.

    Parameters
    ----------
    bucket : str
        Destination bucket.
    stamps : dict[str, list[str]]
        UTC date partition, mapped to the fetch timestamps in it.
    """
    client = boto3.client('s3')
    for day, values in stamps.items():
        for index, fetched_at in enumerate(values):
            client.put_object(
                Bucket=bucket,
                Key=f'curated/collector_run/dt={day}/{index}.jsonl',
                Body=collector_row(fetched_at=fetched_at),
            )


def test_collector_run_columns_cover_every_run_record_field() -> None:
    """The declared column list must not drift from ``RunRecord``.

    The reader is given an explicit schema, so a field added to the
    record would otherwise be dropped silently on the way to Parquet.
    """
    declared = {
        part.split(':')[0].strip()
        for part in COLLECTOR_RUN_COLUMNS.strip('{}').split(',')
    }
    assert declared == set(RunRecord.__annotations__)


def test_merge_collector_run_cuts_a_sydney_day_from_two_utc_days(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """One Sydney day spans two UTC partitions and excludes both edges.

    Sydney is ten hours ahead in September, so 2026-09-17 runs from
    2026-09-16T14:00Z to 2026-09-17T14:00Z.
    """
    put_collector_rows(bucket=_bucket, stamps={
        '2026-09-16': [
            '2026-09-16T13:59:59.999999+00:00',
            '2026-09-16T14:00:00+00:00',
        ],
        '2026-09-17': [
            '2026-09-17T13:59:59.999999+00:00',
            '2026-09-17T14:00:00+00:00',
        ],
    })
    os.environ['BUCKET_NAME'] = _bucket
    from merger.handler import merge_collector_run

    assert merge_collector_run(
        bucket=_bucket, service_date=date(2026, 9, 17),
        endpoint=_s3_endpoint,
    ) == 2


def test_merge_collector_run_follows_daylight_saving(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """The cut uses Sydney's offset on the day, not a fixed one.

    Sydney moves to daylight saving on 2026-10-04, so that service day
    runs 2026-10-03T14:00Z to 2026-10-04T13:00Z and is 23 hours long.
    Holding the offset at ten hours would pull in the hour after it.
    """
    put_collector_rows(bucket=_bucket, stamps={
        '2026-10-03': [
            '2026-10-03T13:59:59.999999+00:00',
            '2026-10-03T14:00:00+00:00',
        ],
        '2026-10-04': [
            '2026-10-04T12:59:59.999999+00:00',
            '2026-10-04T13:00:00+00:00',
        ],
    })
    os.environ['BUCKET_NAME'] = _bucket
    from merger.handler import merge_collector_run

    assert merge_collector_run(
        bucket=_bucket, service_date=date(2026, 10, 4),
        endpoint=_s3_endpoint,
    ) == 2


def test_merge_collector_run_stores_timestamps_as_instants(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """Timestamp columns are typed, not left to the reader to guess.

    Mixed fractional-second formats in one day make inference fall
    back to text, so the types are declared and cast explicitly.
    """
    put_collector_rows(bucket=_bucket, stamps={'2026-09-16': [
        '2026-09-16T23:00:00+00:00',
        '2026-09-16T23:30:00.123456+00:00',
    ]})
    os.environ['BUCKET_NAME'] = _bucket
    from merger.handler import merge_collector_run

    assert merge_collector_run(
        bucket=_bucket, service_date=date(2026, 9, 17),
        endpoint=_s3_endpoint,
    ) == 2
    body = boto3.client('s3').get_object(
        Bucket=_bucket,
        Key='curated/fact_collector_run/collection_date=2026-09-17/data.parquet',
    )['Body'].read()
    schema = pq.read_schema(io.BytesIO(body))
    assert schema.field('fetched_at_utc').type == pa.timestamp('us', tz='UTC')
    assert schema.field('status_code').type == pa.int32()


def test_merge_collector_run_skips_a_day_with_no_source(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """A service day with neither source partition writes nothing.

    DuckDB raises on a glob matching no files, so the days are checked
    before the query is built rather than after it fails.
    """
    put_collector_rows(bucket=_bucket, stamps={
        '2026-09-16': ['2026-09-16T23:00:00+00:00'],
    })
    os.environ['BUCKET_NAME'] = _bucket
    from merger.handler import merge_collector_run

    assert merge_collector_run(
        bucket=_bucket, service_date=date(2026, 12, 25),
        endpoint=_s3_endpoint,
    ) == 0
    written = boto3.client('s3').list_objects_v2(
        Bucket=_bucket, Prefix='curated/fact_collector_run/',
    )
    assert written['KeyCount'] == 0


def test_merge_collector_run_reads_only_the_two_days_it_needs(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """Days outside the service day are never opened.

    An unrelated partition holds unparseable JSON. Naming the two days
    it needs, rather than globbing every day and filtering, means the
    merge never reads it.
    """
    put_collector_rows(bucket=_bucket, stamps={
        '2026-09-16': ['2026-09-16T23:00:00+00:00'],
        '2026-09-17': ['2026-09-17T02:00:00+00:00'],
    })
    boto3.client('s3').put_object(
        Bucket=_bucket,
        Key='curated/collector_run/dt=2026-01-01/broken.jsonl',
        Body=b'this is not json at all {{{\n',
    )
    os.environ['BUCKET_NAME'] = _bucket
    from merger.handler import merge_collector_run

    assert merge_collector_run(
        bucket=_bucket, service_date=date(2026, 9, 17),
        endpoint=_s3_endpoint,
    ) == 2


def test_selected_tables_defaults_to_every_table() -> None:
    """A scheduled event carries no list and assembles everything."""
    from merger.handler import selected_tables

    assert selected_tables(event={}) == frozenset(MergeTable)
    assert selected_tables(event={'tables': []}) == frozenset(MergeTable)


def test_selected_tables_narrows_to_the_named_ones() -> None:
    """A re-run can ask for one table."""
    from merger.handler import selected_tables

    assert selected_tables(event={'tables': ['collector_run']}) == {
        MergeTable.COLLECTOR_RUN,
    }


def test_selected_tables_rejects_an_unknown_name() -> None:
    """A typo must not quietly assemble nothing."""
    from merger.handler import selected_tables

    with pytest.raises(ValueError, match='unknown table'):
        selected_tables(event={'tables': ['fact_trip_stop']})


def test_require_partials_refuses_a_day_with_none_left() -> None:
    """An expired window must not overwrite a good day with an empty one.

    The partial glob spans every date, so a window with nothing left
    matches no files and yields zero rows rather than failing.
    """
    from merger.handler import require_partials

    with pytest.raises(RuntimeError, match='no partials remain'):
        require_partials(coverage=(64, 0), service_date=date(2026, 9, 17))
    require_partials(coverage=(64, 1), service_date=date(2026, 9, 17))


def test_handler_rebuilds_collector_run_without_any_partials(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """Asking for collector_run alone works when the partials have gone.

    This is what a backfill does. The table is folded from the
    collector's JSONL, so it does not need a partial, and the guard
    must not fire for it.
    """
    put_collector_rows(bucket=_bucket, stamps={
        '2026-09-16': ['2026-09-16T23:00:00+00:00'],
        '2026-09-17': ['2026-09-17T02:00:00+00:00'],
    })
    os.environ['BUCKET_NAME'] = _bucket
    from merger.handler import handler

    record = handler(
        {'service_date': '2026-09-17', 'tables': ['collector_run']},
        _Context(),
        endpoint=_s3_endpoint,
    )
    assert record['rows_out'] == 2
    written = boto3.client('s3').list_objects_v2(
        Bucket=_bucket, Prefix='curated/',
    )
    keys = {item['Key'] for item in written['Contents']}
    assert ('curated/fact_collector_run/collection_date=2026-09-17'
            '/data.parquet') in keys
    assert not any(key.startswith('curated/fact_trip_stop/') for key in keys)


def test_partition_bounds_pad_the_merge_window() -> None:
    """The bounds cover the window's UTC dates, plus a day each side.

    The padding matters because a trip is listed in the feed before it
    departs, so a service date appears in partials written before its
    own window opens.
    """
    window = merge_window(service_date=date(2026, 9, 17))
    assert partition_bounds(window=window) == (
        '2026-09-15', '2026-09-18',
    )


def test_partition_bounds_cover_a_daylight_saving_transition() -> None:
    """The window shifts with the UTC offset; the bounds still hold."""
    window = merge_window(service_date=date(2026, 4, 5))
    dt_from, dt_to = partition_bounds(window=window)
    assert dt_from <= f'{window[0]:%Y-%m-%d}'
    assert dt_to >= f'{window[1]:%Y-%m-%d}'


def test_load_extensions_prefers_the_baked_httpfs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """A layer copy of httpfs is loaded without reaching the network.

    The merger cold-starts on every scheduled run, so an extension
    fetched at runtime would put DuckDB's extension repository on the
    critical path of every merge.
    """
    staging = duckdb.connect(
        config={'extension_directory': str(tmp_path)},
    )
    staging.execute('INSTALL httpfs;')
    staging.execute('INSTALL aws;')
    baked = next(tmp_path.glob('*/*/httpfs.duckdb_extension'))
    monkeypatch.setattr('common.connection.HTTPFS_EXTENSION', baked)
    monkeypatch.setattr(
        'common.connection.AWS_EXTENSION',
        next(tmp_path.glob('*/*/aws.duckdb_extension')),
    )

    connection = duckdb.connect()
    load_extensions(connection=connection)

    loaded = connection.execute(
        "SELECT loaded FROM duckdb_extensions() "
        "WHERE extension_name = 'httpfs'",
    ).fetchone()
    autoinstall = connection.execute(
        "SELECT current_setting('autoinstall_known_extensions')",
    ).fetchone()
    assert loaded == (True,)
    assert autoinstall == (False,)


def test_baked_extensions_still_allow_the_s3_secret(
    _credentials: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Creating the S3 secret survives autoloading being turned off.

    Loading httpfs from the layer disables autoloading, and the
    ``credential_chain`` provider lives in the separate ``aws``
    extension. Without that loaded too, every merge fails at
    ``CREATE SECRET`` before reading a row.
    """
    staging = duckdb.connect(
        config={'extension_directory': str(tmp_path)},
    )
    staging.execute('INSTALL httpfs;')
    staging.execute('INSTALL aws;')
    monkeypatch.setattr(
        'common.connection.HTTPFS_EXTENSION',
        next(tmp_path.glob('*/*/httpfs.duckdb_extension')),
    )
    monkeypatch.setattr(
        'common.connection.AWS_EXTENSION',
        next(tmp_path.glob('*/*/aws.duckdb_extension')),
    )

    connection = duckdb.connect()
    load_extensions(connection=connection)
    create_s3_secret(connection=connection, endpoint=None)

    secrets = connection.execute(
        "SELECT count(*) FROM duckdb_secrets() WHERE type = 's3'",
    ).fetchone()
    assert secrets == (1,)


def test_configure_resolves_a_named_timezone(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """icu is available without being installed at runtime.

    It is compiled into the DuckDB wheel. Without it ``AT TIME ZONE``
    cannot resolve ``Australia/Sydney``, and every scheduled arrival
    would be an hour out for half the year.
    """
    connection = duckdb.connect()
    configure(
        connection=connection, limits=DUCKDB_LIMITS,
        endpoint=_s3_endpoint,
    )
    resolved = connection.execute(
        "SELECT TIMESTAMP '2026-04-05 09:00:00' "
        "AT TIME ZONE 'Australia/Sydney'",
    ).fetchone()
    assert resolved is not None
    assert resolved[0] == datetime(2026, 4, 4, 23, 0, tzinfo=UTC)


def test_partial_hour_from_key_rejects_a_malformed_key() -> None:
    """A key that doesn't match the partial shape names itself."""
    with pytest.raises(ValueError, match='malformed partial object key'):
        partial_hour_from_key(key='curated/_partial/oops.parquet')


def _write_parquet(
    *,
    path: str,
    rows: list[dict[str, object]],
    schema: pa.Schema,
) -> None:
    """Write partial rows to a local Parquet file.

    Parameters
    ----------
    path : str
        Local destination path.
    rows : list[dict[str, object]]
        Rows to write.
    schema : pyarrow.Schema
        Explicit schema, so an all-None column never infers as
        pyarrow's null type.
    """
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


def test_handler_records_a_short_day(
    _bucket: str, _s3_endpoint: str, tmp_path: Path,
) -> None:
    """Partials for only one of many hours still produce a record.

    The merge window spans roughly 32 hours, but this test writes a
    partial for only one of them - the same shape as a compactor gap
    or a partial aged out by the 3-day ``ExpirePartials`` lifecycle.
    The merger must still write output, while recording the shortfall
    rather than reading it as a quiet night.
    """
    service_date = date(2026, 9, 17)
    start, _end = merge_window(service_date=service_date)
    client = boto3.client('s3')
    client.put_object(
        Bucket=_bucket,
        Key='curated/collector_run/dt=2026-09-17/0.jsonl',
        Body=json.dumps({
            'feed': 'vehiclepos',
            'fetched_at_utc': '2026-09-17T02:00:00+00:00',
            'received_at_utc': '2026-09-17T02:00:01+00:00',
            'rtt_s': 0.5,
            'server_date_utc': None,
            'skew_s': None,
            'status_code': 200,
            'body_bytes': 1234,
            'error': None,
        }).encode() + b'\n',
    )
    trip_path = f'{tmp_path}/trip.parquet'
    _write_parquet(
        path=trip_path,
        rows=[{
            'service_date': '20260917',
            'trip_id': '1012281',
            'stop_id': '200013',
            'stop_sequence': 1,
            'route_id': '2447_160',
            'final_predicted_arrival_utc': datetime(
                2026, 9, 17, 0, 0, tzinfo=UTC,
            ),
            'delay_s': 30,
            'final_predicted_departure_utc': None,
            'departure_delay_s': None,
            'last_update_at_utc': datetime(
                2026, 9, 17, 0, 0, tzinfo=UTC,
            ),
            'n_updates': 1,
            'schedule_relationship': 'SCHEDULED',
            'had_vehicle': True,
            'lost_tracking': False,
            'last_observed_at_utc': datetime(
                2026, 9, 17, 0, 0, tzinfo=UTC,
            ),
        }],
        schema=TRIP_STOP_SCHEMA,
    )
    position_path = f'{tmp_path}/position.parquet'
    _write_parquet(
        path=position_path,
        rows=[{
            'observed_at_utc': start,
            'fetched_at_utc': start,
            'position_age_s': 1.0,
            'vehicle_id': '8183',
            'vehicle_label': '8183',
            'trip_id': '1012281',
            'route_id': '2447_160',
            'lat': -33.8,
            'lon': 151.2,
            'bearing': 0.0,
            'speed': 0.0,
            'occupancy_status': None,
            'congestion_level': None,
            'schedule_relationship': None,
            'current_status': None,
            'null_island': False,
        }],
        schema=POSITION_SCHEMA,
    )
    client.upload_file(
        trip_path, _bucket,
        f'curated/_partial/trip_stop/dt={start:%Y-%m-%d}/'
        f'hour={start:%H}/data.parquet',
    )
    client.upload_file(
        position_path, _bucket,
        f'curated/_partial/vehicle_position/dt={start:%Y-%m-%d}/'
        f'hour={start:%H}/data.parquet',
    )
    status_path = f'{tmp_path}/status.parquet'
    _write_parquet(
        path=status_path,
        rows=[{
            'start_date': '20260917',
            'trip_id': '1012290',
            'route_id': '2447_160',
            'start_time': '07:30:00',
            'final_status': 'CANCELED',
            'final_status_at_utc': start,
            'scheduled_polls': 0,
            'canceled_polls': 1,
            'added_polls': 0,
            'first_seen_at_utc': start,
            'last_seen_at_utc': start,
            'first_canceled_at_utc': start,
            'last_canceled_at_utc': start,
            'had_vehicle': False,
        }],
        schema=TRIP_SCHEMA,
    )
    client.upload_file(
        status_path, _bucket,
        f'curated/_partial/trip/dt={start:%Y-%m-%d}/'
        f'hour={start:%H}/data.parquet',
    )
    os.environ['BUCKET_NAME'] = _bucket
    record = handler(
        {'service_date': '2026-09-17'}, _Context(),
        endpoint=_s3_endpoint,
    )
    assert record['job'] == 'merger'
    assert record['rows_out'] == 4
    assert record['error'] is None
    assert record['objects_expected'] > record['objects_read']
    assert record['objects_read'] == 3
    fact = pq.read_table(io.BytesIO(client.get_object(
        Bucket=_bucket,
        Key='curated/fact_trip/service_date=2026-09-17/data.parquet',
    )['Body'].read()))
    assert fact.column('final_status').to_pylist() == ['CANCELED']


def test_merge_trip_stops_resolves_scheduled_arrival(
    _bucket: str, _s3_endpoint: str, tmp_path: Path,
) -> None:
    """scheduled_arrival_utc is resolved for a normal and a >24:00 time.

    The dimension snapshot's ``valid_from`` predates the service date,
    as it must for the lookup to find it.
    """
    service_date = date(2026, 9, 17)
    client = boto3.client('s3')
    dim_path = f'{tmp_path}/dim.parquet'
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    'trip_id': '1012281',
                    'stop_id': '200013',
                    'stop_sequence': 1,
                    'arrival_time': '07:30:00',
                    'departure_time': '07:30:30',
                    'shape_dist_traveled': None,
                },
                {
                    'trip_id': '1012281',
                    'stop_id': '200099',
                    'stop_sequence': 2,
                    'arrival_time': '25:15:00',
                    'departure_time': '25:15:30',
                    'shape_dist_traveled': None,
                },
            ],
            schema=SCHEDULED_STOP_TIME_SCHEMA,
        ),
        dim_path,
    )
    client.upload_file(
        dim_path, _bucket,
        'curated/dim_scheduled_stop_time/valid_from=2026-09-01T020011Z/'
        'data.parquet',
    )
    trip_path = f'{tmp_path}/trip.parquet'
    last_update = datetime(2026, 9, 17, 0, 0, tzinfo=UTC)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    'service_date': '20260917',
                    'trip_id': '1012281',
                    'stop_id': '200013',
                    'stop_sequence': 1,
                    'route_id': '2447_160',
                    'final_predicted_arrival_utc': last_update,
                    'delay_s': 30,
                    'final_predicted_departure_utc': None,
                    'departure_delay_s': None,
                    'last_update_at_utc': last_update,
                    'n_updates': 1,
                    'schedule_relationship': 'SCHEDULED',
                    'had_vehicle': True,
                    'lost_tracking': False,
                    'last_observed_at_utc': last_update,
                },
                {
                    'service_date': '20260917',
                    'trip_id': '1012281',
                    'stop_id': '200099',
                    'stop_sequence': 2,
                    'route_id': '2447_160',
                    'final_predicted_arrival_utc': last_update,
                    'delay_s': 30,
                    'final_predicted_departure_utc': None,
                    'departure_delay_s': None,
                    'last_update_at_utc': last_update,
                    'n_updates': 1,
                    'schedule_relationship': 'SCHEDULED',
                    'had_vehicle': True,
                    'lost_tracking': False,
                    'last_observed_at_utc': last_update,
                },
            ],
            schema=TRIP_STOP_SCHEMA,
        ),
        trip_path,
    )
    client.upload_file(
        trip_path, _bucket,
        'curated/_partial/trip_stop/dt=2026-09-17/hour=00/data.parquet',
    )
    connection = duckdb.connect()
    configure(
        connection=connection, limits=DUCKDB_LIMITS,
        endpoint=_s3_endpoint,
    )
    merge_trip_stops(
        connection=connection,
        bucket=_bucket,
        service_date=service_date,
        session=boto3.Session(),
    )
    body = client.get_object(
        Bucket=_bucket,
        Key='curated/fact_trip_stop/service_date=2026-09-17/data.parquet',
    )['Body'].read()
    table = pq.read_table(io.BytesIO(body))
    by_sequence = dict(zip(
        table.column('stop_sequence').to_pylist(),
        table.column('scheduled_arrival_utc').to_pylist(),
    ))
    assert by_sequence[1] == datetime(2026, 9, 16, 21, 30, tzinfo=UTC)
    assert by_sequence[2] == datetime(2026, 9, 17, 15, 15, tzinfo=UTC)


def put_snapshots(*, bucket: str, valid_from: list[str]) -> None:
    """Store an empty timetable snapshot for each ``valid_from``.

    Parameters
    ----------
    bucket : str
        Destination bucket.
    valid_from : list[str]
        Snapshot dates, ``YYYY-MM-DD``.
    """
    client = boto3.client('s3')
    for value in valid_from:
        client.put_object(
            Bucket=bucket,
            Key=(
                'curated/dim_scheduled_stop_time/'
                f'valid_from={value}/data.parquet'
            ),
            Body=b'',
        )


def test_resolve_dim_source_takes_the_latest_snapshot_in_effect(
    _bucket: str,
) -> None:
    """A service day uses the newest snapshot at or before it."""
    put_snapshots(bucket=_bucket, valid_from=[
        '2026-09-19T173455Z', '2026-09-22T020011Z',
    ])
    assert resolve_dim_source(
        bucket=_bucket, service_date=date(2026, 9, 23),
        session=boto3.Session(),
    ) == (
        f's3://{_bucket}/curated/dim_scheduled_stop_time/'
        'valid_from=2026-09-22T020011Z/data.parquet'
    )


def test_resolve_dim_source_falls_back_to_the_earliest_snapshot(
    _bucket: str,
) -> None:
    """A day before the first snapshot borrows the earliest one."""
    put_snapshots(bucket=_bucket, valid_from=[
        '2026-09-22T020011Z', '2026-09-19T173455Z',
    ])
    assert resolve_dim_source(
        bucket=_bucket, service_date=date(2026, 9, 17),
        session=boto3.Session(),
    ) == (
        f's3://{_bucket}/curated/dim_scheduled_stop_time/'
        'valid_from=2026-09-19T173455Z/data.parquet'
    )


def test_resolve_dim_source_without_snapshots_is_none(_bucket: str) -> None:
    """With no timetable ever captured there is nothing to join."""
    assert resolve_dim_source(
        bucket=_bucket, service_date=date(2026, 9, 17),
        session=boto3.Session(),
    ) is None


def test_partial_paths_lists_only_partitions_inside_the_bounds(
    _bucket: str,
) -> None:
    """Partials outside the dt bounds are never handed to DuckDB."""
    client = boto3.client('s3')
    for day in ('2026-09-15', '2026-09-16', '2026-09-20'):
        client.put_object(
            Bucket=_bucket,
            Key=f'curated/_partial/trip/dt={day}/hour=03/data.parquet',
            Body=b'',
        )
    assert partial_paths(
        client=client, bucket=_bucket, table='trip',
        bounds=('2026-09-16', '2026-09-19'),
    ) == [
        f's3://{_bucket}/curated/_partial/trip/dt=2026-09-16/'
        'hour=03/data.parquet',
    ]


def test_merge_trips_skips_a_day_without_trip_partials(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """A day with no trip-status partials writes nothing and reports 0."""
    service_date = date(2026, 9, 17)
    connection = duckdb.connect()
    configure(
        connection=connection, limits=DUCKDB_LIMITS,
        endpoint=_s3_endpoint,
    )
    assert merge_trips(
        connection=connection, bucket=_bucket, service_date=service_date,
        session=boto3.Session(),
    ) == 0
    listed = boto3.client('s3').list_objects_v2(
        Bucket=_bucket, Prefix='curated/fact_trip/',
    )
    assert 'Contents' not in listed


def test_resolve_dim_source_dates_a_check_in_sydney(_bucket: str) -> None:
    """A check applies from its Sydney date, and the latest on a date wins.

    23:09 UTC on 23 September is 09:09 on the 24th in Sydney. By then
    TfNSW has regenerated the bundle, which can drop trips that ran in
    the small hours of the 23rd, so that check must not serve the 23rd.
    """
    put_snapshots(bucket=_bucket, valid_from=[
        '2026-09-22T020011Z', '2026-09-23T020011Z', '2026-09-23T020511Z',
        '2026-09-23T230911Z',
    ])
    chosen = {
        day: resolve_dim_source(
            bucket=_bucket, service_date=date(2026, 9, day),
            session=boto3.Session(),
        )
        for day in (23, 24)
    }
    prefix = f's3://{_bucket}/curated/dim_scheduled_stop_time/valid_from='
    assert chosen == {
        23: f'{prefix}2026-09-23T020511Z/data.parquet',
        24: f'{prefix}2026-09-23T230911Z/data.parquet',
    }
