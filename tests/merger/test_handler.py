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

from common.service_day import merge_window
from compactor.positions import POSITION_SCHEMA
from compactor.trip_updates import TRIP_STOP_SCHEMA
from merger.handler import (
    configure,
    handler,
    create_s3_secret,
    load_extensions,
    merge_trip_stops,
    partial_hour_from_key,
    partition_bounds,
    target_service_date,
)
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
    monkeypatch.setattr('merger.handler.HTTPFS_EXTENSION', baked)
    monkeypatch.setattr(
        'merger.handler.AWS_EXTENSION',
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
        'merger.handler.HTTPFS_EXTENSION',
        next(tmp_path.glob('*/*/httpfs.duckdb_extension')),
    )
    monkeypatch.setattr(
        'merger.handler.AWS_EXTENSION',
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
    configure(connection=connection, endpoint=_s3_endpoint)
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
            'trip_schedule_relationship': 'SCHEDULED',
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
    os.environ['BUCKET_NAME'] = _bucket
    record = handler(
        {'service_date': '2026-09-17'}, _Context(),
        endpoint=_s3_endpoint,
    )
    assert record['job'] == 'merger'
    assert record['rows_out'] == 3
    assert record['error'] is None
    assert record['objects_expected'] > record['objects_read']
    assert record['objects_read'] == 2


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
        'curated/dim_scheduled_stop_time/valid_from=2026-09-01/'
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
                    'trip_schedule_relationship': 'SCHEDULED',
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
                    'trip_schedule_relationship': 'SCHEDULED',
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
    configure(connection=connection, endpoint=_s3_endpoint)
    merge_trip_stops(
        connection=connection,
        bucket=_bucket,
        service_date=service_date,
        window=merge_window(service_date=service_date),
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
