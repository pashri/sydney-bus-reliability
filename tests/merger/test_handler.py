"""Tests for the daily merger handler."""

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.common.service_day import merge_window
from src.compactor.positions import POSITION_SCHEMA
from src.compactor.trip_updates import TRIP_STOP_SCHEMA
from src.merger.handler import (
    handler,
    partial_hour_from_key,
    target_service_date,
)


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
    from src.merger.handler import merge_collector_run

    assert merge_collector_run(
        bucket=_bucket, service_date=date(2026, 9, 17),
        endpoint=_s3_endpoint,
    ) == 3


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
