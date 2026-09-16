"""Tests for S3 storage of raw feed payloads."""

import gzip
import json
from datetime import UTC, datetime
from http import HTTPStatus

import boto3

from src.common.storage import RawFeedRepository, raw_key, run_key
from src.common.types_ import Feed, FetchResult

REGION = 'ap-southeast-2'
FETCHED_AT = datetime(2026, 9, 15, 14, 23, 7, tzinfo=UTC)
RECEIVED_AT = datetime(2026, 9, 15, 14, 23, 7, 200000, tzinfo=UTC)
SERVER_DATE = datetime(2026, 9, 15, 14, 23, 7, 100000, tzinfo=UTC)


def _ok_result(*, body: bytes = b'payload') -> FetchResult:
    return FetchResult(
        feed=Feed.VEHICLE_POSITIONS,
        fetched_at_utc=FETCHED_AT,
        received_at_utc=RECEIVED_AT,
        server_date_utc=SERVER_DATE,
        status_code=HTTPStatus.OK,
        body=body,
        error=None,
    )


def test_raw_key_uses_actual_fetch_time() -> None:
    key = raw_key(feed=Feed.VEHICLE_POSITIONS, fetched_at=FETCHED_AT)
    assert key == (
        'raw/vehiclepos/dt=2026-09-15/hour=14/142307.pb.gz'
    )


def test_run_key_includes_invocation_id() -> None:
    key = run_key(fetched_at=FETCHED_AT, invocation_id='abc-123')
    assert key == (
        'curated/collector_run/dt=2026-09-15/abc-123.jsonl'
    )


def test_put_raw_writes_gzipped_body(_bucket: str) -> None:
    repo = RawFeedRepository(bucket=_bucket)
    key = repo.put_raw(result=_ok_result())
    assert key is not None
    client = boto3.client('s3', region_name=REGION)
    stored = client.get_object(Bucket=_bucket, Key=key)['Body'].read()
    assert gzip.decompress(stored) == b'payload'


def test_put_raw_skips_empty_body(_bucket: str) -> None:
    failed = FetchResult(
        feed=Feed.VEHICLE_POSITIONS,
        fetched_at_utc=FETCHED_AT,
        received_at_utc=RECEIVED_AT,
        server_date_utc=None,
        status_code=HTTPStatus.FORBIDDEN,
        body=b'',
        error='HTTP 403',
    )
    repo = RawFeedRepository(bucket=_bucket)
    assert repo.put_raw(result=failed) is None
    client = boto3.client('s3', region_name=REGION)
    listing = client.list_objects_v2(Bucket=_bucket, Prefix='raw/')
    assert listing.get('KeyCount') == 0


def test_put_run_record_writes_one_line_per_result(
    _bucket: str,
) -> None:
    repo = RawFeedRepository(bucket=_bucket)
    key = repo.put_run_record(
        results=[_ok_result(), _ok_result(body=b'second')],
        invocation_id='abc-123',
    )
    client = boto3.client('s3', region_name=REGION)
    body = client.get_object(Bucket=_bucket, Key=key)['Body'].read()
    lines = body.decode().strip().split('\n')
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first['feed'] == 'vehiclepos'
    assert first['status_code'] == HTTPStatus.OK
    assert first['rtt_s'] == 0.2
    assert first['skew_s'] == 0.0
    assert first['body_bytes'] == 7


def test_put_run_record_handles_missing_server_date(
    _bucket: str,
) -> None:
    result = FetchResult(
        feed=Feed.TRIP_UPDATES,
        fetched_at_utc=FETCHED_AT,
        received_at_utc=RECEIVED_AT,
        server_date_utc=None,
        status_code=None,
        body=b'',
        error='timeout',
    )
    repo = RawFeedRepository(bucket=_bucket)
    key = repo.put_run_record(results=[result], invocation_id='x')
    client = boto3.client('s3', region_name=REGION)
    body = client.get_object(Bucket=_bucket, Key=key)['Body'].read()
    record = json.loads(body.decode().strip())
    assert record['skew_s'] is None
    assert record['rtt_s'] == 0.2
    assert record['error'] == 'timeout'
