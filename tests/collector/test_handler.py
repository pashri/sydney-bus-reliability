"""Tests for the collector Lambda handler."""

import boto3
import pytest
import responses

from src.collector.handler import collect, handler, poll_schedule, read_offsets
from src.common.storage import RawFeedRepository
from src.common.types_ import Feed

REGION = 'ap-southeast-2'
VEHICLE_URL = (
    'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses'
)
TRIP_URL = 'https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses'
FAST_SCHEDULE: list[tuple[float, Feed]] = [
    (0.0, Feed.VEHICLE_POSITIONS),
    (1.1, Feed.VEHICLE_POSITIONS),
    (0.0, Feed.TRIP_UPDATES),
]


class _Context:
    """Minimal stand-in for the Lambda context object."""

    aws_request_id = 'req-1'
    function_name = 'collector'
    memory_limit_in_mb = 256
    invoked_function_arn = (
        'arn:aws:lambda:ap-southeast-2:000000000000:'
        'function:collector'
    )


def test_poll_schedule_covers_every_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('VEHICLE_OFFSETS_S', '0,10,20,30,40,50')
    schedule = poll_schedule()
    vehicle = [f for _, f in schedule if f is Feed.VEHICLE_POSITIONS]
    trips = [f for _, f in schedule if f is Feed.TRIP_UPDATES]
    assert len(vehicle) == 6
    assert len(trips) == 1
    assert [o for o, _ in schedule][:6] == [0, 10, 20, 30, 40, 50]


def test_read_offsets_requires_the_environment_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('VEHICLE_OFFSETS_S', raising=False)
    with pytest.raises(KeyError):
        read_offsets()


def test_read_offsets_parses_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('VEHICLE_OFFSETS_S', '0,0.5,1')
    assert read_offsets() == (0.0, 0.5, 1.0)


def test_read_offsets_rejects_nonsense(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('VEHICLE_OFFSETS_S', '0,soon')
    with pytest.raises(ValueError):
        read_offsets()


@responses.activate
def test_collect_stores_every_successful_poll(_bucket: str) -> None:
    responses.add(responses.GET, VEHICLE_URL, body=b'vp', status=200)
    responses.add(responses.GET, TRIP_URL, body=b'tu', status=200)

    result = collect(
        schedule=FAST_SCHEDULE,
        api_key='k',
        repository=RawFeedRepository(bucket=_bucket),
        invocation_id='req-1',
    )

    assert result == {'fetched': 3, 'stored': 3, 'failed': 0}


@responses.activate
def test_collect_survives_one_failing_feed(_bucket: str) -> None:
    responses.add(responses.GET, VEHICLE_URL, body=b'vp', status=200)
    responses.add(responses.GET, TRIP_URL, body=b'', status=500)

    result = collect(
        schedule=FAST_SCHEDULE,
        api_key='k',
        repository=RawFeedRepository(bucket=_bucket),
        invocation_id='req-1',
    )

    assert result['failed'] == 1
    assert result['stored'] == 2


@responses.activate
def test_collect_stores_raw_objects(_bucket: str) -> None:
    responses.add(responses.GET, VEHICLE_URL, body=b'vp', status=200)
    responses.add(responses.GET, TRIP_URL, body=b'tu', status=200)

    collect(
        schedule=FAST_SCHEDULE,
        api_key='k',
        repository=RawFeedRepository(bucket=_bucket),
        invocation_id='req-1',
    )

    client = boto3.client('s3', region_name=REGION)
    listing = client.list_objects_v2(Bucket=_bucket, Prefix='raw/')
    assert listing['KeyCount'] == 3


@responses.activate
def test_collect_always_writes_a_run_record(_bucket: str) -> None:
    responses.add(responses.GET, VEHICLE_URL, body=b'', status=403)
    responses.add(responses.GET, TRIP_URL, body=b'', status=403)

    collect(
        schedule=FAST_SCHEDULE,
        api_key='k',
        repository=RawFeedRepository(bucket=_bucket),
        invocation_id='req-1',
    )

    client = boto3.client('s3', region_name=REGION)
    listing = client.list_objects_v2(
        Bucket=_bucket, Prefix='curated/collector_run/',
    )
    assert listing['KeyCount'] == 1


@responses.activate
def test_handler_collects_and_stores_end_to_end(
    _bucket: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('BUCKET_NAME', _bucket)
    monkeypatch.setenv('TFNSW_API_KEY', 'k')
    monkeypatch.setenv('VEHICLE_OFFSETS_S', '0,1.1')
    responses.add(responses.GET, VEHICLE_URL, body=b'vp', status=200)
    responses.add(responses.GET, TRIP_URL, body=b'tu', status=200)

    result = handler({}, _Context())

    assert result == {'fetched': 3, 'stored': 3, 'failed': 0}
    client = boto3.client('s3', region_name=REGION)
    listing = client.list_objects_v2(Bucket=_bucket, Prefix='raw/')
    assert listing['KeyCount'] == 3


def test_handler_requires_bucket_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('TFNSW_API_KEY', 'k')
    monkeypatch.setenv('VEHICLE_OFFSETS_S', '0')
    monkeypatch.delenv('BUCKET_NAME', raising=False)

    with pytest.raises(KeyError):
        handler({}, _Context())
