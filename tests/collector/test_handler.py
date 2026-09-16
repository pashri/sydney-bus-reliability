"""Tests for the collector Lambda handler."""

import threading
import time

import boto3
import pytest
import responses
from botocore.exceptions import ClientError

from src.collector.handler import (
    collect,
    handler,
    poll_schedule,
    read_offsets,
    store_all,
)
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
# Seven polls (six vehicle, one trip) against a pool of only two
# workers: the shape that exposes chaining through the pool queue.
# Trip updates is submitted LAST at offset 0.0 — the exact case that
# fired ~40s late in production.
TIMING_SCHEDULE: list[tuple[float, Feed]] = [
    (0.0, Feed.VEHICLE_POSITIONS),
    (0.3, Feed.VEHICLE_POSITIONS),
    (0.6, Feed.VEHICLE_POSITIONS),
    (0.9, Feed.VEHICLE_POSITIONS),
    (1.2, Feed.VEHICLE_POSITIONS),
    (1.5, Feed.VEHICLE_POSITIONS),
    (0.0, Feed.TRIP_UPDATES),
]
SIMULATED_ROUND_TRIP_S = 0.2


class _FlakyRepository(RawFeedRepository):
    """A repository whose first ``put_raw`` call always raises."""

    def __init__(self, *, bucket: str) -> None:
        super().__init__(bucket=bucket)
        self.raise_next = True

    def put_raw(self, *, result):
        if self.raise_next:
            self.raise_next = False
            raise ClientError(
                {'Error': {'Code': 'InternalError', 'Message': 'x'}},
                'PutObject',
            )
        return super().put_raw(result=result)


class _AlwaysFailingRepository(RawFeedRepository):
    """A repository whose ``put_raw`` always raises."""

    def put_raw(self, *, result):
        raise ClientError(
            {'Error': {'Code': 'InternalError', 'Message': 'x'}},
            'PutObject',
        )


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


def test_read_offsets_rejects_negative(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('VEHICLE_OFFSETS_S', '0,-5')
    with pytest.raises(ValueError):
        read_offsets()


def test_read_offsets_rejects_offset_at_the_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('VEHICLE_OFFSETS_S', '0,600')
    with pytest.raises(ValueError):
        read_offsets()


@responses.activate
def test_polls_fire_near_their_absolute_offset(_bucket: str) -> None:
    """A poll's fire time must track its own offset, not a queue.

    Trip updates is scheduled for offset 0.0 but submitted last.
    Chaining through a starved pool would fire it seconds late; an
    absolute schedule fires it near t+0 regardless of submission
    order.
    """
    started_at = time.monotonic()
    fire_times: dict[Feed, list[float]] = {
        Feed.VEHICLE_POSITIONS: [],
        Feed.TRIP_UPDATES: [],
    }

    def _record(feed: Feed, body: bytes):
        def callback(_request):
            fire_times[feed].append(time.monotonic() - started_at)
            time.sleep(SIMULATED_ROUND_TRIP_S)
            return (200, {}, body)

        return callback

    responses.add_callback(
        responses.GET,
        VEHICLE_URL,
        callback=_record(Feed.VEHICLE_POSITIONS, b'vp'),
    )
    responses.add_callback(
        responses.GET,
        TRIP_URL,
        callback=_record(Feed.TRIP_UPDATES, b'tu'),
    )

    collect(
        schedule=TIMING_SCHEDULE,
        api_key='k',
        repository=RawFeedRepository(bucket=_bucket),
        invocation_id='req-1',
    )

    trip_fired_at = fire_times[Feed.TRIP_UPDATES][0]
    # Generous tolerance: this catches a multi-second chaining bug,
    # not millisecond jitter on a loaded CI runner.
    assert trip_fired_at < 1.0


@responses.activate
def test_at_most_two_requests_in_flight(_bucket: str) -> None:
    """No more than MAX_CONCURRENT_POLLS requests run at once."""
    lock = threading.Lock()
    state = {'in_flight': 0, 'max_in_flight': 0}

    def callback(_request):
        with lock:
            state['in_flight'] += 1
            state['max_in_flight'] = max(
                state['max_in_flight'], state['in_flight'],
            )
        time.sleep(SIMULATED_ROUND_TRIP_S)
        with lock:
            state['in_flight'] -= 1
        return (200, {}, b'ok')

    responses.add_callback(
        responses.GET, VEHICLE_URL, callback=callback,
    )
    responses.add_callback(
        responses.GET, TRIP_URL, callback=callback,
    )

    collect(
        schedule=TIMING_SCHEDULE,
        api_key='k',
        repository=RawFeedRepository(bucket=_bucket),
        invocation_id='req-1',
    )

    assert state['max_in_flight'] <= 2


@responses.activate
def test_store_all_survives_one_storage_failure(_bucket: str) -> None:
    responses.add(responses.GET, VEHICLE_URL, body=b'vp', status=200)
    responses.add(responses.GET, TRIP_URL, body=b'tu', status=200)

    result = collect(
        schedule=FAST_SCHEDULE,
        api_key='k',
        repository=_FlakyRepository(bucket=_bucket),
        invocation_id='req-1',
    )

    assert result['fetched'] == 3
    assert result['stored'] == 2
    assert result['failed'] == 1
    client = boto3.client('s3', region_name=REGION)
    listing = client.list_objects_v2(
        Bucket=_bucket, Prefix='curated/collector_run/',
    )
    assert listing['KeyCount'] == 1


@responses.activate
def test_store_all_writes_run_record_when_every_store_fails(
    _bucket: str,
) -> None:
    responses.add(responses.GET, VEHICLE_URL, body=b'vp', status=200)
    responses.add(responses.GET, TRIP_URL, body=b'tu', status=200)

    result = collect(
        schedule=FAST_SCHEDULE,
        api_key='k',
        repository=_AlwaysFailingRepository(bucket=_bucket),
        invocation_id='req-1',
    )

    assert result == {'fetched': 3, 'stored': 0, 'failed': 3}
    client = boto3.client('s3', region_name=REGION)
    listing = client.list_objects_v2(
        Bucket=_bucket, Prefix='curated/collector_run/',
    )
    assert listing['KeyCount'] == 1


def test_store_all_handles_empty_results(_bucket: str) -> None:
    result = store_all(
        results=[],
        repository=RawFeedRepository(bucket=_bucket),
        invocation_id='req-1',
    )
    assert result == {'fetched': 0, 'stored': 0, 'failed': 0}
