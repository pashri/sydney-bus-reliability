"""Collector Lambda: poll the bus feeds and store raw payloads.

Invoked every 60 seconds. Vehicle positions are polled six times at
10-second offsets to match the feed's own update cadence; trip
updates once. Because the timeout exceeds the trigger interval,
invocations overlap by design — object keys carry the actual fetch
second, so overlap is harmless.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Final

import requests
from aws_lambda_powertools import Logger

from src.common.feeds import fetch_feed
from src.common.storage import RawFeedRepository
from src.common.types_ import CollectionCounts, Feed, FetchResult

logger = Logger()

TRIP_OFFSET_S: Final[float] = 0
MAX_CONCURRENT_POLLS: Final[int] = 2


def read_offsets() -> tuple[float, ...]:
    """Read the vehicle-position poll offsets from the environment.

    Returns
    -------
    tuple[float, ...]
        Seconds after invocation start at which to poll.

    Raises
    ------
    KeyError
        If ``VEHICLE_OFFSETS_S`` is not set. The cadence lives in
        ``template.yaml`` and has no in-code default, so that there
        is exactly one place to change it.
    ValueError
        If the value is not a comma-separated list of numbers.
    """
    raw = os.environ['VEHICLE_OFFSETS_S']
    return tuple(float(part) for part in raw.split(','))


def poll_schedule() -> list[tuple[float, Feed]]:
    """List every poll in one invocation as an offset and a feed.

    Returns
    -------
    list[tuple[float, Feed]]
        Offsets in seconds from invocation start, vehicle polls
        first.
    """
    vehicle = [
        (offset, Feed.VEHICLE_POSITIONS) for offset in read_offsets()
    ]
    return [*vehicle, (TRIP_OFFSET_S, Feed.TRIP_UPDATES)]


def poll_at(
    *,
    offset_s: float,
    feed: Feed,
    started_at: float,
    api_key: str,
    session: requests.Session,
) -> FetchResult:
    """Wait until an absolute offset, then fetch once.

    The offset is measured from invocation start, never from the
    end of a prior poll, so a slow poll delays only itself and never
    pushes back the start of the next scheduled poll.

    Parameters
    ----------
    offset_s
        Seconds after invocation start at which to issue the
        request.
    feed
        The feed to fetch.
    started_at
        ``time.monotonic()`` captured at invocation start.
    api_key
        TfNSW Open Data Hub API key.
    session
        Shared session, for connection reuse.

    Returns
    -------
    FetchResult
        Never raises; failures are recorded in the result.
    """
    remaining = offset_s - (time.monotonic() - started_at)
    if remaining > 0:
        time.sleep(remaining)
    return fetch_feed(feed=feed, api_key=api_key, session=session)


def store_all(
    *,
    results: list[FetchResult],
    repository: RawFeedRepository,
    invocation_id: str,
) -> CollectionCounts:
    """Persist every payload and the invocation's audit record.

    Parameters
    ----------
    results
        Every fetch attempted.
    repository
        Destination repository.
    invocation_id
        Lambda request id.

    Returns
    -------
    CollectionCounts
        Counts of fetches attempted, payloads stored and fetches
        failed.
    """
    stored = [
        key
        for key in (
            repository.put_raw(result=result) for result in results
        )
        if key is not None
    ]
    repository.put_run_record(
        results=results, invocation_id=invocation_id,
    )
    return CollectionCounts(
        fetched=len(results),
        stored=len(stored),
        failed=sum(1 for r in results if r.error is not None),
    )


def collect(
    *,
    schedule: list[tuple[float, Feed]],
    api_key: str,
    repository: RawFeedRepository,
    invocation_id: str,
) -> CollectionCounts:
    """Run one full collection round against a given schedule.

    The schedule is a parameter rather than a module constant so
    tests can supply a fast one without patching module state. Every
    poll is submitted up front at its absolute offset; the pool is
    capped well under TfNSW's rate limit, and one poll's failure
    (``fetch_feed`` never raises) cannot abort the others because
    each future is awaited independently.

    Parameters
    ----------
    schedule
        Offsets in seconds and the feed to poll at each.
    api_key
        TfNSW Open Data Hub API key.
    repository
        Destination for payloads and the audit record.
    invocation_id
        Lambda request id.

    Returns
    -------
    CollectionCounts
        Counts of fetches attempted, payloads stored and fetches
        failed.
    """
    started_at = time.monotonic()
    session = requests.Session()
    workers = min(MAX_CONCURRENT_POLLS, len(schedule))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                poll_at,
                offset_s=offset,
                feed=feed,
                started_at=started_at,
                api_key=api_key,
                session=session,
            )
            for offset, feed in schedule
        ]
        results = [future.result() for future in futures]
    return store_all(
        results=results,
        repository=repository,
        invocation_id=invocation_id,
    )


@logger.inject_lambda_context
def handler(
    event: dict[str, Any],  # pylint: disable=unused-argument
    context: Any,
) -> CollectionCounts:
    """Poll every feed on schedule and store what comes back.

    Parameters
    ----------
    event
        EventBridge event; unused.
    context
        Lambda context, read for ``aws_request_id``.

    Returns
    -------
    CollectionCounts
        Counts of fetches attempted, payloads stored and fetches
        failed.
    """
    counts = collect(
        schedule=poll_schedule(),
        api_key=os.environ['TFNSW_API_KEY'],
        repository=RawFeedRepository(bucket=os.environ['BUCKET_NAME']),
        invocation_id=context.aws_request_id,
    )
    logger.info('Collection complete', extra=counts)
    return counts
