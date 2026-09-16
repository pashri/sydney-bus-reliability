"""Collector Lambda: poll the bus feeds and store raw payloads.

Invoked every 60 seconds. Vehicle positions are polled six times at
10-second offsets to match the feed's own update cadence; trip
updates once. Because the timeout exceeds the trigger interval,
invocations overlap by design — object keys carry the actual fetch
second, so overlap is harmless.

Every poll gets its own thread and sleeps until its own absolute
offset from invocation start; only the network call itself is gated
by a semaphore, so a slow or late poll never delays another poll's
start — it can only ever delay itself.
"""

import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from typing import Any, Final

import requests
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
from botocore.exceptions import ClientError

from src.common.feeds import fetch_feed
from src.common.storage import RawFeedRepository
from src.common.types_ import CollectionCounts, Feed, FetchResult

logger = Logger()

TRIP_OFFSET_S: Final[float] = 0.0
MAX_CONCURRENT_POLLS: Final[int] = 2
MAX_OFFSET_S: Final[float] = 55.0


def validate_offset(*, offset: float) -> None:
    """Reject an offset that cannot fit inside one invocation.

    Parameters
    ----------
    offset
        Seconds after invocation start.

    Raises
    ------
    ValueError
        If ``offset`` is negative or at/beyond ``MAX_OFFSET_S``. The
        Lambda timeout is 65s; an offset at or past that leaves no
        room to fetch and store, which would otherwise wedge the
        invocation until Lambda kills it, silently, with no raw
        objects and no run record.
    """
    if offset < 0:
        raise ValueError(f'Offset {offset} must not be negative')
    if offset >= MAX_OFFSET_S:
        raise ValueError(
            f'Offset {offset} must be under {MAX_OFFSET_S}s',
        )


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
        If the value is not a comma-separated list of numbers, or
        any offset is out of range. See ``validate_offset``.
    """
    raw = os.environ['VEHICLE_OFFSETS_S']
    offsets = tuple(float(part) for part in raw.split(','))
    for offset in offsets:
        validate_offset(offset=offset)
    return offsets


def read_api_key() -> str:
    """Read the TfNSW API key, preferring the environment in tests.

    Returns
    -------
    str
        The API key.
    """
    from_env = os.getenv('TFNSW_API_KEY')
    if from_env is not None:
        return from_env
    return parameters.get_parameter(
        os.environ['API_KEY_PARAMETER_NAME'], decrypt=True, max_age=3600,
    )


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


@dataclass(frozen=True, slots=True)
class PollContext:
    """Resources shared by every poll in one invocation.

    Bundled together so the per-poll functions below take one
    logical argument for "how to fetch" instead of three positional
    ones, keeping their signatures short.
    """

    api_key: str
    session: requests.Session
    semaphore: threading.Semaphore


def poll_at(
    *,
    offset_s: float,
    feed: Feed,
    started_at: float,
    context: PollContext,
) -> FetchResult:
    """Wait until an absolute offset, then fetch once.

    The offset is measured from invocation start, never from the
    end of a prior poll, so a slow poll delays only itself and never
    pushes back the start of the next scheduled poll. Each poll runs
    on its own thread; the semaphore bounds only the concurrent
    network calls, not the waiting, so it never reintroduces
    chaining between polls.

    Parameters
    ----------
    offset_s
        Seconds after invocation start at which to issue the
        request.
    feed
        The feed to fetch.
    started_at
        ``time.monotonic()`` captured at invocation start.
    context
        Shared API key, session and rate-limiting semaphore.

    Returns
    -------
    FetchResult
        Never raises; failures are recorded in the result.
    """
    remaining = offset_s - (time.monotonic() - started_at)
    if remaining > 0:
        time.sleep(remaining)
    with context.semaphore:
        return fetch_feed(
            feed=feed,
            api_key=context.api_key,
            session=context.session,
        )


def submit_polls(
    *,
    schedule: list[tuple[float, Feed]],
    started_at: float,
    context: PollContext,
    pool: ThreadPoolExecutor,
) -> list[Future[FetchResult]]:
    """Submit every poll in the schedule to the pool at once.

    Submitting all of them up front, rather than one at a time, is
    what lets each poll's wait run concurrently with the others
    instead of queueing behind them.

    Parameters
    ----------
    schedule
        Offsets in seconds and the feed to poll at each.
    started_at
        ``time.monotonic()`` captured at invocation start.
    context
        Shared API key, session and rate-limiting semaphore.
    pool
        Executor with one worker per scheduled poll.

    Returns
    -------
    list[Future[FetchResult]]
        One future per scheduled poll, in schedule order.
    """
    return [
        pool.submit(
            poll_at,
            offset_s=offset,
            feed=feed,
            started_at=started_at,
            context=context,
        )
        for offset, feed in schedule
    ]


def run_schedule(
    *,
    schedule: list[tuple[float, Feed]],
    context: PollContext,
) -> list[FetchResult]:
    """Fire every poll in the schedule at its own absolute offset.

    One thread per poll, so a poll waiting on its offset (or blocked
    on the semaphore behind an in-flight request) never occupies a
    slot that another poll needs in order to start waiting on its
    own offset.

    Parameters
    ----------
    schedule
        Offsets in seconds and the feed to poll at each.
    context
        Shared API key, session and rate-limiting semaphore.

    Returns
    -------
    list[FetchResult]
        One result per scheduled poll, in schedule order.
    """
    started_at = time.monotonic()
    with ThreadPoolExecutor(max_workers=len(schedule)) as pool:
        futures = submit_polls(
            schedule=schedule,
            started_at=started_at,
            context=context,
            pool=pool,
        )
        return [future.result() for future in futures]


def store_one(
    *, result: FetchResult, repository: RawFeedRepository,
) -> tuple[str | None, bool]:
    """Persist one payload, tolerating a storage failure.

    Parameters
    ----------
    result
        The fetch to store.
    repository
        Destination repository.

    Returns
    -------
    tuple[str | None, bool]
        The key written (or None if the body was empty), and whether
        storing it failed. A storage failure is logged and reported,
        never raised, so one poll's S3 error cannot abort storage of
        the other polls or the run record.
    """
    try:
        return repository.put_raw(result=result), False
    except ClientError:
        logger.exception(
            'Failed to store raw payload',
            extra={'feed': result.feed.value},
        )
        return None, True


def audit_record(
    *, result: FetchResult, store_failed: bool,
) -> FetchResult:
    """Fold a storage failure into one poll's audit record.

    Parameters
    ----------
    result
        The original fetch outcome.
    store_failed
        Whether storing this result's payload raised.

    Returns
    -------
    FetchResult
        Unchanged when the fetch itself already failed or storage
        succeeded; otherwise a copy carrying a storage-failure
        error, so the audit trail distinguishes "fetched but could
        not store" from "fetch failed".
    """
    if not store_failed or result.error is not None:
        return result
    return replace(result, error='storage failed')


def store_all(
    *,
    results: list[FetchResult],
    repository: RawFeedRepository,
    invocation_id: str,
) -> CollectionCounts:
    """Persist every payload and the invocation's audit record.

    The audit record is written even when every poll failed to
    fetch or to store, because it is what answers "was the gap
    TfNSW or me?" when a week of data looks empty.

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
        Counts of fetches attempted, payloads stored and fetches or
        stores failed.
    """
    if not results:
        return CollectionCounts(fetched=0, stored=0, failed=0)
    stored_keys: list[str] = []
    audited: list[FetchResult] = []
    for result in results:
        key, store_failed = store_one(
            result=result, repository=repository,
        )
        if key is not None:
            stored_keys.append(key)
        audited.append(
            audit_record(result=result, store_failed=store_failed),
        )
    repository.put_run_record(
        results=audited, invocation_id=invocation_id,
    )
    return CollectionCounts(
        fetched=len(results),
        stored=len(stored_keys),
        failed=sum(1 for r in audited if r.error is not None),
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
    tests can supply a fast one without patching module state.

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
        Counts of fetches attempted, payloads stored and fetches or
        stores failed.
    """
    context = PollContext(
        api_key=api_key,
        session=requests.Session(),
        semaphore=threading.Semaphore(MAX_CONCURRENT_POLLS),
    )
    results = run_schedule(schedule=schedule, context=context)
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
        Counts of fetches attempted, payloads stored and fetches or
        stores failed.
    """
    counts = collect(
        schedule=poll_schedule(),
        api_key=read_api_key(),
        repository=RawFeedRepository(bucket=os.environ['BUCKET_NAME']),
        invocation_id=context.aws_request_id,
    )
    logger.info('Collection complete', extra=counts)
    return counts
