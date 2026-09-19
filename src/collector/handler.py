"""Collector Lambda: poll the bus feeds and store raw payloads.

Invoked every 60 seconds. Vehicle positions are polled six times at
10-second offsets; trip updates once. The Lambda timeout exceeds the
trigger interval, so invocations overlap. Object keys carry the
actual fetch second, so an overlap cannot overwrite anything.

Every poll runs on its own thread and sleeps until its own absolute
offset from invocation start. A semaphore gates only the network
call, not the waiting, so a slow poll delays only itself.

Each poll stores its own payload inside its own worker, as soon as
the fetch returns and after the semaphore has been released. The
worker hands back only a ``RunRecord``: counts and timestamps, never
bytes, so payloads are not held alive until the end of the run.
"""

import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Final

import requests
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
from botocore.exceptions import BotoCoreError, ClientError

from common.feeds import fetch_feed
from common.storage import RawFeedRepository, run_record
from common.types_ import (
    CRASHED_POLL_ERROR,
    CollectionCounts,
    Feed,
    FetchResult,
    RunRecord,
)

logger = Logger()

TRIP_OFFSET_S: Final[float] = 0.0  # seconds
MAX_CONCURRENT_POLLS: Final[int] = 2  # polls
MAX_OFFSET_S: Final[float] = 55.0  # seconds

_repository_cache: dict[str, RawFeedRepository] = {}


def validate_offset(*, offset: float) -> None:
    """Reject an offset that cannot fit inside one invocation.

    Parameters
    ----------
    offset : float
        Seconds after invocation start.

    Raises
    ------
    ValueError
        If ``offset`` is negative or at/beyond ``MAX_OFFSET_S``. The
        Lambda timeout is 65s, so an offset at or past that leaves no
        room to fetch and store: the invocation runs until Lambda
        kills it, with no raw objects and no run record.
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
        If ``VEHICLE_OFFSETS_S`` is not set. It is set in
        ``template.yaml`` and has no in-code default.
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


def get_repository() -> RawFeedRepository:
    """Build or reuse this execution environment's S3 repository.

    ``BUCKET_NAME`` is read on every call, not only on a cache miss,
    so a missing variable still raises rather than being masked by a
    cached entry. The boto3 client is created at most once per bucket
    per execution environment.

    Returns
    -------
    RawFeedRepository
        The cached repository for the current ``BUCKET_NAME``.

    Raises
    ------
    KeyError
        If ``BUCKET_NAME`` is not set.
    """
    bucket = os.environ['BUCKET_NAME']
    if bucket not in _repository_cache:
        _repository_cache[bucket] = RawFeedRepository(bucket=bucket)
    return _repository_cache[bucket]


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
    """Resources shared by every poll in one invocation."""

    api_key: str
    session: requests.Session
    semaphore: threading.Semaphore
    repository: RawFeedRepository


@dataclass(frozen=True, slots=True)
class PollOutcome:
    """What one poll hands back once its payload is stored.

    Holds no ``bytes``. The payload's length is on
    ``RunRecord.body_bytes``; carrying the body here would keep every
    payload alive inside the futures until the end of the run.
    """

    record: RunRecord
    stored: bool


def store_one(
    *, result: FetchResult, repository: RawFeedRepository,
) -> tuple[str | None, bool]:
    """Persist one payload, tolerating a storage failure.

    Parameters
    ----------
    result : FetchResult
        The fetch to store.
    repository : RawFeedRepository
        Destination repository.

    Returns
    -------
    tuple[str | None, bool]
        The key written (or None if the body was empty), and whether
        storing it failed. A storage failure is logged and reported,
        never raised, so one poll's S3 error cannot abort storage of
        the other polls or the run record. ``BotoCoreError`` covers
        transient network faults (endpoint, connect and read
        timeouts, closed connections) and ``ClientError`` covers S3
        rejecting the request; both are contained the same way.
    """
    try:
        return repository.put_raw(result=result), False
    except (ClientError, BotoCoreError):
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
    result : FetchResult
        The original fetch outcome.
    store_failed : bool
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


def poll_at(
    *,
    offset_s: float,
    feed: Feed,
    started_at: float,
    context: PollContext,
) -> FetchResult:
    """Wait until an absolute offset, then fetch once.

    The offset is measured from invocation start, not from the end
    of a prior poll, so a slow poll delays only itself. The semaphore
    bounds the concurrent network calls, not the waiting.

    Parameters
    ----------
    offset_s : float
        Seconds after invocation start at which to issue the
        request.
    feed : Feed
        The feed to fetch.
    started_at : float
        ``time.monotonic()`` captured at invocation start.
    context : PollContext
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


def poll_and_store(
    *,
    offset_s: float,
    feed: Feed,
    started_at: float,
    context: PollContext,
) -> PollOutcome:
    """Fetch one feed at its offset and store it straight away.

    ``result`` dies with this frame, so the body is collectable as
    soon as the store returns, and the S3 writes spread across the
    invocation instead of clustering after the last poll.

    The store runs outside ``poll_at``'s semaphore, which ``poll_at``
    has already released by the time it returns. Holding the
    semaphore across an S3 write queues later polls behind a slow
    upload; that once made trip updates fire about 40s late.

    Parameters
    ----------
    offset_s : float
        Seconds after invocation start at which to issue the
        request.
    feed : Feed
        The feed to fetch.
    started_at : float
        ``time.monotonic()`` captured at invocation start.
    context : PollContext
        Shared API key, session, semaphore and repository.

    Returns
    -------
    PollOutcome
        Metadata only. Neither a fetch failure nor a store failure
        raises, so one poll can never lose its siblings.
    """
    result = poll_at(
        offset_s=offset_s,
        feed=feed,
        started_at=started_at,
        context=context,
    )
    key, store_failed = store_one(
        result=result, repository=context.repository,
    )
    audited = audit_record(result=result, store_failed=store_failed)
    return PollOutcome(
        record=run_record(result=audited), stored=key is not None,
    )


def submit_polls(
    *,
    schedule: list[tuple[float, Feed]],
    started_at: float,
    context: PollContext,
    pool: ThreadPoolExecutor,
) -> list[Future[PollOutcome]]:
    """Submit every poll in the schedule to the pool at once.

    All polls are submitted up front, so each poll's wait runs
    concurrently with the others instead of queueing behind them.

    Parameters
    ----------
    schedule : list[tuple[float, Feed]]
        Offsets in seconds and the feed to poll at each.
    started_at : float
        ``time.monotonic()`` captured at invocation start.
    context : PollContext
        Shared API key, session, semaphore and repository.
    pool : ThreadPoolExecutor
        Executor with one worker per scheduled poll.

    Returns
    -------
    list[Future[PollOutcome]]
        One future per scheduled poll, in schedule order. The
        futures hold metadata only, so a completed poll's payload is
        not kept alive by the future that produced it.
    """
    return [
        pool.submit(
            poll_and_store,
            offset_s=offset,
            feed=feed,
            started_at=started_at,
            context=context,
        )
        for offset, feed in schedule
    ]


class ScheduleError(Exception):
    """A poll worker raised something ``store_one`` did not expect.

    Carries one outcome per scheduled poll, including the crashed
    ones, so the caller can still write a full run record before
    letting the original error surface.

    Parameters
    ----------
    outcomes : list[PollOutcome]
        One outcome per scheduled poll: real for a survivor, a
        crash placeholder for one that raised.
    cause : BaseException
        The exception a worker raised.
    """

    def __init__(
        self, *, outcomes: list[PollOutcome], cause: BaseException,
    ) -> None:
        super().__init__(str(cause))
        self.outcomes = outcomes
        self.cause = cause


def _crash_outcome(*, feed: Feed) -> PollOutcome:
    """Build a placeholder outcome for a poll that crashed.

    The placeholder is attributed to its own feed, so the audit trail
    says which scheduled poll died. No typed field tells a crash
    apart from a transport failure: ``fetch_feed``'s transport-failure
    path also gives ``status_code=None``, ``body=b''`` and, via
    ``run_record``, ``server_date_utc=None``, ``skew_s=None`` and
    ``body_bytes=0``. Only the text of ``error`` differs, and that is
    a human-readable message, not a marker to branch on.

    Parameters
    ----------
    feed : Feed
        The feed the crashed poll would have fetched.

    Returns
    -------
    PollOutcome
        Never stored, since nothing was ever fetched to store.
    """
    now = datetime.now(UTC).isoformat()
    return PollOutcome(
        record=RunRecord(
            feed=feed.value,
            fetched_at_utc=now,
            received_at_utc=now,
            rtt_s=0.0,
            server_date_utc=None,
            skew_s=None,
            status_code=None,
            body_bytes=0,
            error=CRASHED_POLL_ERROR,
        ),
        stored=False,
    )


def _gather(
    *,
    futures: list[Future[PollOutcome]],
    schedule: list[tuple[float, Feed]],
) -> list[PollOutcome]:
    """Collect every future's result, logging each failure.

    A future that raised is replaced with a crash placeholder for
    its own scheduled feed, rather than being dropped, so the
    returned list always has one entry per scheduled poll.

    Parameters
    ----------
    futures : list[Future[PollOutcome]]
        One future per scheduled poll, in schedule order.
    schedule : list[tuple[float, Feed]]
        The same schedule the futures were submitted from, used to
        attribute a crashed future to its feed.

    Returns
    -------
    list[PollOutcome]
        One outcome per scheduled poll: the real outcome for a
        survivor, a crash placeholder for one that raised.

    Raises
    ------
    ScheduleError
        If any future raised. Every failing future is logged, not
        only the one whose exception is raised. The exception is
        stripped of its traceback before it is kept: those frames
        include ``poll_and_store``'s local ``result``, which would
        otherwise hold the fetched body alive for the rest of the
        invocation.
    """
    outcomes: list[PollOutcome] = []
    first_error: BaseException | None = None
    for future, (_, feed) in zip(futures, schedule, strict=True):
        try:
            outcomes.append(future.result())
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception('Poll worker failed unexpectedly')
            exc.with_traceback(None)
            outcomes.append(_crash_outcome(feed=feed))
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise ScheduleError(
            outcomes=outcomes, cause=first_error,
        ) from first_error
    return outcomes


def _recover_from_mismatch(
    *, schedule: list[tuple[float, Feed]], error: ValueError,
) -> ScheduleError:
    """Turn a futures/schedule length mismatch into a full crash set.

    A mismatch means a caller bug. It degrades to "every poll marked
    crashed" so that a run record is still written.

    Parameters
    ----------
    schedule : list[tuple[float, Feed]]
        The schedule that was submitted; used to attribute one
        crash placeholder to each scheduled poll.
    error : ValueError
        The error ``zip(..., strict=True)`` raised.

    Returns
    -------
    ScheduleError
        Carries a crash placeholder for every scheduled poll, so
        the caller can still write a complete run record.
    """
    logger.exception('Futures and schedule length mismatch')
    outcomes = [_crash_outcome(feed=feed) for _, feed in schedule]
    return ScheduleError(outcomes=outcomes, cause=error)


def run_schedule(
    *,
    schedule: list[tuple[float, Feed]],
    context: PollContext,
) -> list[PollOutcome]:
    """Fire every poll in the schedule at its own absolute offset.

    One thread per poll. A poll waiting on its offset, or blocked on
    the semaphore, never occupies a slot another poll needs to start
    waiting on its own offset.

    Parameters
    ----------
    schedule : list[tuple[float, Feed]]
        Offsets in seconds and the feed to poll at each.
    context : PollContext
        Shared API key, session, semaphore and repository.

    Returns
    -------
    list[PollOutcome]
        One outcome per scheduled poll, in schedule order, each
        already stored and carrying no payload bytes.

    Raises
    ------
    ScheduleError
        If any worker raised something ``store_one`` did not
        anticipate, or if ``futures`` and ``schedule`` came out of
        step (see ``_recover_from_mismatch``). ``fetch_feed`` never
        raises and ``store_one`` contains ``ClientError`` and
        ``BotoCoreError``, so the first case covers only an
        unenumerated failure.
    """
    started_at = time.monotonic()
    with ThreadPoolExecutor(max_workers=len(schedule)) as pool:
        futures = submit_polls(
            schedule=schedule,
            started_at=started_at,
            context=context,
            pool=pool,
        )
        try:
            return _gather(futures=futures, schedule=schedule)
        except ValueError as error:
            raise _recover_from_mismatch(
                schedule=schedule, error=error,
            ) from error


def record_run(
    *,
    results: list[PollOutcome],
    repository: RawFeedRepository,
    invocation_id: str,
) -> CollectionCounts:
    """Write the invocation's audit record and tally the outcomes.

    The payloads are already stored by this point, each by its own
    poll. The run record is written even when every poll failed to
    fetch, failed to store, or crashed: ``_gather`` turns a crashed
    poll into a placeholder outcome, so ``results`` has one entry per
    scheduled poll unless the schedule was empty.

    Parameters
    ----------
    results : list[PollOutcome]
        One outcome per scheduled poll, with every payload that was
        actually fetched already stored.
    repository : RawFeedRepository
        Destination repository.
    invocation_id : str
        Lambda request id.

    Returns
    -------
    CollectionCounts
        Counts of fetches attempted, payloads stored and fetches or
        stores failed. A crashed poll counts toward ``failed``: it
        was scheduled and attempted, even though it never produced
        a real result.
    """
    if not results:
        return CollectionCounts(fetched=0, stored=0, failed=0)
    records = [o.record for o in results]
    repository.put_run_record(
        records=records, invocation_id=invocation_id,
    )
    return CollectionCounts(
        fetched=len(results),
        stored=sum(1 for o in results if o.stored),
        failed=sum(
            1 for o in results if o.record['error'] is not None
        ),
    )


def collect(
    *,
    schedule: list[tuple[float, Feed]],
    api_key: str,
    repository: RawFeedRepository,
    invocation_id: str,
) -> CollectionCounts:
    """Run one full collection round against a given schedule.

    Parameters
    ----------
    schedule : list[tuple[float, Feed]]
        Offsets in seconds and the feed to poll at each.
    api_key : str
        TfNSW Open Data Hub API key.
    repository : RawFeedRepository
        Destination for payloads and the audit record.
    invocation_id : str
        Lambda request id.

    Returns
    -------
    CollectionCounts
        Counts of fetches attempted, payloads stored and fetches or
        stores failed.

    Raises
    ------
    ScheduleError
        If a poll worker raised something unanticipated. The run
        record for every poll that did complete is written in a
        ``finally`` block before this propagates, so the invocation
        fails loudly without losing the audit trail.
    """
    outcomes: list[PollOutcome] = []
    with requests.Session() as session:
        context = PollContext(
            api_key=api_key,
            session=session,
            semaphore=threading.Semaphore(MAX_CONCURRENT_POLLS),
            repository=repository,
        )
        try:
            outcomes = run_schedule(
                schedule=schedule, context=context,
            )
        except ScheduleError as error:
            outcomes = error.outcomes
            raise
        finally:
            counts = record_run(
                results=outcomes,
                repository=repository,
                invocation_id=invocation_id,
            )
    return counts


@logger.inject_lambda_context
def handler(
    event: dict[str, Any],  # pylint: disable=unused-argument
    context: Any,
) -> CollectionCounts:
    """Poll every feed on schedule and store what comes back.

    Parameters
    ----------
    event : dict[str, Any]
        EventBridge event; unused.
    context : Any
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
        repository=get_repository(),
        invocation_id=context.aws_request_id,
    )
    logger.info('Collection complete', extra=counts)
    return counts
