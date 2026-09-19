# pylint: disable=too-many-lines
"""Daily visibility into the live collector's S3 output.

Run locally, read-only, against the live bucket, from the repo root::

    uv run python -m scripts.check_collection

Summarises one UTC ``dt=`` partition of ``curated/collector_run``
audit records: poll counts, failures, timing, payload sizes, and
per-minute coverage gaps in the raw ``vehiclepos`` feed.

Pass ``--memory`` to also report the Lambda's memory and duration
envelope for that day, sourced from CloudWatch Logs Insights
rather than the S3 audit trail.
"""

import argparse
import json
import os
import statistics
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from typing import Final

import boto3
from botocore.exceptions import ClientError

from common.types_ import CRASHED_POLL_ERROR as CRASH_ERROR
from common.types_ import Feed, RunRecord

DEFAULT_STACK_NAME: Final[str] = 'sydney-bus-reliability'
MAX_WORKERS: Final[int] = 16  # threads
EXAMPLE_LIMIT: Final[int] = 5  # examples
WORST_GAPS_LIMIT: Final[int] = 10  # gaps
EXPECTED_PER_MINUTE: Final[int] = 6  # objects
MINUTES_PER_DAY: Final[int] = 24 * 60  # minutes
EXPECTED_PER_MINUTE_BY_FEED: Final[dict[str, int]] = {
    Feed.VEHICLE_POSITIONS.value: 6,
    Feed.TRIP_UPDATES.value: 1,
}
COLLECTOR_FUNCTION_NAME: Final[str] = 'sydney-bus-reliability-collector'
COLLECTOR_LOG_GROUP: Final[str] = (
    '/aws/lambda/sydney-bus-reliability-collector'
)
BOUNDARY_WINDOW: Final[timedelta] = timedelta(minutes=2)
MEMORY_BIN_MINUTES: Final[int] = 10  # minutes
MEMORY_QUERY: Final[str] = (
    'filter @type = "REPORT"\n'
    '| stats max(@maxMemoryUsed)/1000/1000 as maxMB, '
    'max(@duration) as maxDurationMs, count(*) as invocations '
    'by bin(10m)'
)
QUERY_POLL_INTERVAL_S: Final[float] = 1.0  # seconds
QUERY_TIMEOUT_S: Final[float] = 60.0  # seconds


@dataclass(frozen=True, slots=True)
class CollectionWindow:
    """The observed span of activity within a day's audit rows.

    Attributes
    ----------
    start : datetime
        Earliest ``fetched_at_utc`` seen across all rows.
    end : datetime
        Latest ``fetched_at_utc`` seen across all rows.
    minutes : int
        Whole minutes spanned, inclusive of both ends.
    """

    start: datetime
    end: datetime
    minutes: int


@dataclass(frozen=True, slots=True)
class FeedCount:
    """Actual versus expected polls for one feed, over one window.

    Attributes
    ----------
    actual : int
        Number of audit rows seen for this feed.
    expected : int
        Polls expected over the observed collection window, not a
        fixed daily total, so a partial day is not misread as loss.
    """

    actual: int
    expected: int


@dataclass(frozen=True, slots=True)
class BytesStats:
    """Payload size statistics for one feed.

    Attributes
    ----------
    minimum : int
        Smallest ``body_bytes`` seen.
    median : float
        Median ``body_bytes``.
    maximum : int
        Largest ``body_bytes`` seen.
    """

    minimum: int
    median: float
    maximum: int


@dataclass(frozen=True, slots=True)
class TimingStats:
    """Min/median/max of a timing measurement, in seconds.

    Attributes
    ----------
    minimum : float
        Smallest value seen.
    median : float
        Median value.
    maximum : float
        Largest value seen.
    """

    minimum: float
    median: float
    maximum: float


@dataclass(frozen=True, slots=True)
class FailureSummary:
    """Counts and examples of rows that indicate a lost poll.

    Each row is filed under exactly one of these counts, its most
    specific failure kind, so a crashed poll (which also carries a
    non-null ``error`` and a null ``server_date_utc``) is never
    added to more than one total. See `_failure_category`.

    Attributes
    ----------
    crashed_count : int
        Rows whose poll worker crashed before ever making a
        request.
    transport_error_count : int
        Rows with a non-null ``error`` and no HTTP response at all
        (``status_code`` is null), other than a crash.
    non_200_count : int
        Rows with an HTTP response whose ``status_code`` is not 200.
    null_server_date_count : int
        Otherwise-successful rows with a null ``server_date_utc``.
    examples : list[RunRecord]
        A handful of the offending rows, for triage.
    """

    crashed_count: int
    transport_error_count: int
    non_200_count: int
    null_server_date_count: int
    examples: list[RunRecord]


@dataclass(frozen=True, slots=True)
class CoverageGap:
    """One minute of the day with too few vehiclepos polls.

    Attributes
    ----------
    minute : str
        The minute, as ``HH:MM`` UTC.
    count : int
        Polls actually seen in that minute.
    """

    minute: str
    count: int


@dataclass(frozen=True, slots=True)
class CoverageSummary:
    """Per-minute coverage of the vehiclepos feed.

    Attributes
    ----------
    minutes_short : int
        Minutes with fewer than the expected polls.
    worst : list[CoverageGap]
        The worst-covered minutes, sparsest first.
    """

    minutes_short: int
    worst: list[CoverageGap]


@dataclass(frozen=True, slots=True)
class CollectionSummary:  # pylint: disable=too-many-instance-attributes
    """Everything the human needs to see about one day's collection.

    Attributes
    ----------
    feed_counts : dict[str, FeedCount]
        Actual versus expected polls, keyed by feed name.
    status_counts : dict[str, dict[str, int]]
        Poll counts keyed by feed name, then status code as a
        string (``"None"`` for a missing status code).
    failures : FailureSummary
        Rows that indicate lost data.
    rtt : TimingStats | None
        Round-trip time stats, or None if there were no rows.
    skew : TimingStats | None
        Clock skew stats, or None if every row had a null skew.
    payload_by_feed : dict[str, BytesStats]
        Payload size stats, keyed by feed name.
    total_bytes : int
        Total uncompressed ``body_bytes`` across every row. This
        is what was fetched, not what is stored - S3 objects are
        gzipped and considerably smaller.
    coverage : CoverageSummary
        Per-minute coverage of the vehiclepos feed, within the
        observed collection window only.
    window : CollectionWindow | None
        The observed span of activity, or None if there were no
        rows at all.
    """

    feed_counts: dict[str, FeedCount]
    status_counts: dict[str, dict[str, int]]
    failures: FailureSummary
    rtt: TimingStats | None
    skew: TimingStats | None
    payload_by_feed: dict[str, BytesStats]
    total_bytes: int
    coverage: CoverageSummary
    window: CollectionWindow | None


@dataclass(frozen=True, slots=True)
class MemoryBin:
    """One 10-minute bin of Lambda memory and duration stats.

    Attributes
    ----------
    label : str
        The bin's start time, as ``HH:MM`` UTC.
    max_mb : float
        Largest ``@maxMemoryUsed`` seen in this bin, in MB.
    max_duration_ms : float
        Largest ``@duration`` seen in this bin, in milliseconds.
    invocations : int
        Number of REPORT lines seen in this bin.
    """

    label: str
    max_mb: float
    max_duration_ms: float
    invocations: int


@dataclass(frozen=True, slots=True)
class MemoryHeadroom:
    """How much memory ceiling is left above observed usage.

    Attributes
    ----------
    max_used_mb : float
        Largest ``@maxMemoryUsed`` across every bin, in MB.
    ceiling_mb : int
        The function's configured memory ceiling, in MB.
    headroom_mb : float
        `ceiling_mb` minus `max_used_mb`.
    headroom_pct : float
        `headroom_mb` as a percentage of `ceiling_mb`.
    """

    max_used_mb: float
    ceiling_mb: int
    headroom_mb: float
    headroom_pct: float


def _memory_headroom(
    *, max_used_mb: float, ceiling_mb: int,
) -> MemoryHeadroom:
    """Compute headroom between observed usage and the ceiling.

    Parameters
    ----------
    max_used_mb : float
        Largest ``@maxMemoryUsed`` across every bin, in MB.
    ceiling_mb : int
        The function's configured memory ceiling, in MB.

    Returns
    -------
    MemoryHeadroom
        The observed usage alongside its headroom.
    """
    headroom_mb = ceiling_mb - max_used_mb
    return MemoryHeadroom(
        max_used_mb=max_used_mb,
        ceiling_mb=ceiling_mb,
        headroom_mb=headroom_mb,
        headroom_pct=headroom_mb / ceiling_mb * 100,
    )


@dataclass(frozen=True, slots=True)
class MemorySummary:
    """The collector Lambda's memory and duration envelope.

    Attributes
    ----------
    bins : list[MemoryBin]
        Per-bin breakdown, ordered by `label`.
    headroom : MemoryHeadroom
        Observed peak usage versus the configured ceiling.
    total_invocations : int
        Sum of `invocations` across every bin.
    max_duration_ms : float
        Largest ``@duration`` across every bin, in milliseconds.
    covered_minutes : int
        Minutes of bins actually observed. Bins with no REPORT
        lines never appear in the query results, so this is a
        lower bound on the true collection window, not a precise
        span - enough to flag a partial day.
    """

    bins: list[MemoryBin]
    headroom: MemoryHeadroom
    total_invocations: int
    max_duration_ms: float
    covered_minutes: int


def _memory_bins(rows: list[dict[str, str]]) -> list[MemoryBin]:
    """Parse raw Logs Insights rows into sorted memory bins.

    Parameters
    ----------
    rows : list[dict[str, str]]
        One dict per result row, built from the ``field``/``value``
        pairs returned by ``get_query_results``.

    Returns
    -------
    list[MemoryBin]
        One bin per row, ordered by `MemoryBin.label`.
    """
    bins = [
        MemoryBin(
            label=row['bin(10m)'][11:16],
            max_mb=float(row['maxMB']),
            max_duration_ms=float(row['maxDurationMs']),
            invocations=int(row['invocations']),
        )
        for row in rows
    ]
    return sorted(bins, key=lambda one_bin: one_bin.label)


def summarize_memory(
    rows: list[dict[str, str]], *, memory_size_mb: int,
) -> MemorySummary | None:
    """Summarise Logs Insights memory rows. Pure, no I/O.

    Parameters
    ----------
    rows : list[dict[str, str]]
        Raw result rows from the memory query, one dict per row.
    memory_size_mb : int
        The function's configured memory ceiling, in MB.

    Returns
    -------
    MemorySummary | None
        The full memory envelope, or None if `rows` is empty.
    """
    if not rows:
        return None
    bins = _memory_bins(rows)
    max_used_mb = max(one_bin.max_mb for one_bin in bins)
    headroom = _memory_headroom(
        max_used_mb=max_used_mb, ceiling_mb=memory_size_mb,
    )
    return MemorySummary(
        bins=bins,
        headroom=headroom,
        total_invocations=sum(one_bin.invocations for one_bin in bins),
        max_duration_ms=max(one_bin.max_duration_ms for one_bin in bins),
        covered_minutes=len(bins) * MEMORY_BIN_MINUTES,
    )


def _feed_counts(
    rows: list[RunRecord], *, window_minutes: int,
) -> dict[str, FeedCount]:
    """Tally actual polls per feed against the observed window.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.
    window_minutes : int
        Minutes the collector was actually observed running for.

    Returns
    -------
    dict[str, FeedCount]
        Actual versus expected counts, keyed by feed name.
    """
    actual = Counter(row['feed'] for row in rows)
    return {
        feed: FeedCount(
            actual=actual.get(feed, 0),
            expected=window_minutes * per_minute,
        )
        for feed, per_minute in EXPECTED_PER_MINUTE_BY_FEED.items()
    }


def _failure_category(row: RunRecord) -> str:
    """Classify one row into its single most specific failure kind.

    Checked in order from most to least specific, so a crashed
    poll (which also has a non-null ``error`` and a null
    ``server_date_utc``) is never also read as a transport error or
    a missing-date anomaly.

    Parameters
    ----------
    row : RunRecord
        One audit row.

    Returns
    -------
    str
        One of ``'crashed'``, ``'transport_error'``, ``'non_200'``,
        ``'null_server_date'``, or ``'ok'``.
    """
    if row['error'] == CRASH_ERROR:
        return 'crashed'
    if row['error'] is not None and row['status_code'] is None:
        return 'transport_error'
    if row['status_code'] is not None and row['status_code'] != (
        HTTPStatus.OK
    ):
        return 'non_200'
    if row['server_date_utc'] is None:
        return 'null_server_date'
    return 'ok'


def _status_counts(rows: list[RunRecord]) -> dict[str, dict[str, int]]:
    """Tally polls per feed, broken down by outcome.

    A crash and a transport failure both carry ``status_code=None``
    but are filed under distinct keys (``'crashed'`` versus
    ``'transport_error'``), so neither is hidden behind a shared
    ``'None'`` bucket.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.

    Returns
    -------
    dict[str, dict[str, int]]
        Counts keyed by feed name, then status code (or
        ``'crashed'``/``'transport_error'``) as a string.
    """
    result: dict[str, dict[str, int]] = {}
    for row in rows:
        by_status = result.setdefault(row['feed'], {})
        category = _failure_category(row)
        status_key = (
            category
            if category in ('crashed', 'transport_error')
            else str(row['status_code'])
        )
        by_status[status_key] = by_status.get(status_key, 0) + 1
    return result


def _failure_summary(rows: list[RunRecord]) -> FailureSummary:
    """Find rows that indicate a poll was lost or unusable.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.

    Returns
    -------
    FailureSummary
        Counts and a handful of example rows, each row counted
        exactly once under its most specific category.
    """
    categories = [_failure_category(row) for row in rows]
    examples = [
        row for row, category in zip(rows, categories)
        if category != 'ok'
    ][:EXAMPLE_LIMIT]
    return FailureSummary(
        crashed_count=categories.count('crashed'),
        transport_error_count=categories.count('transport_error'),
        non_200_count=categories.count('non_200'),
        null_server_date_count=categories.count('null_server_date'),
        examples=examples,
    )


def _timing_stats(values: list[float]) -> TimingStats | None:
    """Summarise a list of timing measurements.

    Parameters
    ----------
    values : list[float]
        Non-null measurements in seconds.

    Returns
    -------
    TimingStats | None
        Min/median/max, or None if `values` is empty.
    """
    if not values:
        return None
    return TimingStats(
        minimum=min(values),
        median=statistics.median(values),
        maximum=max(values),
    )


def _payload_by_feed(rows: list[RunRecord]) -> dict[str, BytesStats]:
    """Summarise payload size per feed.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.

    Returns
    -------
    dict[str, BytesStats]
        Min/median/max ``body_bytes``, keyed by feed name.
    """
    by_feed: dict[str, list[int]] = {}
    for row in rows:
        by_feed.setdefault(row['feed'], []).append(row['body_bytes'])
    return {
        feed: BytesStats(
            minimum=min(sizes),
            median=statistics.median(sizes),
            maximum=max(sizes),
        )
        for feed, sizes in by_feed.items()
    }


def _collection_window(
    rows: list[RunRecord],
) -> CollectionWindow | None:
    """Find the observed span of activity across all rows.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.

    Returns
    -------
    CollectionWindow | None
        The observed window, or None if `rows` is empty.
    """
    if not rows:
        return None
    fetched_ats = [
        datetime.fromisoformat(row['fetched_at_utc']) for row in rows
    ]
    start, end = min(fetched_ats), max(fetched_ats)
    minutes = int((end - start).total_seconds() // 60) + 1
    return CollectionWindow(start=start, end=end, minutes=minutes)


def _minute_labels(*, window: CollectionWindow) -> list[str]:
    """List every minute of a window as ``HH:MM`` strings.

    Parameters
    ----------
    window : CollectionWindow
        The observed span of activity.

    Returns
    -------
    list[str]
        One label per minute, in order, starting at the window's
        first minute.
    """
    floor_start = window.start.replace(second=0, microsecond=0)
    return [
        (floor_start + timedelta(minutes=offset)).strftime('%H:%M')
        for offset in range(window.minutes)
    ]


def _coverage_summary(
    rows: list[RunRecord],
    *,
    boundary_rows: list[RunRecord],
    window: CollectionWindow,
) -> CoverageSummary:
    """Find minutes within the window with too few vehiclepos polls.

    `boundary_rows` are stitched in purely to fill out minute
    counts at the edges of a UTC-midnight-straddling invocation;
    the window itself is derived from `rows` alone, so this never
    grows the range of minutes evaluated.

    Only rows with no ``error`` count towards a minute's coverage:
    a row carrying an error, whether a crash placeholder or a
    fetch failure, never produced a stored S3 object, so counting
    it would hide a real, permanent loss of data.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.
    boundary_rows : list[RunRecord]
        Rows from a neighbouring partition, for gap-filling only.
    window : CollectionWindow
        The observed span of activity. Minutes outside it are not
        evaluated, since the collector was not running then.

    Returns
    -------
    CoverageSummary
        How many minutes were short, and the worst offenders.
    """
    vehiclepos = [
        row for row in rows + boundary_rows
        if row['feed'] == Feed.VEHICLE_POSITIONS.value
        and row['error'] is None
    ]
    per_minute = Counter(row['fetched_at_utc'][11:16] for row in vehiclepos)
    gaps = [
        CoverageGap(minute=minute, count=per_minute.get(minute, 0))
        for minute in _minute_labels(window=window)
        if per_minute.get(minute, 0) < EXPECTED_PER_MINUTE
    ]
    worst = sorted(gaps, key=lambda gap: gap.count)[:WORST_GAPS_LIMIT]
    return CoverageSummary(minutes_short=len(gaps), worst=worst)


def _coverage_for(
    rows: list[RunRecord],
    *,
    boundary_rows: list[RunRecord],
    window: CollectionWindow | None,
) -> CoverageSummary:
    """Compute coverage gaps, or an empty result with no window.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.
    boundary_rows : list[RunRecord]
        Rows from a neighbouring partition, for gap-filling only.
    window : CollectionWindow | None
        The observed span of activity, or None if `rows` is empty.

    Returns
    -------
    CoverageSummary
        Coverage gaps within `window`, or an empty summary.
    """
    if window is None:
        return CoverageSummary(minutes_short=0, worst=[])
    return _coverage_summary(rows, boundary_rows=boundary_rows, window=window)


def _window_and_coverage(
    rows: list[RunRecord], *, boundary_rows: list[RunRecord] | None,
) -> tuple[CollectionWindow | None, CoverageSummary]:
    """Compute the observed window and its coverage summary.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.
    boundary_rows : list[RunRecord] | None
        Rows from a neighbouring partition, for gap-filling only.
        None (e.g. a missing neighbouring partition) behaves as
        empty.

    Returns
    -------
    tuple[CollectionWindow | None, CoverageSummary]
        The observed window, and coverage gaps within it.
    """
    window = _collection_window(rows)
    coverage = _coverage_for(
        rows, boundary_rows=boundary_rows or [], window=window,
    )
    return window, coverage


def summarize_day(
    rows: list[RunRecord], *, boundary_rows: list[RunRecord] | None = None,
) -> CollectionSummary:
    """Summarise one UTC day's audit rows. Pure, no I/O.

    `boundary_rows` is only used to fill in per-minute vehiclepos
    counts at a UTC-midnight-straddling invocation; it never
    contributes to poll totals, payload totals, timing stats or
    failure counts, which are computed from `rows` alone.

    Parameters
    ----------
    rows : list[RunRecord]
        Every audit row for the day, in any order.
    boundary_rows : list[RunRecord] | None
        Rows pulled from a neighbouring ``dt=`` partition that
        fall on this day, for coverage-gap stitching only. None
        (e.g. a missing neighbouring partition) behaves as empty.

    Returns
    -------
    CollectionSummary
        The full report structure, ready to print.
    """
    rtts = [row['rtt_s'] for row in rows]
    skews = [row['skew_s'] for row in rows if row['skew_s'] is not None]
    total_bytes = sum(row['body_bytes'] for row in rows)
    window, coverage = _window_and_coverage(rows, boundary_rows=boundary_rows)
    window_minutes = window.minutes if window else MINUTES_PER_DAY
    return CollectionSummary(
        feed_counts=_feed_counts(rows, window_minutes=window_minutes),
        status_counts=_status_counts(rows),
        failures=_failure_summary(rows),
        rtt=_timing_stats(rtts),
        skew=_timing_stats(skews),
        payload_by_feed=_payload_by_feed(rows),
        total_bytes=total_bytes,
        coverage=coverage,
        window=window,
    )


class CollectionRunRepository:
    """Reads one day's collector_run audit rows from S3.

    Parameters
    ----------
    bucket : str
        S3 bucket name.
    session : boto3.Session | None
        Optional boto3 session. Defaults to a new session using
        the standard credential chain (respects ``AWS_PROFILE``).
    """

    def __init__(
        self,
        *,
        bucket: str,
        session: boto3.Session | None = None,
    ) -> None:
        session = session or boto3.Session()
        self.bucket = bucket
        self.client = session.client('s3')

    def list_run_keys(self, *, date: str) -> list[str]:
        """List every audit object key for one day.

        Parameters
        ----------
        date : str
            The day, as ``YYYY-MM-DD``.

        Returns
        -------
        list[str]
            S3 keys under ``curated/collector_run/dt=<date>/``.
        """
        prefix = f'curated/collector_run/dt={date}/'
        paginator = self.client.get_paginator('list_objects_v2')
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj['Key'] for obj in page.get('Contents', []))
        return keys

    def _recent_keys(
        self, *, date: str, cutoff: datetime,
    ) -> list[str]:
        """List one partition's keys with ``LastModified >= cutoff``.

        Listing itself covers the whole partition, which is cheap
        (keys and metadata only); this bounds which keys go on to
        be fetched with `_fetch_rows`, so a boundary stitch never
        downloads a full day's worth of objects.

        Parameters
        ----------
        date : str
            The neighbouring day, as ``YYYY-MM-DD``.
        cutoff : datetime
            Only keys last modified at or after this instant are
            returned.

        Returns
        -------
        list[str]
            Matching S3 keys, possibly empty if the partition does
            not exist or has nothing recent enough.
        """
        prefix = f'curated/collector_run/dt={date}/'
        paginator = self.client.get_paginator('list_objects_v2')
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(
                obj['Key'] for obj in page.get('Contents', [])
                if obj['LastModified'] >= cutoff
            )
        return keys

    def fetch_boundary_rows(self, *, date: str) -> list[RunRecord]:
        """Fetch the previous day's rows that spill onto `date`.

        An invocation straddling UTC midnight is filed under the
        earlier day's partition, but some of its rows carry a
        `fetched_at_utc` on `date`. Only objects modified within
        `BOUNDARY_WINDOW` of midnight are fetched, for stitching
        coverage gaps at ``00:00`` - never for `date`'s own totals.

        Parameters
        ----------
        date : str
            The day being analysed, as ``YYYY-MM-DD``.

        Returns
        -------
        list[RunRecord]
            Rows from the previous partition whose `fetched_at_utc`
            falls on `date`; empty if that partition is missing.
        """
        midnight = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=UTC)
        previous_date = (midnight - timedelta(days=1)).strftime('%Y-%m-%d')
        keys = self._recent_keys(
            date=previous_date, cutoff=midnight - BOUNDARY_WINDOW,
        )
        return self._fetch_boundary_matches(keys=keys, date=date)

    def _fetch_boundary_matches(
        self, *, keys: list[str], date: str,
    ) -> list[RunRecord]:
        """Fetch keys from the previous partition and keep matches.

        Parameters
        ----------
        keys : list[str]
            S3 keys to fetch from the previous partition.
        date : str
            The day being analysed, as ``YYYY-MM-DD``.

        Returns
        -------
        list[RunRecord]
            Rows whose `fetched_at_utc` falls on `date`.
        """
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            results = pool.map(lambda key: self._fetch_rows(key=key), keys)
        rows = [row for batch in results for row in batch]
        return [row for row in rows if row['fetched_at_utc'][:10] == date]

    def _fetch_rows(self, *, key: str) -> list[RunRecord]:
        """Fetch and parse one JSON Lines audit object.

        Parameters
        ----------
        key : str
            S3 key of the object to fetch.

        Returns
        -------
        list[RunRecord]
            One row per line in the object.
        """
        body = self.client.get_object(
            Bucket=self.bucket, Key=key,
        )['Body'].read()
        lines = body.decode().splitlines()
        return [json.loads(line) for line in lines if line]

    def fetch_day(
        self, *, date: str, max_workers: int = MAX_WORKERS,
    ) -> list[RunRecord]:
        """Fetch every audit row for one day, concurrently.

        Parameters
        ----------
        date : str
            The day, as ``YYYY-MM-DD``.
        max_workers : int
            Bound on concurrent S3 fetches.

        Returns
        -------
        list[RunRecord]
            Every audit row across all objects for the day.
        """
        keys = self.list_run_keys(date=date)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = pool.map(
                lambda key: self._fetch_rows(key=key), keys,
            )
        return [row for rows in results for row in rows]


class CloudWatchMemoryRepository:
    """Reads the collector's memory envelope from CloudWatch.

    Parameters
    ----------
    session : boto3.Session | None
        Optional boto3 session. Defaults to a new session using
        the standard credential chain (respects ``AWS_PROFILE``).
    """

    def __init__(self, *, session: boto3.Session | None = None) -> None:
        session = session or boto3.Session()
        self.logs = session.client('logs')
        self.lambda_client = session.client('lambda')

    def memory_size_mb(self, *, function_name: str) -> int:
        """Read the function's configured memory ceiling.

        Parameters
        ----------
        function_name : str
            Name of the deployed Lambda function.

        Returns
        -------
        int
            Configured memory size, in MB.
        """
        config = self.lambda_client.get_function_configuration(
            FunctionName=function_name,
        )
        return int(config['MemorySize'])

    def query_memory_rows(
        self, *, log_group: str, start: datetime, end: datetime,
    ) -> list[dict[str, str]]:
        """Run the memory-envelope query and return its rows.

        Parameters
        ----------
        log_group : str
            CloudWatch Logs group to query.
        start : datetime
            Start of the query window, UTC.
        end : datetime
            End of the query window, UTC.

        Returns
        -------
        list[dict[str, str]]
            One dict per result row, keyed by field name.
        """
        query_id = self.logs.start_query(
            logGroupName=log_group,
            startTime=int(start.timestamp()),
            endTime=int(end.timestamp()),
            queryString=MEMORY_QUERY,
        )['queryId']
        return self._poll_results(query_id=query_id)

    def _poll_results(self, *, query_id: str) -> list[dict[str, str]]:
        """Poll ``get_query_results`` until the query settles.

        Parameters
        ----------
        query_id : str
            Query id returned by ``start_query``.

        Returns
        -------
        list[dict[str, str]]
            One dict per result row, keyed by field name.

        Raises
        ------
        RuntimeError
            If the query fails, is cancelled, or times out
            server-side.
        TimeoutError
            If the query has not settled within `QUERY_TIMEOUT_S`.
        """
        deadline = time.monotonic() + QUERY_TIMEOUT_S
        while time.monotonic() < deadline:
            response = self.logs.get_query_results(queryId=query_id)
            status = response['status']
            if status == 'Complete':
                return [
                    {field['field']: field['value'] for field in row}
                    for row in response['results']
                ]
            if status in ('Failed', 'Cancelled', 'Timeout'):
                raise RuntimeError(f'query {status.lower()}: {query_id}')
            time.sleep(QUERY_POLL_INTERVAL_S)
        raise TimeoutError(f'query did not complete: {query_id}')


def _memory_window(*, date: str) -> tuple[datetime, datetime]:
    """Compute the UTC query window for one day's memory report.

    Parameters
    ----------
    date : str
        The day, as ``YYYY-MM-DD``.

    Returns
    -------
    tuple[datetime, datetime]
        Start and end of the window, UTC. The end is capped at
        now, so a partial in-progress day is not queried past
        its actual data.
    """
    start = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=UTC)
    end = min(start + timedelta(days=1), datetime.now(UTC))
    return start, end


def _fmt_duration(*, minutes: int) -> str:
    """Format a minute count as ``HhMMm``.

    Parameters
    ----------
    minutes : int
        Duration in whole minutes.

    Returns
    -------
    str
        Duration formatted as e.g. ``4h 08m``.
    """
    hours, remainder = divmod(minutes, 60)
    return f'{hours}h {remainder:02d}m'


def _fmt_window(window: CollectionWindow | None) -> str:
    """Format the observed collection window for display.

    Parameters
    ----------
    window : CollectionWindow | None
        The observed span of activity, or None if there were no
        rows at all.

    Returns
    -------
    str
        A human-readable window description.
    """
    if window is None:
        return 'no rows observed'
    span = _fmt_duration(minutes=window.minutes)
    return f'{window.start:%H:%M}–{window.end:%H:%M} UTC ({span})'


def _print_window(summary: CollectionSummary) -> None:
    """Print the observed collection window and a partial-day note.

    Parameters
    ----------
    summary : CollectionSummary
        The report to print from.
    """
    window = summary.window
    print(f'collection window: {_fmt_window(window)}')
    if window is not None and window.minutes < MINUTES_PER_DAY:
        covered = _fmt_duration(minutes=window.minutes)
        print(f'partial day: window covers {covered} of 24h')


def _print_counts(summary: CollectionSummary) -> None:
    """Print poll count and status breakdowns.

    Parameters
    ----------
    summary : CollectionSummary
        The report to print from.
    """
    print('\n== Poll counts (expected over the observed window) ==')
    for feed, count in summary.feed_counts.items():
        shortfall = count.expected - count.actual
        print(
            f'{feed}: {count.actual:,} / {count.expected:,} '
            f'expected (shortfall {shortfall:,})'
        )
    for feed, by_status in summary.status_counts.items():
        print(f'  {feed} by status: {by_status}')


def _print_failures(summary: CollectionSummary) -> None:
    """Print failure counts and a few example rows.

    Parameters
    ----------
    summary : CollectionSummary
        The report to print from.
    """
    failures = summary.failures
    print('\n== Failures ==')
    print(f'crashed polls: {failures.crashed_count:,}')
    print(f'transport errors: {failures.transport_error_count:,}')
    print(f'non-200 statuses: {failures.non_200_count:,}')
    print(f'null server_date_utc: {failures.null_server_date_count:,}')
    for example in failures.examples:
        print(f'  example: {example}')


def _fmt_seconds(value: float) -> str:
    """Format a timing value with fixed decimal places.

    Parameters
    ----------
    value : float
        A duration in seconds.

    Returns
    -------
    str
        `value` formatted to millisecond precision.
    """
    return f'{value:.3f}s'


def _fmt_timing(stats: TimingStats | None) -> str:
    """Format min/median/max timing statistics for display.

    Parameters
    ----------
    stats : TimingStats | None
        The statistics to format, or None if there were none.

    Returns
    -------
    str
        A human-readable summary of `stats`.
    """
    if stats is None:
        return 'n/a'
    return (
        f'min {_fmt_seconds(stats.minimum)}, '
        f'median {_fmt_seconds(stats.median)}, '
        f'max {_fmt_seconds(stats.maximum)}'
    )


def _fmt_bytes(value: float) -> str:
    """Format a byte count with a sensible unit.

    Parameters
    ----------
    value : float
        A size in bytes.

    Returns
    -------
    str
        `value` formatted in B, KB or MB, whichever reads best.
    """
    if value >= 1_000_000:
        return f'{value / 1_000_000:.1f} MB'
    if value >= 1_000:
        return f'{value / 1_000:.1f} KB'
    return f'{value:,.0f} B'


def _fmt_bytes_stats(stats: BytesStats) -> str:
    """Format min/median/max payload-size statistics for display.

    Parameters
    ----------
    stats : BytesStats
        The statistics to format.

    Returns
    -------
    str
        A human-readable summary of `stats`.
    """
    return (
        f'min {_fmt_bytes(stats.minimum)}, '
        f'median {_fmt_bytes(stats.median)}, '
        f'max {_fmt_bytes(stats.maximum)}'
    )


def _print_timing_and_payload(summary: CollectionSummary) -> None:
    """Print timing and payload statistics.

    Parameters
    ----------
    summary : CollectionSummary
        The report to print from.
    """
    print('\n== Timing ==')
    print(f'rtt_s: {_fmt_timing(summary.rtt)}')
    print(f'skew_s: {_fmt_timing(summary.skew)}')
    print('\n== Payload ==')
    for feed, stats in summary.payload_by_feed.items():
        print(f'{feed}: {_fmt_bytes_stats(stats)}')
    print(
        'total payload fetched (uncompressed): '
        f'{_fmt_bytes(summary.total_bytes)}'
    )


def _print_coverage(summary: CollectionSummary) -> None:
    """Print coverage gap statistics, within the observed window.

    Parameters
    ----------
    summary : CollectionSummary
        The report to print from.
    """
    coverage = summary.coverage
    shown = coverage.worst[:WORST_GAPS_LIMIT]
    remaining = coverage.minutes_short - len(shown)
    print('\n== Coverage gaps (vehiclepos, within window) ==')
    print(f'minutes short of {EXPECTED_PER_MINUTE} polls: '
          f'{coverage.minutes_short:,}')
    for gap in shown:
        print(f'  {gap.minute}: {gap.count} polls')
    if remaining > 0:
        print(f'  ... and {remaining:,} more short minutes')


def _fmt_memory_bin(one_bin: MemoryBin) -> str:
    """Format one memory bin as a single output line.

    Parameters
    ----------
    one_bin : MemoryBin
        The bin to format.

    Returns
    -------
    str
        A human-readable summary of `one_bin`.
    """
    return (
        f'  {one_bin.label}: {one_bin.max_mb:.0f} MB max, '
        f'{one_bin.max_duration_ms:.0f} ms max, '
        f'{one_bin.invocations} inv'
    )


def _print_memory_summary(summary: MemorySummary) -> None:
    """Print max usage, headroom, duration, and invocation count.

    Parameters
    ----------
    summary : MemorySummary
        The memory report to print from.
    """
    headroom = summary.headroom
    print(
        f'max memory used: {headroom.max_used_mb:.0f} MB / '
        f'{headroom.ceiling_mb} MB configured'
    )
    print(
        f'headroom: {headroom.headroom_mb:.0f} MB '
        f'({headroom.headroom_pct:.1f}% of ceiling)'
    )
    print(f'max duration: {summary.max_duration_ms:.0f} ms')
    print(f'invocations observed: {summary.total_invocations:,}')


def _print_memory_bins(summary: MemorySummary) -> None:
    """Print the per-bin memory and duration breakdown.

    Parameters
    ----------
    summary : MemorySummary
        The memory report to print from.
    """
    print(f'per-bin breakdown ({MEMORY_BIN_MINUTES}m bins):')
    for one_bin in summary.bins:
        print(_fmt_memory_bin(one_bin))


def print_memory_report(
    summary: MemorySummary | None, *, date: str,
) -> None:
    """Print the full memory-envelope report.

    Parameters
    ----------
    summary : MemorySummary | None
        The memory report, or None if no REPORT lines were found.
    date : str
        The UTC day the report covers, as ``YYYY-MM-DD``.
    """
    print(f'\n== Memory envelope (dt={date}, CloudWatch Logs) ==')
    if summary is None:
        print('no REPORT log lines found for this window')
        return
    if summary.covered_minutes < MINUTES_PER_DAY:
        covered = _fmt_duration(minutes=summary.covered_minutes)
        print(f'partial day: bins cover at least {covered} of 24h')
    _print_memory_summary(summary)
    _print_memory_bins(summary)


def print_report(summary: CollectionSummary, *, date: str) -> None:
    """Print the full human-readable report.

    Parameters
    ----------
    summary : CollectionSummary
        The report to print.
    date : str
        The UTC day the report covers, as ``YYYY-MM-DD``.
    """
    print(
        f'Report for dt={date} '
        f'({date}T00:00:00Z to {date}T23:59:59Z, UTC partition; '
        f'this is NOT a Sydney calendar day)'
    )
    _print_window(summary)
    _print_counts(summary)
    _print_failures(summary)
    _print_timing_and_payload(summary)
    _print_coverage(summary)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments.

    Parameters
    ----------
    argv : list[str]
        Arguments, excluding the program name.

    Returns
    -------
    argparse.Namespace
        Parsed `date`, `bucket`, `stack_name`, and `profile` values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', default=None, help='YYYY-MM-DD, UTC')
    parser.add_argument('--bucket', default=None)
    parser.add_argument('--stack-name', default=DEFAULT_STACK_NAME)
    parser.add_argument(
        '--profile',
        default=None,
        help='AWS named profile; defaults to AWS_PROFILE/the chain',
    )
    parser.add_argument(
        '--memory',
        action='store_true',
        help='also report the Lambda memory/duration envelope',
    )
    return parser.parse_args(argv)


def _bucket_from_stack(
    *, stack_name: str, session: boto3.Session | None = None,
) -> str | None:
    """Look up the collector bucket from the deployed stack.

    Parameters
    ----------
    stack_name : str
        Name of the deployed CloudFormation stack.
    session : boto3.Session | None
        Optional boto3 session. Defaults to a new session.

    Returns
    -------
    str | None
        The stack's ``BucketName`` output, or None if the stack
        or that output does not exist.
    """
    session = session or boto3.Session()
    client = session.client('cloudformation')
    try:
        stacks = client.describe_stacks(StackName=stack_name)['Stacks']
    except ClientError:
        return None
    outputs = stacks[0].get('Outputs', [])
    matches = [o for o in outputs if o['OutputKey'] == 'BucketName']
    return matches[0]['OutputValue'] if matches else None


def resolve_bucket(
    *,
    bucket_arg: str | None,
    stack_name: str,
    session: boto3.Session | None = None,
) -> str:
    """Resolve the collector's S3 bucket name.

    Tries, in order: the `bucket_arg` command-line value, the
    ``BUCKET_NAME`` environment variable, then a CloudFormation
    stack output lookup.

    Parameters
    ----------
    bucket_arg : str | None
        Value of the ``--bucket`` argument, if given.
    stack_name : str
        Stack to query when no bucket is given directly.
    session : boto3.Session | None
        Optional boto3 session. Defaults to a new session.

    Returns
    -------
    str
        The resolved S3 bucket name.

    Raises
    ------
    SystemExit
        If none of the three sources yields a bucket name.
    """
    bucket = (
        bucket_arg
        or os.getenv('BUCKET_NAME')
        or _bucket_from_stack(stack_name=stack_name, session=session)
    )
    if bucket:
        return bucket
    raise SystemExit(
        'Could not determine the bucket: pass --bucket, set '
        'BUCKET_NAME, or deploy the stack so it can be looked up.'
    )


def _session_for(*, profile: str | None) -> boto3.Session | None:
    """Build a boto3 session for an explicit profile, if given.

    Parameters
    ----------
    profile : str | None
        AWS named profile from ``--profile``, or None to use the
        standard credential chain (e.g. ``AWS_PROFILE``).

    Returns
    -------
    boto3.Session | None
        A session pinned to `profile`, or None to let the
        repository build a default session.
    """
    return boto3.Session(profile_name=profile) if profile else None


def _fetch_summary(
    *, args: argparse.Namespace, date: str,
) -> CollectionSummary:
    """Resolve the bucket, fetch a day's rows, and summarise them.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.
    date : str
        The day to fetch, as ``YYYY-MM-DD`` UTC.

    Returns
    -------
    CollectionSummary
        The summarised report for that day.
    """
    session = _session_for(profile=args.profile)
    bucket = resolve_bucket(
        bucket_arg=args.bucket,
        stack_name=args.stack_name,
        session=session,
    )
    repo = CollectionRunRepository(bucket=bucket, session=session)
    rows = repo.fetch_day(date=date)
    boundary_rows = repo.fetch_boundary_rows(date=date)
    return summarize_day(rows, boundary_rows=boundary_rows)


def _fetch_memory_summary(
    *, args: argparse.Namespace, date: str,
) -> MemorySummary | None:
    """Query CloudWatch and summarise the memory envelope.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.
    date : str
        The day to query, as ``YYYY-MM-DD`` UTC.

    Returns
    -------
    MemorySummary | None
        The summarised memory envelope, or None if no REPORT
        lines were found.
    """
    session = _session_for(profile=args.profile)
    repo = CloudWatchMemoryRepository(session=session)
    memory_size_mb = repo.memory_size_mb(
        function_name=COLLECTOR_FUNCTION_NAME,
    )
    start, end = _memory_window(date=date)
    rows = repo.query_memory_rows(
        log_group=COLLECTOR_LOG_GROUP, start=start, end=end,
    )
    return summarize_memory(rows, memory_size_mb=memory_size_mb)


def main(argv: list[str] | None = None) -> int:
    """Fetch, summarise, and print one day's collection report.

    Parameters
    ----------
    argv : list[str] | None
        Command-line arguments, or None to use `sys.argv`.

    Returns
    -------
    int
        Zero on success.
    """
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    date = args.date or datetime.now(UTC).strftime('%Y-%m-%d')
    summary = _fetch_summary(args=args, date=date)
    print_report(summary, date=date)
    if args.memory:
        memory_summary = _fetch_memory_summary(args=args, date=date)
        print_memory_report(memory_summary, date=date)
    return 0


if __name__ == '__main__':
    sys.exit(main())
