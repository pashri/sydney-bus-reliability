"""Daily visibility into the live collector's S3 output.

Run locally, read-only, against the live bucket, from the repo root::

    uv run python -m scripts.check_collection

Not deployed, and never imported from ``src/``. Summarises one UTC
``dt=`` partition of ``curated/collector_run`` audit records: poll
counts, failures, timing, payload sizes, and per-minute coverage
gaps in the raw ``vehiclepos`` feed.
"""

import argparse
import json
import os
import statistics
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from typing import Final

import boto3
from botocore.exceptions import ClientError

from src.common.types_ import Feed, RunRecord

DEFAULT_STACK_NAME: Final[str] = 'sydney-bus-reliability'
MAX_WORKERS: Final[int] = 16
EXAMPLE_LIMIT: Final[int] = 5
WORST_GAPS_LIMIT: Final[int] = 10
EXPECTED_PER_MINUTE: Final[int] = 6
MINUTES_PER_DAY: Final[int] = 24 * 60
EXPECTED_PER_MINUTE_BY_FEED: Final[dict[str, int]] = {
    Feed.VEHICLE_POSITIONS.value: 6,
    Feed.TRIP_UPDATES.value: 1,
}


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

    Attributes
    ----------
    error_count : int
        Rows with a non-null ``error``.
    non_200_count : int
        Rows whose ``status_code`` is not HTTP 200.
    null_server_date_count : int
        Rows with a null ``server_date_utc``.
    examples : list[RunRecord]
        A handful of the offending rows, for triage.
    """

    error_count: int
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
        is what was fetched, not what is stored — S3 objects are
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


def _status_counts(rows: list[RunRecord]) -> dict[str, dict[str, int]]:
    """Tally polls per feed, broken down by HTTP status code.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.

    Returns
    -------
    dict[str, dict[str, int]]
        Counts keyed by feed name, then status code as a string.
    """
    result: dict[str, dict[str, int]] = {}
    for row in rows:
        by_status = result.setdefault(row['feed'], {})
        status_key = str(row['status_code'])
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
        Counts and a handful of example rows.
    """
    errors = [row for row in rows if row['error'] is not None]
    non_200 = [
        row for row in rows
        if row['status_code'] != HTTPStatus.OK
    ]
    null_dates = [
        row for row in rows if row['server_date_utc'] is None
    ]
    examples = (errors + non_200 + null_dates)[:EXAMPLE_LIMIT]
    return FailureSummary(
        error_count=len(errors),
        non_200_count=len(non_200),
        null_server_date_count=len(null_dates),
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
    rows: list[RunRecord], *, window: CollectionWindow,
) -> CoverageSummary:
    """Find minutes within the window with too few vehiclepos polls.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.
    window : CollectionWindow
        The observed span of activity. Minutes outside it are not
        evaluated, since the collector was not running then.

    Returns
    -------
    CoverageSummary
        How many minutes were short, and the worst offenders.
    """
    vehiclepos = [
        row for row in rows
        if row['feed'] == Feed.VEHICLE_POSITIONS.value
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
    rows: list[RunRecord], *, window: CollectionWindow | None,
) -> CoverageSummary:
    """Compute coverage gaps, or an empty result with no window.

    Parameters
    ----------
    rows : list[RunRecord]
        Audit rows for the day.
    window : CollectionWindow | None
        The observed span of activity, or None if `rows` is empty.

    Returns
    -------
    CoverageSummary
        Coverage gaps within `window`, or an empty summary.
    """
    if window is None:
        return CoverageSummary(minutes_short=0, worst=[])
    return _coverage_summary(rows, window=window)


def summarize_day(rows: list[RunRecord]) -> CollectionSummary:
    """Summarise one UTC day's audit rows. Pure, no I/O.

    Parameters
    ----------
    rows : list[RunRecord]
        Every audit row for the day, in any order.

    Returns
    -------
    CollectionSummary
        The full report structure, ready to print.
    """
    rtts = [row['rtt_s'] for row in rows]
    skews = [row['skew_s'] for row in rows if row['skew_s'] is not None]
    total_bytes = sum(row['body_bytes'] for row in rows)
    window = _collection_window(rows)
    window_minutes = window.minutes if window else MINUTES_PER_DAY
    return CollectionSummary(
        feed_counts=_feed_counts(rows, window_minutes=window_minutes),
        status_counts=_status_counts(rows),
        failures=_failure_summary(rows),
        rtt=_timing_stats(rtts),
        skew=_timing_stats(skews),
        payload_by_feed=_payload_by_feed(rows),
        total_bytes=total_bytes,
        coverage=_coverage_for(rows, window=window),
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
    print(f'errors: {failures.error_count:,}')
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
    """Resolve the collector's bucket name without hardcoding it.

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
    return summarize_day(rows)


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
    return 0


if __name__ == '__main__':
    sys.exit(main())
