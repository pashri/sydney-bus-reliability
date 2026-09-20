"""Summarising one day of the collector's audit rows."""

import statistics
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Final

from common.types_ import CRASHED_POLL_ERROR as CRASH_ERROR
from common.types_ import Feed, RunRecord

EXAMPLE_LIMIT: Final[int] = 5  # examples
WORST_GAPS_LIMIT: Final[int] = 10  # gaps
EXPECTED_PER_MINUTE: Final[int] = 6  # objects
MINUTES_PER_DAY: Final[int] = 24 * 60  # minutes
EXPECTED_PER_MINUTE_BY_FEED: Final[dict[str, int]] = {
    Feed.VEHICLE_POSITIONS.value: 6,
    Feed.TRIP_UPDATES.value: 1,
}
BOUNDARY_WINDOW: Final[timedelta] = timedelta(minutes=2)
"""How far either side of midnight a stitched poll can sit."""


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
