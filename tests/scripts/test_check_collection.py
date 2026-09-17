"""Tests for the pure summarising logic in check_collection.py."""

from datetime import UTC, datetime
from http import HTTPStatus

from scripts.check_collection import (
    CollectionRunRepository,
    summarize_day,
    summarize_memory,
)
from src.common.types_ import Feed, RunRecord

DATE = '2026-09-15'


def _row(
    *,
    feed: Feed = Feed.VEHICLE_POSITIONS,
    minute: str = '00:00',
    second: str = '00',
    rtt_s: float = 0.2,
    skew_s: float | None = 0.05,
    status_code: int | None = HTTPStatus.OK,
    body_bytes: int = 1_000,
    server_date_utc: str | None = '2026-09-15T00:00:00+00:00',
    error: str | None = None,
) -> RunRecord:
    """Build one audit row with sensible defaults, overridable."""
    fetched_at = f'{DATE}T{minute}:{second}+00:00'
    return RunRecord(
        feed=feed.value,
        fetched_at_utc=fetched_at,
        received_at_utc=fetched_at,
        rtt_s=rtt_s,
        server_date_utc=server_date_utc,
        skew_s=skew_s,
        status_code=status_code,
        body_bytes=body_bytes,
        error=error,
    )


def _full_minute(*, minute: str) -> list[RunRecord]:
    """Build 6 vehiclepos rows filling one minute, at 10s intervals."""
    return [
        _row(minute=minute, second=f'{second:02d}')
        for second in range(0, 60, 10)
    ]


def test_summarize_day_counts_by_feed() -> None:
    rows = [
        _row(feed=Feed.VEHICLE_POSITIONS),
        _row(feed=Feed.VEHICLE_POSITIONS, minute='00:01'),
        _row(feed=Feed.TRIP_UPDATES),
    ]
    summary = summarize_day(rows)
    vp = summary.feed_counts['vehiclepos']
    tu = summary.feed_counts['tripupdates']
    # Window spans 00:00-00:01 (2 minutes): expected scales with it,
    # not with a fixed 8,640/1,440 daily total.
    assert (vp.actual, vp.expected) == (2, 12)
    assert (tu.actual, tu.expected) == (1, 2)


def test_summarize_day_status_counts_by_feed() -> None:
    rows = [
        _row(feed=Feed.VEHICLE_POSITIONS, status_code=HTTPStatus.OK),
        _row(
            feed=Feed.VEHICLE_POSITIONS,
            minute='00:01',
            status_code=HTTPStatus.FORBIDDEN,
        ),
    ]
    summary = summarize_day(rows)
    assert summary.status_counts['vehiclepos'] == {'200': 1, '403': 1}


def test_summarize_day_counts_error_row() -> None:
    """A transport failure (no HTTP response at all) is filed as a
    transport error, not double-booked as a non-200 status too."""
    rows = [
        _row(),
        _row(
            minute='00:01',
            status_code=None,
            body_bytes=0,
            server_date_utc=None,
            skew_s=None,
            error='ConnectionError: timed out',
        ),
    ]
    summary = summarize_day(rows)
    assert summary.failures.transport_error_count == 1
    assert summary.failures.crashed_count == 0
    assert summary.failures.non_200_count == 0
    assert summary.failures.null_server_date_count == 0
    assert len(summary.failures.examples) >= 1
    assert summary.failures.examples[0]['error'] is not None


def test_summarize_day_non_200_counted_once() -> None:
    """A real HTTP failure response is filed as non-200 only."""
    rows = [
        _row(
            minute='00:01',
            status_code=HTTPStatus.FORBIDDEN,
            body_bytes=0,
            server_date_utc=None,
            error='HTTP 403',
        ),
    ]
    summary = summarize_day(rows)
    assert summary.failures.non_200_count == 1
    assert summary.failures.crashed_count == 0
    assert summary.failures.transport_error_count == 0
    assert summary.failures.null_server_date_count == 0


def test_summarize_day_crash_row_counted_once() -> None:
    """A crashed poll's placeholder row lands in exactly one
    failure category, never triple-counted."""
    rows = [
        _row(
            minute='00:01',
            status_code=None,
            body_bytes=0,
            server_date_utc=None,
            skew_s=None,
            rtt_s=0.0,
            error='poll worker crashed unexpectedly',
        ),
    ]
    summary = summarize_day(rows)
    assert summary.failures.crashed_count == 1
    assert summary.failures.transport_error_count == 0
    assert summary.failures.non_200_count == 0
    assert summary.failures.null_server_date_count == 0


def test_status_counts_distinguish_crash_from_transport_error() -> None:
    """`_status_counts` must not file a crash and a transport
    failure under the same ``'None'`` key."""
    rows = [
        _row(
            minute='00:01',
            status_code=None,
            body_bytes=0,
            server_date_utc=None,
            skew_s=None,
            rtt_s=0.0,
            error='poll worker crashed unexpectedly',
        ),
        _row(
            minute='00:02',
            status_code=None,
            body_bytes=0,
            server_date_utc=None,
            skew_s=None,
            error='ConnectionError: timed out',
        ),
    ]
    summary = summarize_day(rows)
    by_status = summary.status_counts['vehiclepos']
    assert by_status.get('crashed') == 1
    assert by_status.get('transport_error') == 1
    assert 'None' not in by_status


def test_summarize_day_timing_stats_skip_null_skew() -> None:
    rows = [
        _row(rtt_s=0.1, skew_s=0.01),
        _row(minute='00:01', rtt_s=0.3, skew_s=None),
        _row(minute='00:02', rtt_s=0.2, skew_s=0.03),
    ]
    summary = summarize_day(rows)
    assert summary.rtt is not None
    assert summary.rtt.minimum == 0.1
    assert summary.rtt.maximum == 0.3
    assert summary.skew is not None
    assert summary.skew.minimum == 0.01
    assert summary.skew.maximum == 0.03


def test_summarize_day_no_skew_when_all_null() -> None:
    rows = [_row(skew_s=None, server_date_utc=None)]
    summary = summarize_day(rows)
    assert summary.skew is None


def test_summarize_day_payload_stats_per_feed() -> None:
    rows = [
        _row(feed=Feed.VEHICLE_POSITIONS, body_bytes=1_000),
        _row(
            feed=Feed.VEHICLE_POSITIONS,
            minute='00:01',
            body_bytes=3_000,
        ),
        _row(feed=Feed.TRIP_UPDATES, body_bytes=500),
    ]
    summary = summarize_day(rows)
    vp_stats = summary.payload_by_feed['vehiclepos']
    assert (vp_stats.minimum, vp_stats.median, vp_stats.maximum) == (
        1_000, 2_000, 3_000,
    )
    assert summary.total_bytes == 4_500


def _full_day_except(*, sparse_minute: str) -> list[RunRecord]:
    """Build a full day of vehiclepos rows, one minute short."""
    rows: list[RunRecord] = []
    for hour in range(24):
        for minute in range(60):
            label = f'{hour:02d}:{minute:02d}'
            if label == sparse_minute:
                rows += [_row(minute=label, second='00')]
            else:
                rows += _full_minute(minute=label)
    return rows


def test_summarize_day_detects_coverage_gap() -> None:
    rows = _full_day_except(sparse_minute='00:01')
    summary = summarize_day(rows)
    short_minutes = {gap.minute: gap.count for gap in summary.coverage.worst}
    assert short_minutes == {'00:01': 1}
    assert summary.coverage.minutes_short == 1


def test_summarize_day_full_minute_has_no_gap_for_that_minute() -> None:
    rows = _full_minute(minute='00:00')
    summary = summarize_day(rows)
    covered = {gap.minute for gap in summary.coverage.worst}
    assert '00:00' not in covered


def test_summarize_day_windows_a_partial_day() -> None:
    """A collector that started mid-day must not report hours of
    pre-deployment silence as coverage gaps."""
    rows: list[RunRecord] = []
    for minute in ('16:06', '16:07', '16:08', '16:09', '16:10'):
        if minute == '16:08':
            rows += [_row(minute=minute, second='00')]
        else:
            rows += _full_minute(minute=minute)
    summary = summarize_day(rows)
    assert summary.window is not None
    assert summary.window.minutes == 5
    short_minutes = {gap.minute: gap.count for gap in summary.coverage.worst}
    assert short_minutes == {'16:08': 1}
    assert summary.coverage.minutes_short == 1
    vp = summary.feed_counts['vehiclepos']
    assert vp.expected == 5 * 6


def _boundary_row(*, second: str) -> RunRecord:
    """Build one vehiclepos row fetched at 00:00:<second> on
    `DATE`, as `fetch_boundary_rows` would return it after
    filtering the previous partition down to rows on `DATE`."""
    fetched_at = f'{DATE}T00:00:{second}+00:00'
    return RunRecord(
        feed=Feed.VEHICLE_POSITIONS.value,
        fetched_at_utc=fetched_at,
        received_at_utc=fetched_at,
        rtt_s=0.2,
        server_date_utc=fetched_at,
        skew_s=0.05,
        status_code=HTTPStatus.OK,
        body_bytes=1_000,
        error=None,
    )


def test_summarize_day_midnight_straddle_not_a_gap() -> None:
    """A full 6 polls split 3-and-3 across midnight is not a gap
    once the neighbouring partition's boundary rows are supplied."""
    day_rows = [
        _row(minute='00:00', second=f'{second:02d}')
        for second in (0, 10, 20)
    ]
    boundary_rows = [
        _boundary_row(second=f'{second:02d}') for second in (30, 40, 50)
    ]
    summary = summarize_day(day_rows, boundary_rows=boundary_rows)
    short = {gap.minute: gap.count for gap in summary.coverage.worst}
    assert '00:00' not in short
    assert summary.coverage.minutes_short == 0
    vp = summary.feed_counts['vehiclepos']
    assert vp.actual == 3
    assert summary.total_bytes == 3_000


def test_summarize_day_midnight_straddle_boundary_error_row_excluded() -> (
    None
):
    """A crashed/errored row stitched in from the boundary partition
    must be filtered the same way as a same-day row: it must not
    paper over a genuine boundary gap."""
    day_rows = [
        _row(minute='00:00', second=f'{second:02d}')
        for second in (0, 10, 20)
    ]
    boundary_rows = [
        _boundary_row(second='30'),
        _boundary_row(second='40'),
        RunRecord(
            feed=Feed.VEHICLE_POSITIONS.value,
            fetched_at_utc=f'{DATE}T00:00:50+00:00',
            received_at_utc=f'{DATE}T00:00:50+00:00',
            rtt_s=0.0,
            server_date_utc=None,
            skew_s=None,
            status_code=None,
            body_bytes=0,
            error='poll worker crashed unexpectedly',
        ),
    ]
    summary = summarize_day(day_rows, boundary_rows=boundary_rows)
    short = {gap.minute: gap.count for gap in summary.coverage.worst}
    assert short == {'00:00': 5}
    assert summary.coverage.minutes_short == 1


def test_summarize_day_midnight_genuine_gap_still_detected() -> None:
    """Only 4 polls total across the midnight boundary is a real
    gap and must still be reported after stitching."""
    day_rows = [
        _row(minute='00:00', second=f'{second:02d}') for second in (0, 10)
    ]
    boundary_rows = [_boundary_row(second='50')]
    summary = summarize_day(day_rows, boundary_rows=boundary_rows)
    short = {gap.minute: gap.count for gap in summary.coverage.worst}
    assert short == {'00:00': 3}
    assert summary.coverage.minutes_short == 1


def test_summarize_day_crash_row_leaves_minute_short() -> None:
    """A crashed poll's placeholder row carries `feed='vehiclepos'`
    but produced no S3 object, so it must not count toward
    coverage: the minute reads short, not complete."""
    rows = _full_minute(minute='00:00')[:-1] + [
        _row(
            minute='00:00',
            second='50',
            status_code=None,
            body_bytes=0,
            server_date_utc=None,
            skew_s=None,
            rtt_s=0.0,
            error='poll worker crashed unexpectedly',
        ),
    ]
    summary = summarize_day(rows)
    short_minutes = {gap.minute: gap.count for gap in summary.coverage.worst}
    assert short_minutes == {'00:00': 5}
    assert summary.coverage.minutes_short == 1


def test_summarize_day_fetch_failure_row_leaves_minute_short() -> None:
    """A row with `error` set but a non-200 `status_code` also did
    not produce a stored object, so it must not count either."""
    rows = _full_minute(minute='00:00')[:-1] + [
        _row(
            minute='00:00',
            second='50',
            status_code=HTTPStatus.FORBIDDEN,
            body_bytes=0,
            server_date_utc=None,
            error='HTTP 403',
        ),
    ]
    summary = summarize_day(rows)
    short_minutes = {gap.minute: gap.count for gap in summary.coverage.worst}
    assert short_minutes == {'00:00': 5}
    assert summary.coverage.minutes_short == 1


def test_summarize_day_six_clean_rows_still_complete() -> None:
    """A minute of 6 error-free rows still reports as complete."""
    rows = _full_minute(minute='00:00')
    summary = summarize_day(rows)
    covered = {gap.minute for gap in summary.coverage.worst}
    assert '00:00' not in covered
    assert summary.coverage.minutes_short == 0


def test_summarize_day_missing_boundary_rows_does_not_error() -> None:
    """No boundary rows supplied (e.g. neighbouring partition is
    missing) behaves exactly like the pre-fix path."""
    rows = _full_minute(minute='00:00')
    summary = summarize_day(rows, boundary_rows=None)
    assert summary.coverage.minutes_short == 0


class _FakeBody:
    """Minimal stand-in for a botocore streaming body."""

    def __init__(self, *, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        """Return the whole body, matching the real client's API."""
        return self._data


class _FakePaginator:
    """Stand-in for a boto3 paginator over a fixed set of pages."""

    def __init__(self, *, pages: list[dict[str, object]]) -> None:
        self._pages = pages

    def paginate(self, **_kwargs: object) -> list[dict[str, object]]:
        """Return the fixed pages, ignoring the given kwargs."""
        return self._pages


class _FakeS3Client:
    """Hand-built S3 client fake covering only what the repository
    calls: listing one partition, and fetching one object."""

    def __init__(
        self,
        *,
        objects_by_date: dict[str, list[tuple[str, datetime]]],
        bodies: dict[str, str],
    ) -> None:
        self._objects_by_date = objects_by_date
        self._bodies = bodies

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        """Return the fake body registered for `Key`."""
        del Bucket
        return {'Body': _FakeBody(data=self._bodies[Key].encode())}


def _repository_with(
    *, objects_by_date: dict[str, list[tuple[str, datetime]]],
    bodies: dict[str, str],
) -> CollectionRunRepository:
    """Build a repository whose S3 client is a hand-built fake.

    Parameters
    ----------
    objects_by_date : dict[str, list[tuple[str, datetime]]]
        Maps ``dt=`` date strings to (key, LastModified) pairs.
    bodies : dict[str, str]
        Maps object keys to their raw JSON Lines content.

    Returns
    -------
    CollectionRunRepository
        A repository backed entirely by fakes, no network.
    """
    repo = CollectionRunRepository.__new__(CollectionRunRepository)
    repo.bucket = 'fake-bucket'

    class _Client(_FakeS3Client):
        def get_paginator(_self, _name: str) -> _FakePaginator:
            def paginate(**kwargs: object) -> list[dict[str, object]]:
                prefix = str(kwargs['Prefix'])
                date = prefix.split('dt=')[1].rstrip('/')
                contents = [
                    {'Key': key, 'LastModified': modified}
                    for key, modified in objects_by_date.get(date, [])
                ]
                return [{'Contents': contents}]
            paginator = _FakePaginator(pages=[])
            paginator.paginate = paginate
            return paginator

    repo.client = _Client(objects_by_date=objects_by_date, bodies=bodies)
    return repo


def test_fetch_boundary_rows_only_reads_the_recent_window() -> None:
    """Objects well before the boundary are listed but never
    fetched, so they cannot inflate the stitched row count."""
    early = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
    late = datetime(2026, 9, 14, 23, 59, 40, tzinfo=UTC)
    repo = _repository_with(
        objects_by_date={
            '2026-09-14': [
                ('midday.jsonl', early),
                ('boundary.jsonl', late),
            ],
        },
        bodies={
            'midday.jsonl': (
                '{"feed": "vehiclepos", '
                '"fetched_at_utc": "2026-09-14T12:00:00+00:00"}\n'
            ),
            'boundary.jsonl': (
                '{"feed": "vehiclepos", '
                '"fetched_at_utc": "2026-09-15T00:00:10+00:00"}\n'
            ),
        },
    )
    rows = repo.fetch_boundary_rows(date='2026-09-15')
    assert [row['fetched_at_utc'] for row in rows] == [
        '2026-09-15T00:00:10+00:00',
    ]


def test_fetch_boundary_rows_missing_partition_returns_empty() -> None:
    """No neighbouring partition (e.g. the first collected day)
    must not raise, just yield nothing to stitch."""
    repo = _repository_with(objects_by_date={}, bodies={})
    assert repo.fetch_boundary_rows(date='2026-09-15') == []


def _memory_row(
    *,
    bin_label: str = '2026-09-15 00:00:00.000',
    max_mb: float = 200.0,
    max_duration_ms: float = 51_000.0,
    invocations: int = 6,
) -> dict[str, str]:
    """Build one raw Logs Insights memory-query result row."""
    return {
        'bin(10m)': bin_label,
        'maxMB': str(max_mb),
        'maxDurationMs': str(max_duration_ms),
        'invocations': str(invocations),
    }


def test_summarize_memory_none_when_no_rows() -> None:
    assert summarize_memory([], memory_size_mb=384) is None


def test_summarize_memory_headroom_arithmetic() -> None:
    rows = [_memory_row(max_mb=200.0)]
    summary = summarize_memory(rows, memory_size_mb=384)
    assert summary is not None
    assert summary.headroom.max_used_mb == 200.0
    assert summary.headroom.headroom_mb == 184.0
    expected_pct = round(184 / 384 * 100, 2)
    assert round(summary.headroom.headroom_pct, 2) == expected_pct


def test_summarize_memory_close_to_ceiling() -> None:
    rows = [_memory_row(max_mb=370.0)]
    summary = summarize_memory(rows, memory_size_mb=384)
    assert summary is not None
    assert summary.headroom.headroom_mb == 14.0
    expected_pct = round(14 / 384 * 100, 2)
    assert round(summary.headroom.headroom_pct, 2) == expected_pct


def test_summarize_memory_takes_max_across_bins() -> None:
    rows = [
        _memory_row(bin_label='2026-09-15 00:00:00.000', max_mb=210.0),
        _memory_row(bin_label='2026-09-15 00:10:00.000', max_mb=290.0),
        _memory_row(bin_label='2026-09-15 00:20:00.000', max_mb=250.0),
    ]
    summary = summarize_memory(rows, memory_size_mb=384)
    assert summary is not None
    assert summary.headroom.max_used_mb == 290.0
    assert summary.max_duration_ms == 51_000.0


def test_summarize_memory_sums_invocations_and_sorts_bins() -> None:
    rows = [
        _memory_row(bin_label='2026-09-15 00:20:00.000', invocations=6),
        _memory_row(bin_label='2026-09-15 00:00:00.000', invocations=6),
        _memory_row(bin_label='2026-09-15 00:10:00.000', invocations=6),
    ]
    summary = summarize_memory(rows, memory_size_mb=384)
    assert summary is not None
    assert summary.total_invocations == 18
    assert [one_bin.label for one_bin in summary.bins] == [
        '00:00', '00:10', '00:20',
    ]


def test_summarize_memory_flags_partial_day() -> None:
    rows = [_memory_row(bin_label='2026-09-15 16:00:00.000')]
    summary = summarize_memory(rows, memory_size_mb=384)
    assert summary is not None
    assert summary.covered_minutes == 10
