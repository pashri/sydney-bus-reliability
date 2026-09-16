"""Tests for the pure summarising logic in check_collection.py."""

from http import HTTPStatus

from scripts.check_collection import summarize_day, summarize_memory
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
    assert summary.failures.error_count == 1
    assert summary.failures.non_200_count == 1
    assert summary.failures.null_server_date_count == 1
    assert len(summary.failures.examples) >= 1
    assert summary.failures.examples[0]['error'] is not None


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
