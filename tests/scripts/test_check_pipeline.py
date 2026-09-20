"""Tests for the checker's local renderer."""

import argparse
from typing import Any

import pytest

from scripts.check_pipeline import (
    build_event,
    fmt_bytes,
    fmt_duration,
    fmt_timing,
    fmt_window,
    print_report,
)


def _args(**overrides: Any) -> argparse.Namespace:
    """Build a parsed-argument namespace with defaults."""
    values: dict[str, Any] = {
        'date': None,
        'collection_days': None,
        'curation_days': None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _response() -> dict[str, Any]:
    """Build one checker response covering both halves."""
    return {
        'schema_version': 1,
        'requested': {
            'date': '2026-09-17',
            'collection_days': 1,
            'curation_days': 1,
        },
        'collection': {'days': [{
            'date': '2026-09-17',
            'source': 'live',
            'window': {
                'start': '2026-09-16T14:00:30+00:00',
                'end': '2026-09-17T13:59:00+00:00',
                'minutes': 1439,
            },
            'feed_counts': {'vehiclepos': {
                'actual': 8610, 'expected': 8634,
            }},
            'status_counts': {'vehiclepos': {'200': 8610}},
            'failures': {
                'crashed_count': 2,
                'transport_error_count': 0,
                'non_200_count': 6,
                'null_server_date_count': 0,
                'examples': [],
            },
            'rtt': {'minimum': 0.08, 'median': 0.21, 'maximum': 3.9},
            'skew': None,
            'payload_by_feed': {'vehiclepos': {
                'minimum': 180422, 'median': 241880.0, 'maximum': 402118,
            }},
            'total_bytes': 2148301882,
            'coverage': {
                'minutes_short': 1,
                'worst': [{'minute': '03:14', 'count': 2}],
            },
        }]},
        'curation': {'days': [{
            'day': '2026-09-17',
            'hours_in_day': 24,
            'jobs': {'compactor': {
                'runs_expected': 24,
                'runs_seen': 23,
                'missing_partitions': ['2026-09-17T05'],
                'totals': {
                    'objects_expected': 2880,
                    'objects_read': 2874,
                    'rows_in': 41207,
                    'rows_out': 41190,
                    'dupes_collapsed': 17,
                    'dupes_differing_position': 2,
                    'unjoined_route_ids': 0,
                    'unjoined_trip_ids': 122,
                    'unjoined_stop_ids': 0,
                },
                'peak_rss_mb': 412,
                'errors': 0,
            }},
            'schedule': {
                'checks_expected': 1, 'checks_seen': 1, 'changed': True,
            },
        }]},
    }


def test_build_event_omits_unset_arguments() -> None:
    assert build_event(args=_args()) == {}


def test_build_event_passes_what_was_set() -> None:
    assert build_event(args=_args(date='2026-09-17', curation_days=7)) == {
        'date': '2026-09-17', 'curation_days': 7,
    }


@pytest.mark.parametrize(('value', 'expected'), [
    (512, '512 B'),
    (2_400, '2.4 KB'),
    (2_400_000, '2.4 MB'),
    (2_400_000_000, '2.4 GB'),
])
def test_fmt_bytes_picks_a_readable_unit(
    value: int, expected: str,
) -> None:
    assert fmt_bytes(value) == expected


def test_fmt_duration_pads_the_minutes() -> None:
    assert fmt_duration(minutes=248) == '4h 08m'


def test_fmt_window_without_rows_says_so() -> None:
    assert fmt_window(None) == 'no rows observed'


def test_fmt_timing_without_measurements_says_so() -> None:
    assert fmt_timing(None) == 'not measured'


def test_print_report_shows_both_halves(
    capsys: pytest.CaptureFixture[str],
) -> None:
    print_report(response=_response())
    out = capsys.readouterr().out
    assert '8,610 polls of 8,634 expected' in out
    assert '2 crashed, 6 non-200' in out
    assert '23/24 runs' in out


def test_print_report_flags_a_missing_run(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A skipped compactor hour is the point of the curation half."""
    print_report(response=_response())
    assert 'MISSING 2026-09-17T05' in capsys.readouterr().out


def test_print_report_warns_on_an_unknown_schema(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The renderer and the Lambda are deployed separately."""
    response = _response()
    response['schema_version'] = 99
    print_report(response=response)
    assert 'schema 99' in capsys.readouterr().err
