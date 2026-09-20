"""Tests for the checker's two sources of collector audit rows."""

import json
import sys
from datetime import date

import boto3
import duckdb
import pytest

from checker.collection import summarize_day
from checker.handler import DUCKDB_LIMITS
from checker.rows import CollectorRunRepository, RowSource
from common.connection import configure
from merger.handler import merge_collector_run

DAY = date(2026, 9, 17)
FEED = 'vehiclepos'


def _row(*, fetched_at: str) -> bytes:
    """Build one audit line as the collector writes it."""
    return json.dumps({
        'feed': FEED,
        'fetched_at_utc': fetched_at,
        'received_at_utc': fetched_at,
        'rtt_s': 0.5,
        'server_date_utc': None,
        'skew_s': None,
        'status_code': 200,
        'body_bytes': 1234,
        'error': None,
    }).encode() + b'\n'


def _put_rows(*, bucket: str, stamps: dict[str, list[str]]) -> None:
    """Write audit lines into their UTC date partitions."""
    client = boto3.client('s3')
    for day, values in stamps.items():
        for index, fetched_at in enumerate(values):
            client.put_object(
                Bucket=bucket,
                Key=f'curated/collector_run/dt={day}/{index}.jsonl',
                Body=_row(fetched_at=fetched_at),
            )


@pytest.fixture(name='stamps')
def _stamps() -> dict[str, list[str]]:
    """Provide timestamps spanning a Sydney day's two UTC partitions."""
    return {
        '2026-09-16': [
            '2026-09-16T13:00:00+00:00',  # before the Sydney day
            '2026-09-16T14:00:30+00:00',  # first minute of it
            '2026-09-16T14:01:00+00:00',
        ],
        '2026-09-17': [
            '2026-09-17T03:30:00+00:00',
            '2026-09-17T13:59:00+00:00',  # last minute of it
            '2026-09-17T14:00:30+00:00',  # after the Sydney day
        ],
    }


def _repository(*, bucket: str, endpoint: str) -> CollectorRunRepository:
    """Build a repository against the local moto server."""
    connection = duckdb.connect()
    configure(
        connection=connection, limits=DUCKDB_LIMITS, endpoint=endpoint,
    )
    return CollectorRunRepository(connection=connection, bucket=bucket)


def test_live_path_cuts_the_sydney_day(
    _bucket: str, _s3_endpoint: str, stamps: dict[str, list[str]],
) -> None:
    """Rows are chosen by fetch time, not by which partition holds
    them, so the two rows outside the Sydney day are dropped."""
    _put_rows(bucket=_bucket, stamps=stamps)
    fetched = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    assert fetched.source is RowSource.LIVE
    assert len(fetched.rows) == 4


def test_live_path_returns_utc_iso_timestamps(
    _bucket: str, _s3_endpoint: str, stamps: dict[str, list[str]],
) -> None:
    """Timestamps come back in the shape ``summarize_day`` parses."""
    _put_rows(bucket=_bucket, stamps=stamps)
    fetched = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    assert fetched.rows[0]['fetched_at_utc'] == (
        '2026-09-16T14:00:30+00:00'
    )


def test_missing_partitions_yield_no_rows(
    _bucket: str, _s3_endpoint: str,
) -> None:
    fetched = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    assert fetched.rows == []
    assert fetched.source is RowSource.LIVE


def test_merged_and_live_paths_agree(
    _bucket: str, _s3_endpoint: str, stamps: dict[str, list[str]],
) -> None:
    """Once the merger has run, the fact table must summarise to
    exactly what the raw JSONL summarises to."""
    _put_rows(bucket=_bucket, stamps=stamps)
    live = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    merge_collector_run(
        bucket=_bucket, service_date=DAY, endpoint=_s3_endpoint,
    )
    merged = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    assert live.source is RowSource.LIVE
    assert merged.source is RowSource.MERGED
    assert merged.rows == live.rows
    assert summarize_day(merged.rows) == summarize_day(live.rows)


@pytest.fixture(name='no_pytz')
def _no_pytz(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``import pytz`` fail, as it does on Lambda.

    Local dev has pytz installed as a dev dependency, so a test run on
    a laptop cannot otherwise tell whether a query would work in
    production. DuckDB imports it only when a TIMESTAMPTZ column is
    fetched into Python, which no static import check can see.
    """
    monkeypatch.setitem(sys.modules, 'pytz', None)


def test_fetching_a_timestamptz_needs_pytz(no_pytz: None) -> None:
    """Guards the guard: without this, the fixture could go stale and
    the tests below would pass for the wrong reason."""
    connection = duckdb.connect()
    with pytest.raises(duckdb.InvalidInputException, match='pytz'):
        connection.execute(
            "SELECT CAST('2026-09-17T03:30:00+00:00' AS TIMESTAMPTZ)",
        ).fetchall()


def test_live_path_reads_without_pytz(
    _bucket: str,
    _s3_endpoint: str,
    stamps: dict[str, list[str]],
    no_pytz: None,
) -> None:
    _put_rows(bucket=_bucket, stamps=stamps)
    fetched = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    assert len(fetched.rows) == 4


def test_merged_path_reads_without_pytz(
    _bucket: str,
    _s3_endpoint: str,
    stamps: dict[str, list[str]],
    no_pytz: None,
) -> None:
    _put_rows(bucket=_bucket, stamps=stamps)
    merge_collector_run(
        bucket=_bucket, service_date=DAY, endpoint=_s3_endpoint,
    )
    fetched = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    assert fetched.source is RowSource.MERGED
    assert len(fetched.rows) == 4
