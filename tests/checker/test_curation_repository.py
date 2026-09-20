"""Tests for reading curation audit rows through DuckDB."""

import json
from datetime import date
from typing import Any

import boto3
import duckdb
import pytest

from checker.curation import CurationRepository, expected_hour_partitions
from checker.handler import DUCKDB_LIMITS
from common.connection import configure
from common.types_ import CurationJob

DAY = date(2026, 9, 17)


def _curation_row(*, job: str, partition: str, **overrides: Any) -> bytes:
    """Build one curation audit line as a Lambda writes it."""
    row: dict[str, Any] = {
        'job': job,
        'invocation_id': f'id-{partition}',
        'started_at_utc': '2026-09-17T01:10:00+00:00',
        'finished_at_utc': '2026-09-17T01:10:30+00:00',
        'partition': partition,
        'objects_expected': 120,
        'objects_read': 120,
        'rows_in': 100,
        'rows_out': 100,
        'dupes_collapsed': 0,
        'dupes_differing_position': 0,
        'unjoined_route_ids': 0,
        'unjoined_trip_ids': 0,
        'unjoined_stop_ids': 0,
        'peak_rss_mb': 300,
        'error': None,
    }
    row.update(overrides)
    return json.dumps(row).encode() + b'\n'


def _put(*, bucket: str, key: str, body: bytes) -> None:
    """Write one object into the bucket."""
    boto3.client('s3').put_object(Bucket=bucket, Key=key, Body=body)


def _repository(*, bucket: str, endpoint: str) -> CurationRepository:
    """Build a repository against the local moto server."""
    connection = duckdb.connect()
    configure(
        connection=connection, limits=DUCKDB_LIMITS, endpoint=endpoint,
    )
    return CurationRepository(connection=connection, bucket=bucket)


@pytest.fixture(name='compactor_day')
def _compactor_day(_bucket: str) -> None:
    """Write a full day of compactor records, bar one hour."""
    for index, partition in enumerate(
        expected_hour_partitions(day=DAY)[:-1],
    ):
        _put(
            bucket=_bucket,
            key=f'curated/curation_run/dt=2026-09-17/{index}.jsonl',
            body=_curation_row(
                job=CurationJob.COMPACTOR.value, partition=partition,
            ),
        )


def test_fetch_day_with_no_records_reports_every_run_missing(
    _bucket: str, _s3_endpoint: str,
) -> None:
    summary = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    compactor = summary.jobs[CurationJob.COMPACTOR.value]
    assert compactor.runs_seen == 0
    assert len(compactor.missing_partitions) == 24


def test_fetch_day_finds_the_hour_that_never_ran(
    _bucket: str, _s3_endpoint: str, compactor_day: None,
) -> None:
    """The gap an invocation-count alarm cannot see."""
    summary = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    compactor = summary.jobs[CurationJob.COMPACTOR.value]
    assert compactor.runs_seen == 23
    assert compactor.missing_partitions == ['2026-09-17T13']


def test_fetch_day_totals_the_counters(
    _bucket: str, _s3_endpoint: str, compactor_day: None,
) -> None:
    summary = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    totals = summary.jobs[CurationJob.COMPACTOR.value].totals
    assert totals['rows_in'] == 2300
    assert totals['objects_read'] == 2760


def test_fetch_day_reads_a_schedule_check(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """Checks are filed by UTC date but counted by Sydney date."""
    _put(
        bucket=_bucket,
        key='curated/schedule_check/dt=2026-09-17/010000-abc.jsonl',
        body=json.dumps({
            'checked_at_utc': '2026-09-17T01:00:00+00:00',
            'zip_sha256': 'abc',
            'zip_filename': 'gtfs.zip',
            'changed': True,
            'valid_from': '2026-09-17',
        }).encode() + b'\n',
    )
    summary = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    assert summary.schedule.checks_seen == 1
    assert summary.schedule.changed is True


def test_fetch_day_ignores_a_check_from_another_sydney_day(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """14:00 UTC is already the next day in Sydney."""
    _put(
        bucket=_bucket,
        key='curated/schedule_check/dt=2026-09-17/140000-abc.jsonl',
        body=json.dumps({
            'checked_at_utc': '2026-09-17T14:00:00+00:00',
            'zip_sha256': 'abc',
            'zip_filename': 'gtfs.zip',
            'changed': True,
            'valid_from': '2026-09-17',
        }).encode() + b'\n',
    )
    summary = _repository(
        bucket=_bucket, endpoint=_s3_endpoint,
    ).fetch_day(day=DAY)
    assert summary.schedule.checks_seen == 0
