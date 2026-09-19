"""Tests for the curation audit repository."""

import json
from datetime import UTC, datetime

import boto3

from common.curation import CurationRepository, curation_key
from common.types_ import CurationJob, CurationRecord

STARTED: datetime = datetime(2026, 9, 17, 22, 10, tzinfo=UTC)


def make_record() -> CurationRecord:
    """Build a representative audit record.

    Returns
    -------
    CurationRecord
        One compactor run over a single UTC hour.
    """
    return CurationRecord(
        job=CurationJob.COMPACTOR.value,
        invocation_id='abc-123',
        started_at_utc=STARTED.isoformat(),
        finished_at_utc=STARTED.isoformat(),
        partition='2026-09-17T21',
        objects_expected=360,
        objects_read=360,
        rows_in=1_086_002,
        rows_out=1_085_476,
        dupes_collapsed=305_318,
        dupes_differing_position=526,
        unjoined_route_ids=8,
        unjoined_trip_ids=13,
        unjoined_stop_ids=3,
        peak_rss_mb=692,
        error=None,
    )


def test_curation_key_partitions_by_date() -> None:
    """The key carries the UTC date and the invocation id."""
    assert curation_key(
        started_at=STARTED, invocation_id='abc-123',
    ) == 'curated/curation_run/dt=2026-09-17/abc-123.jsonl'


def test_put_record_writes_one_line(_bucket: str) -> None:
    """The record round-trips as a single JSON Lines object."""
    repository = CurationRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    key = repository.put_record(record=make_record())
    body = boto3.client('s3').get_object(
        Bucket=_bucket, Key=key,
    )['Body'].read().decode()
    assert body.count('\n') == 1
    assert '"dupes_differing_position": 526' in body


def test_put_record_round_trips_every_field(_bucket: str) -> None:
    """Every field survives the write, not just the ones spot-checked.

    The record exists to carry counters to the compactor and merger,
    so a silently dropped field would surface there as a mystery
    rather than here as a failure.
    """
    record = make_record()
    repository = CurationRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    key = repository.put_record(record=record)
    body = boto3.client('s3').get_object(
        Bucket=_bucket, Key=key,
    )['Body'].read().decode()
    assert json.loads(body) == record


def test_put_record_writes_to_the_derived_key(_bucket: str) -> None:
    """The returned key is the one curation_key derives."""
    record = make_record()
    repository = CurationRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    assert repository.put_record(record=record) == curation_key(
        started_at=STARTED, invocation_id=record['invocation_id'],
    )


def test_put_record_preserves_a_failure(_bucket: str) -> None:
    """A run that failed round-trips its error text.

    The failure path is what an audit table is for, and it is the
    field that varies most in production.
    """
    record = make_record()
    record['error'] = 'no raw objects for hour 2026-09-17T21'
    record['rows_out'] = 0
    repository = CurationRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    key = repository.put_record(record=record)
    body = boto3.client('s3').get_object(
        Bucket=_bucket, Key=key,
    )['Body'].read().decode()
    assert json.loads(body)['error'] == (
        'no raw objects for hour 2026-09-17T21'
    )
