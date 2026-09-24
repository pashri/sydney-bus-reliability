"""Tests for renaming date-named timetable snapshots."""

import json

import boto3
import pytest

from scripts.rename_snapshots import apply_plan, plan_renames

CHECKS: list[dict[str, object]] = [
    {'checked_at_utc': '2026-09-19T17:34:55.1+00:00', 'changed': True,
     'valid_from': '2026-09-19'},
    {'checked_at_utc': '2026-09-20T02:00:11.2+00:00', 'changed': False,
     'valid_from': None},
    {'checked_at_utc': '2026-09-23T02:00:11.3+00:00', 'changed': True,
     'valid_from': '2026-09-23'},
    {'checked_at_utc': '2026-09-23T23:09:11.9+00:00', 'changed': True,
     'valid_from': '2026-09-23'},
]


def test_plan_renames_names_each_date_by_its_last_changed_check() -> None:
    """The surviving snapshot on a date is the last one written to it."""
    assert plan_renames(
        checks=CHECKS, dates={'2026-09-19', '2026-09-23'},
    ) == {
        '2026-09-19': '2026-09-19T173455Z',
        '2026-09-23': '2026-09-23T230911Z',
    }


def test_plan_renames_refuses_a_date_with_no_changed_check() -> None:
    """A partition no check accounts for is not guessed at."""
    with pytest.raises(ValueError, match='2026-09-21'):
        plan_renames(checks=CHECKS, dates={'2026-09-21'})


def put(*, bucket: str, key: str, body: bytes = b'x') -> None:
    """Store one object.

    Parameters
    ----------
    bucket : str
        Destination bucket.
    key : str
        Object key.
    body : bytes
        Object content.
    """
    boto3.client('s3').put_object(Bucket=bucket, Key=key, Body=body)


def keys(*, bucket: str) -> set[str]:
    """List every key in a bucket.

    Parameters
    ----------
    bucket : str
        Bucket to list.

    Returns
    -------
    set[str]
        All keys.
    """
    listed = boto3.client('s3').list_objects_v2(Bucket=bucket)
    return {item['Key'] for item in listed.get('Contents', ())}


def test_apply_plan_moves_every_table_and_keeps_the_bytes(
    _bucket: str,
) -> None:
    """Each dimension and the archived zip move to the check-time name."""
    put(bucket=_bucket,
        key='curated/dim_trip/valid_from=2026-09-23/data.parquet',
        body=b'trips')
    put(bucket=_bucket,
        key='curated/schedule_bundle/valid_from=2026-09-23/b.zip')
    put(bucket=_bucket,
        key='curated/schedule_check/dt=2026-09-23/1.jsonl',
        body=json.dumps(CHECKS[2]).encode())
    moved = apply_plan(
        client=boto3.client('s3'), bucket=_bucket,
        plan={'2026-09-23': '2026-09-23T230911Z'}, apply=True,
    )
    assert moved == 2
    assert keys(bucket=_bucket) == {
        'curated/dim_trip/valid_from=2026-09-23T230911Z/data.parquet',
        'curated/schedule_bundle/valid_from=2026-09-23T230911Z/b.zip',
        'curated/schedule_check/dt=2026-09-23/1.jsonl',
    }
    body = boto3.client('s3').get_object(
        Bucket=_bucket,
        Key='curated/dim_trip/valid_from=2026-09-23T230911Z/data.parquet',
    )['Body'].read()
    assert body == b'trips'


def test_apply_plan_dry_run_changes_nothing(_bucket: str) -> None:
    """Without apply, the plan is only reported."""
    put(bucket=_bucket,
        key='curated/dim_trip/valid_from=2026-09-23/data.parquet')
    moved = apply_plan(
        client=boto3.client('s3'), bucket=_bucket,
        plan={'2026-09-23': '2026-09-23T230911Z'}, apply=False,
    )
    assert moved == 1
    assert keys(bucket=_bucket) == {
        'curated/dim_trip/valid_from=2026-09-23/data.parquet',
    }


def test_apply_plan_refuses_to_overwrite_an_existing_target(
    _bucket: str,
) -> None:
    """A check-time snapshot already present is never replaced."""
    put(bucket=_bucket,
        key='curated/dim_trip/valid_from=2026-09-23/data.parquet')
    put(bucket=_bucket,
        key='curated/dim_trip/valid_from=2026-09-23T230911Z/data.parquet')
    with pytest.raises(FileExistsError):
        apply_plan(
            client=boto3.client('s3'), bucket=_bucket,
            plan={'2026-09-23': '2026-09-23T230911Z'}, apply=True,
        )
