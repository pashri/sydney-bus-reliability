"""Tests for pulling the tables the marts read to a local directory."""

import os
from pathlib import Path

import boto3

from scripts.pull_curated import FileState, plan_pull, pull

FACT: str = 'curated/fact_trip/service_date=2026-09-22/data.parquet'
DIM: str = 'curated/dim_route/valid_from=2026-09-22T020011Z/data.parquet'
POSITION: str = (
    'curated/fact_vehicle_position/service_date=2026-09-22/data.parquet'
)


def test_plan_pull_downloads_a_file_missing_locally() -> None:
    """A key with no local copy is fetched."""
    plan = plan_pull(remote={FACT: FileState(size=3, mtime=100)}, local={})
    assert plan.download == [FACT]
    assert not plan.delete


def test_plan_pull_skips_a_file_that_matches() -> None:
    """Same size and same time means the local copy is current."""
    state = FileState(size=3, mtime=100)
    plan = plan_pull(remote={FACT: state}, local={FACT: state})
    assert not plan.download


def test_plan_pull_downloads_a_same_size_rewrite() -> None:
    """A rewrite of equal size is caught by its time."""
    plan = plan_pull(
        remote={FACT: FileState(size=3, mtime=200)},
        local={FACT: FileState(size=3, mtime=100)},
    )
    assert plan.download == [FACT]


def test_plan_pull_deletes_a_file_gone_from_s3() -> None:
    """A renamed or removed key does not linger locally."""
    plan = plan_pull(remote={}, local={DIM: FileState(size=3, mtime=100)})
    assert plan.delete == [DIM]


def put(*, bucket: str, key: str, body: bytes) -> None:
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


def test_pull_copies_the_mart_tables_only(_bucket: str, tmp_path: Path) -> None:
    """Mart inputs arrive; tables the marts do not read stay behind."""
    put(bucket=_bucket, key=FACT, body=b'abc')
    put(bucket=_bucket, key=POSITION, body=b'abc')
    pull(client=boto3.client('s3'), bucket=_bucket, dest=tmp_path)
    assert (tmp_path / FACT).read_bytes() == b'abc'
    assert not (tmp_path / POSITION).exists()


def test_pull_twice_downloads_nothing_new(
    _bucket: str, tmp_path: Path,
) -> None:
    """A second pull over an unchanged bucket fetches nothing."""
    put(bucket=_bucket, key=FACT, body=b'abc')
    client = boto3.client('s3')
    pull(client=client, bucket=_bucket, dest=tmp_path)
    plan = pull(client=client, bucket=_bucket, dest=tmp_path)
    assert not plan.download


def test_pull_refetches_a_same_size_rewrite(
    _bucket: str, tmp_path: Path,
) -> None:
    """A local copy older than the object is replaced."""
    put(bucket=_bucket, key=FACT, body=b'abc')
    client = boto3.client('s3')
    pull(client=client, bucket=_bucket, dest=tmp_path)
    os.utime(tmp_path / FACT, (0, 0))
    put(bucket=_bucket, key=FACT, body=b'xyz')
    pull(client=client, bucket=_bucket, dest=tmp_path)
    assert (tmp_path / FACT).read_bytes() == b'xyz'


def test_pull_removes_a_renamed_snapshot(
    _bucket: str, tmp_path: Path,
) -> None:
    """The old name of a moved key is deleted locally."""
    old = DIM.replace('2026-09-22T020011Z', '2026-09-22')
    put(bucket=_bucket, key=old, body=b'abc')
    client = boto3.client('s3')
    pull(client=client, bucket=_bucket, dest=tmp_path)
    client.delete_object(Bucket=_bucket, Key=old)
    put(bucket=_bucket, key=DIM, body=b'abc')
    pull(client=client, bucket=_bucket, dest=tmp_path)
    assert not (tmp_path / old).exists()
    assert (tmp_path / DIM).exists()


def test_pull_leaves_files_outside_the_synced_tables(
    _bucket: str, tmp_path: Path,
) -> None:
    """A local cache beside the pulled tables survives a pull."""
    cache = tmp_path / 'marts.duckdb'
    cache.write_bytes(b'cache')
    pull(client=boto3.client('s3'), bucket=_bucket, dest=tmp_path)
    assert cache.exists()
