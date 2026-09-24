"""Copy the tables the marts read from S3 to a local directory.

Run from the repo root::

    uv run python -m scripts.pull_curated build/data \\
        --profile pashri-readonly

The destination mirrors the bucket's layout, so ``DEST/curated/...``
and ``DEST/reference/...`` hold the same keys as S3. Only the tables
listed in ``PREFIXES`` are copied.

The merger rewrites a day's files after the day, often at the same
size, so a file is fetched again whenever its size or modification time
differs from the object's. A downloaded file takes the object's
``LastModified`` as its modification time. A local file under a pulled
table whose key no longer exists in S3 is deleted, because snapshots
are sometimes renamed. Files outside the pulled tables are left alone.
"""

import argparse
import logging
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import boto3

logger = logging.getLogger(__name__)

DEFAULT_BUCKET: Final[str] = 'sydney-bus-reliability-043262084828'
PREFIXES: Final[tuple[str, ...]] = (
    'curated/fact_trip_stop/',
    'curated/fact_trip/',
    'curated/fact_collector_run/',
    'curated/dim_agency/',
    'curated/dim_route/',
    'curated/dim_trip/',
    'curated/dim_stop/',
    'curated/dim_scheduled_stop_time/',
    'curated/dim_calendar/',
    'curated/dim_calendar_dates/',
    'reference/stop_geography/',
    'reference/stop_meshblock/',
)
"""Every table the marts read, as key prefixes ending in a slash."""


@dataclass(frozen=True)
class FileState:
    """What is compared to decide whether a file is current.

    Attributes
    ----------
    size : int
        Size in bytes.
    mtime : int
        Modification time, whole seconds since the epoch.
    """

    size: int
    mtime: int


@dataclass(frozen=True)
class PullPlan:
    """What one pull changes.

    Attributes
    ----------
    download : list[str]
        Keys to fetch.
    delete : list[str]
        Keys whose local copy is removed.
    """

    download: list[str]
    delete: list[str]


def plan_pull(
    *,
    remote: dict[str, FileState],
    local: dict[str, FileState],
) -> PullPlan:
    """Compare the bucket with the local copy.

    Parameters
    ----------
    remote : dict[str, FileState]
        Key to state, for every object under the pulled tables.
    local : dict[str, FileState]
        Key to state, for every local file under the pulled tables.

    Returns
    -------
    PullPlan
        Keys missing or different locally, and local keys gone from S3.
    """
    return PullPlan(
        download=sorted(
            key for key, state in remote.items() if local.get(key) != state
        ),
        delete=sorted(set(local) - set(remote)),
    )


def list_remote(*, client: Any, bucket: str) -> dict[str, FileState]:
    """List every object under the pulled tables.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket to list.

    Returns
    -------
    dict[str, FileState]
        Key to size and ``LastModified``.
    """
    paginator = client.get_paginator('list_objects_v2')
    return {
        item['Key']: FileState(
            size=item['Size'], mtime=int(item['LastModified'].timestamp()),
        )
        for prefix in PREFIXES
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get('Contents', ())
    }


def local_files(*, dest: Path) -> Iterator[Path]:
    """Yield every local file under the pulled tables.

    Parameters
    ----------
    dest : Path
        Root of the local copy.

    Yields
    ------
    Path
        One file.
    """
    for prefix in PREFIXES:
        table = dest / prefix
        if table.is_dir():
            yield from (path for path in table.rglob('*') if path.is_file())


def list_local(*, dest: Path) -> dict[str, FileState]:
    """List every local file under the pulled tables.

    Parameters
    ----------
    dest : Path
        Root of the local copy.

    Returns
    -------
    dict[str, FileState]
        Key to size and modification time.
    """
    return {
        path.relative_to(dest).as_posix(): FileState(
            size=path.stat().st_size, mtime=int(path.stat().st_mtime),
        )
        for path in local_files(dest=dest)
    }


def pull(*, client: Any, bucket: str, dest: Path) -> PullPlan:
    """Bring the local copy level with the bucket.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket to copy from.
    dest : Path
        Root of the local copy. Created if missing.

    Returns
    -------
    PullPlan
        What was fetched and deleted.
    """
    remote = list_remote(client=client, bucket=bucket)
    plan = plan_pull(remote=remote, local=list_local(dest=dest))
    for key in plan.download:
        target = dest / key
        target.parent.mkdir(parents=True, exist_ok=True)
        logger.info('Fetching %s', key)
        client.download_file(Bucket=bucket, Key=key, Filename=str(target))
        os.utime(target, (remote[key].mtime, remote[key].mtime))
    for key in plan.delete:
        logger.info('Deleting %s', key)
        (dest / key).unlink()
    return plan


def parse_args(*, argv: list[str] | None = None) -> argparse.Namespace:
    """Read the command line.

    Parameters
    ----------
    argv : list[str] | None
        Arguments, or None for ``sys.argv``.

    Returns
    -------
    argparse.Namespace
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dest', type=Path, help='root of the local copy')
    parser.add_argument('--profile')
    parser.add_argument('--bucket', default=DEFAULT_BUCKET)
    return parser.parse_args(argv)


def main(*, argv: list[str] | None = None) -> int:
    """Pull the mart tables.

    Parameters
    ----------
    argv : list[str] | None
        Arguments, or None for ``sys.argv``.

    Returns
    -------
    int
        Process exit status.
    """
    args = parse_args(argv=argv)
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    client = boto3.Session(profile_name=args.profile).client('s3')
    plan = pull(client=client, bucket=args.bucket, dest=args.dest)
    logger.info(
        'Fetched %d, deleted %d', len(plan.download), len(plan.delete),
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
