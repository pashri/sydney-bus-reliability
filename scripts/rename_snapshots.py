"""Rename date-named timetable snapshots to their check time.

Run from the repo root::

    PYTHONPATH=src uv run python -m scripts.rename_snapshots \\
        --profile pashri-admin            # dry run
    PYTHONPATH=src uv run python -m scripts.rename_snapshots \\
        --profile pashri-admin --apply

Snapshots used to be named ``valid_from=YYYY-MM-DD``, the UTC date of
the check. They are now named for the check time. Each date-named
partition, under every dimension and the bundle archive, moves to the
name of the last changed check on that date, which is the one whose
write survived. The ``schedule_check`` records are left as written.

Every move is planned and checked before any object is copied, and an
existing target is never overwritten.
"""

import argparse
import json
import logging
import re
import sys
from collections.abc import Iterable, Iterator
from datetime import datetime
from typing import Any, Final

import boto3

from common.gtfs_static import SNAPSHOT_LABEL_FORMAT

logger = logging.getLogger(__name__)

DEFAULT_BUCKET: Final[str] = 'sydney-bus-reliability-043262084828'
CURATED: Final[str] = 'curated/'
CHECK_PREFIX: Final[str] = 'curated/schedule_check/'
SNAPSHOT_TABLES: Final[tuple[str, ...]] = (
    'curated/dim_', 'curated/schedule_bundle/',
)
DATE_PARTITION: Final[re.Pattern[str]] = re.compile(
    r'valid_from=(\d{4}-\d{2}-\d{2})/',
)


def parse_args() -> argparse.Namespace:
    """Read the command line.

    Returns
    -------
    argparse.Namespace
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile')
    parser.add_argument('--bucket', default=DEFAULT_BUCKET)
    parser.add_argument('--apply', action='store_true',
                        help='move the objects; without it, only report')
    return parser.parse_args()


def plan_renames(
    *,
    checks: Iterable[dict[str, Any]],
    dates: set[str],
) -> dict[str, str]:
    """Name each date partition by the last changed check on that date.

    Parameters
    ----------
    checks : Iterable[dict[str, Any]]
        ``schedule_check`` records.
    dates : set[str]
        Date-named ``valid_from`` values present, ``YYYY-MM-DD``.

    Returns
    -------
    dict[str, str]
        Old value to new value.

    Raises
    ------
    ValueError
        If a date has no changed check to name it by.
    """
    latest: dict[str, datetime] = {}
    for check in checks:
        date = check.get('valid_from')
        if check.get('changed') and date in dates:
            checked_at = datetime.fromisoformat(check['checked_at_utc'])
            latest[date] = max(latest.get(date, checked_at), checked_at)
    missing = sorted(dates - set(latest))
    if missing:
        raise ValueError(f'no changed check names {", ".join(missing)}')
    return {
        date: f'{checked_at:{SNAPSHOT_LABEL_FORMAT}}'
        for date, checked_at in latest.items()
    }


def list_keys(*, client: Any, bucket: str, prefix: str) -> Iterator[str]:
    """List every key under a prefix.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket to list.
    prefix : str
        Key prefix.

    Yields
    ------
    str
        One key.
    """
    pages = client.get_paginator('list_objects_v2').paginate(
        Bucket=bucket, Prefix=prefix,
    )
    for page in pages:
        yield from (item['Key'] for item in page.get('Contents', ()))


def snapshot_keys(*, client: Any, bucket: str) -> list[str]:
    """List every object of every dimension and of the bundle archive.

    Only those tables are listed. The rest of the curated layer holds
    far more objects and no snapshots.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.

    Returns
    -------
    list[str]
        Keys under ``curated/dim_*/`` and ``curated/schedule_bundle/``.
    """
    pages = client.get_paginator('list_objects_v2').paginate(
        Bucket=bucket, Prefix=CURATED, Delimiter='/',
    )
    tables = [
        common['Prefix']
        for page in pages for common in page.get('CommonPrefixes', ())
        if common['Prefix'].startswith(SNAPSHOT_TABLES)
    ]
    return [
        key for table in tables
        for key in list_keys(client=client, bucket=bucket, prefix=table)
    ]


def read_checks(*, client: Any, bucket: str) -> list[dict[str, Any]]:
    """Read every ``schedule_check`` record.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.

    Returns
    -------
    list[dict[str, Any]]
        Every record, in no particular order.
    """
    bodies = (
        client.get_object(Bucket=bucket, Key=key)['Body'].read().decode()
        for key in list_keys(client=client, bucket=bucket, prefix=CHECK_PREFIX)
    )
    return [
        json.loads(line)
        for body in bodies for line in body.splitlines() if line
    ]


def planned_moves(
    *,
    client: Any,
    bucket: str,
    plan: dict[str, str],
) -> list[tuple[str, str]]:
    """List the source and target key of every object to move.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.
    plan : dict[str, str]
        Old ``valid_from`` value to new.

    Returns
    -------
    list[tuple[str, str]]
        Source and target keys.
    """
    moves: list[tuple[str, str]] = []
    for key in snapshot_keys(client=client, bucket=bucket):
        match = DATE_PARTITION.search(key)
        if match and match.group(1) in plan:
            moves.append((key, key.replace(
                match.group(0), f'valid_from={plan[match.group(1)]}/', 1,
            )))
    return moves


def apply_plan(
    *,
    client: Any,
    bucket: str,
    plan: dict[str, str],
    apply: bool,
) -> int:
    """Move every date-named snapshot object to its check-time name.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.
    plan : dict[str, str]
        Old ``valid_from`` value to new, from ``plan_renames``.
    apply : bool
        Move the objects when true; otherwise only report them.

    Returns
    -------
    int
        Objects moved, or that would be moved.

    Raises
    ------
    FileExistsError
        If any target key already exists. Nothing is moved.
    """
    moves = planned_moves(client=client, bucket=bucket, plan=plan)
    existing = set(snapshot_keys(client=client, bucket=bucket))
    clashes = [target for _, target in moves if target in existing]
    if clashes:
        raise FileExistsError(f'targets already exist: {clashes}')
    for source, target in moves:
        logger.info('%s %s -> %s', 'move' if apply else 'would move',
                    source, target)
        if apply:
            client.copy_object(
                Bucket=bucket, Key=target,
                CopySource={'Bucket': bucket, 'Key': source},
            )
            client.delete_object(Bucket=bucket, Key=source)
    return len(moves)


def main() -> int:
    """Plan and, with ``--apply``, perform the rename.

    Returns
    -------
    int
        0 on success.
    """
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    args = parse_args()
    client = boto3.Session(profile_name=args.profile).client('s3')
    dates = {
        match.group(1)
        for key in snapshot_keys(client=client, bucket=args.bucket)
        if (match := DATE_PARTITION.search(key))
    }
    plan = plan_renames(
        checks=read_checks(client=client, bucket=args.bucket), dates=dates,
    )
    logger.info('plan: %s', plan)
    moved = apply_plan(
        client=client, bucket=args.bucket, plan=plan, apply=args.apply,
    )
    logger.info('%s %d objects', 'moved' if args.apply else 'would move',
                moved)
    return 0


if __name__ == '__main__':
    sys.exit(main())
