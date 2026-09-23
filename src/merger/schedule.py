"""Choosing the timetable snapshot a service day is merged against."""

from datetime import date
from typing import Any, Final

import boto3
from aws_lambda_powertools import Logger

logger = Logger()


DIM_SCHEDULED_STOP_TIME_PREFIX: Final[str] = (
    'curated/dim_scheduled_stop_time/'
)


def valid_from_for(
    *,
    client: Any,
    bucket: str,
    service_date: date,
) -> str | None:
    """Find the schedule snapshot to use for a service date.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The service date whose schedule to resolve.

    Returns
    -------
    str | None
        The latest ``valid_from`` partition value at or before
        ``service_date``. For a day before the first snapshot, the
        earliest one, with a warning: collection began before the
        timetable was first captured. None when no snapshot exists.
    """
    pages = client.get_paginator('list_objects_v2').paginate(
        Bucket=bucket,
        Prefix=DIM_SCHEDULED_STOP_TIME_PREFIX,
        Delimiter='/',
    )
    candidates = (
        prefix['Prefix'].removeprefix(
            DIM_SCHEDULED_STOP_TIME_PREFIX,
        ).removeprefix('valid_from=').rstrip('/')
        for page in pages
        for prefix in page.get('CommonPrefixes', ())
    )
    snapshots: list[str] = sorted(candidates)
    eligible = [
        value for value in snapshots
        if date.fromisoformat(value) <= service_date
    ]
    if eligible:
        return eligible[-1]
    if not snapshots:
        return None
    logger.warning(
        'No snapshot in effect, using the earliest',
        extra={'valid_from': snapshots[0]},
    )
    return snapshots[0]


def resolve_dim_source(
    *,
    bucket: str,
    service_date: date,
    session: boto3.Session,
) -> str | None:
    """Locate the schedule snapshot in effect for one service date.

    Lists S3 prefixes only. No dimension rows are fetched into
    Python, so no ``TIMESTAMPTZ`` value crosses the DuckDB boundary.
    The join happens entirely in SQL, in ``build_trip_stop_query``.

    Parameters
    ----------
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The Sydney service date being assembled.
    session : boto3.Session
        Session used to list dimension snapshots.

    Returns
    -------
    str | None
        S3 path to the snapshot's Parquet object, chosen by
        ``valid_from_for``, or None when no snapshot exists.
    """
    valid_from = valid_from_for(
        client=session.client('s3'), bucket=bucket,
        service_date=service_date,
    )
    if valid_from is None:
        return None
    return (
        f's3://{bucket}/{DIM_SCHEDULED_STOP_TIME_PREFIX}'
        f'valid_from={valid_from}/data.parquet'
    )
