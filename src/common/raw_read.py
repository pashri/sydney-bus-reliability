"""Reading stored raw feed objects for one UTC hour.

Objects are yielded one at a time and in key order. Key order is
chronological order, so a latest-wins reduction is correct without any
sorting.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

import boto3
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from src.common.types_ import Feed

logger = Logger()

EXPECTED_OBJECTS: Final[dict[Feed, int]] = {
    Feed.VEHICLE_POSITIONS: 360,
    Feed.TRIP_UPDATES: 60,
}
"""Objects a complete hour should contain, at the deployed cadence.

Six vehicle-position polls plus one trip-updates poll per minute. A
real hour may hold fewer, since a failed poll leaves a gap that is
never backfilled.
"""


@dataclass(frozen=True, slots=True)
class RawObject:
    """One stored raw feed payload."""

    key: str
    fetched_at: datetime
    payload: bytes


def hour_prefix(*, feed: Feed, hour: datetime) -> str:
    """Build the S3 prefix for one feed-hour.

    Parameters
    ----------
    feed : Feed
        The feed to read.
    hour : datetime
        Any UTC instant inside the hour.

    Returns
    -------
    str
        S3 prefix ending in a slash.
    """
    return (
        f'raw/{feed.value}/dt={hour:%Y-%m-%d}/hour={hour:%H}/'
    )


def fetched_at_from_key(*, key: str) -> datetime:
    """Recover the actual fetch time encoded in an object key.

    The inverse of ``raw_key`` in ``src.common.storage``: that
    function builds ``raw/<feed>/dt=YYYY-MM-DD/hour=HH/HHMMSS.pb.gz``
    from a UTC ``fetched_at``, and this reads it back.

    Parameters
    ----------
    key : str
        Full S3 key.

    Returns
    -------
    datetime
        Timezone-aware UTC instant, to the second.

    Raises
    ------
    ValueError
        If ``key`` does not have the expected shape. The message
        names the offending key.
    """
    try:
        parts = key.split('/')
        day = parts[2].removeprefix('dt=')
        stamp = parts[4].removesuffix('.pb.gz')
        return datetime.strptime(
            f'{day} {stamp}', '%Y-%m-%d %H%M%S',
        ).replace(tzinfo=UTC)
    except (IndexError, ValueError) as error:
        raise ValueError(
            f'malformed raw object key: {key!r}',
        ) from error


class RawReader:
    """Streams raw feed objects out of one S3 bucket.

    Parameters
    ----------
    bucket : str
        S3 bucket name.
    session : boto3.Session | None
        Optional boto3 session. Defaults to a new session.
    """

    def __init__(
        self,
        *,
        bucket: str,
        session: boto3.Session | None = None,
    ) -> None:
        session = session or boto3.Session()
        self.bucket = bucket
        self.client = session.client('s3')

    def list_hour(self, *, feed: Feed, hour: datetime) -> list[str]:
        """List every object key for one feed-hour, in order.

        Parameters
        ----------
        feed : Feed
            The feed to read.
        hour : datetime
            Any UTC instant inside the hour.

        Returns
        -------
        list[str]
            Sorted keys. Sorting by key sorts by time, because keys
            are ``HHMMSS`` within a fixed prefix.
        """
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=self.bucket,
            Prefix=hour_prefix(feed=feed, hour=hour),
        )
        return sorted(
            item['Key']
            for page in pages
            for item in page.get('Contents', ())
        )

    def stream_hour(
        self,
        *,
        feed: Feed,
        hour: datetime,
    ) -> Iterator[RawObject]:
        """Yield each object for one feed-hour, oldest first.

        Parameters
        ----------
        feed : Feed
            The feed to read.
        hour : datetime
            Any UTC instant inside the hour.

        Yields
        ------
        RawObject
            One stored payload, still gzipped.
        """
        for key in self.list_hour(feed=feed, hour=hour):
            yield RawObject(
                key=key,
                fetched_at=fetched_at_from_key(key=key),
                payload=self._get_payload(key=key, hour=hour),
            )

    def _get_payload(self, *, key: str, hour: datetime) -> bytes:
        """Fetch one object's body, naming it on failure.

        Parameters
        ----------
        key : str
            Full S3 key to fetch.
        hour : datetime
            The feed-hour ``key`` was listed under, for context.

        Returns
        -------
        bytes
            The object body, still gzipped.

        Raises
        ------
        ClientError
            If the object cannot be fetched. The key and feed-hour
            are logged first. ``raw/`` objects expire, so a listed
            key can age out before this GET, and a backfill can
            target an hour that has gone entirely.
        """
        try:
            return self.client.get_object(
                Bucket=self.bucket, Key=key,
            )['Body'].read()
        except ClientError:
            logger.exception(
                'Failed to fetch raw object',
                extra={'key': key, 'hour': hour.isoformat()},
            )
            raise
