"""Tests for reading one UTC hour of raw feed objects."""

import gzip
from datetime import UTC, datetime
from typing import Final

import boto3
import pytest
from botocore.exceptions import ClientError

from src.common.raw_read import (
    EXPECTED_OBJECTS,
    RawReader,
    fetched_at_from_key,
)
from src.common.types_ import Feed

HOUR: datetime = datetime(2026, 9, 16, 21, 0, tzinfo=UTC)

POLLS_PER_MINUTE_VEHICLE: Final[int] = 6
POLLS_PER_MINUTE_TRIP_UPDATES: Final[int] = 1
MINUTES_PER_HOUR: Final[int] = 60


def test_fetched_at_from_key_reads_second_precision() -> None:
    """Keys are second-precision and derived from actual fetch time."""
    key = 'raw/vehiclepos/dt=2026-09-16/hour=21/210034.pb.gz'
    assert fetched_at_from_key(key=key) == datetime(
        2026, 9, 16, 21, 0, 34, tzinfo=UTC,
    )


def test_fetched_at_from_key_names_malformed_key() -> None:
    """A malformed key raises ``ValueError`` naming itself.

    An unattended job that dies on a bare ``IndexError`` tells you
    nothing about which object broke it.
    """
    key = 'raw/vehiclepos/not-a-key.pb.gz'
    with pytest.raises(ValueError, match='not-a-key.pb.gz'):
        fetched_at_from_key(key=key)


def test_expected_objects_matches_deployed_cadence() -> None:
    """Cadence-derived, not the module's own literals restated.

    Six vehicle-position polls a minute, one trip-updates poll a
    minute, sixty minutes an hour.
    """
    assert EXPECTED_OBJECTS[Feed.VEHICLE_POSITIONS] == (
        POLLS_PER_MINUTE_VEHICLE * MINUTES_PER_HOUR
    )
    assert EXPECTED_OBJECTS[Feed.TRIP_UPDATES] == (
        POLLS_PER_MINUTE_TRIP_UPDATES * MINUTES_PER_HOUR
    )


def test_stream_hour_yields_payloads_in_key_order(_bucket: str) -> None:
    """Objects arrive chronologically so latest-wins is correct."""
    client = boto3.client('s3')
    for second in (44, 4, 24):
        client.put_object(
            Bucket=_bucket,
            Key=(
                f'raw/vehiclepos/dt=2026-09-16/hour=21/2100{second:02d}'
                '.pb.gz'
            ),
            Body=gzip.compress(b''),
        )
    reader = RawReader(bucket=_bucket, session=boto3.Session())
    seconds = [
        obj.fetched_at.second
        for obj in reader.stream_hour(
            feed=Feed.VEHICLE_POSITIONS, hour=HOUR,
        )
    ]
    assert seconds == [4, 24, 44]


def test_get_payload_names_key_on_fetch_failure(_bucket: str) -> None:
    """A GET that fails between LIST and fetch names its key.

    ``raw/`` has a 30-day expiry, so an object can be deleted after
    it is listed but before it is fetched. The re-raised error must
    still be visible to the caller as a failure, not swallowed.
    """
    reader = RawReader(bucket=_bucket, session=boto3.Session())
    with pytest.raises(ClientError):
        reader._get_payload(  # pylint: disable=protected-access
            key='raw/vehiclepos/dt=2026-09-16/hour=21/210004.pb.gz',
            hour=HOUR,
        )


def test_stream_hour_empty_prefix_yields_nothing(_bucket: str) -> None:
    """A missing hour yields no objects rather than raising here.

    Whether that is an error is the handler's decision, since a
    genuinely quiet hour and a collection gap look identical at this
    level.
    """
    reader = RawReader(bucket=_bucket, session=boto3.Session())
    assert not list(
        reader.stream_hour(feed=Feed.TRIP_UPDATES, hour=HOUR),
    )
