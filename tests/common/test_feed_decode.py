"""Tests for HasField-safe protobuf decoding."""

import gzip
from typing import Final

import pytest
from google.transit import gtfs_realtime_pb2

from common.feed_decode import decode_feed, optional_enum, optional_field

# field 4 (current_status), varint 99 - a value the bindings do not
# define
UNKNOWN_STATUS_WIRE: Final[bytes] = b'\x20\x63'


def build_entity(*, with_status: bool) -> object:
    """Build one VehiclePosition, optionally setting current_status.

    Parameters
    ----------
    with_status : bool
        Whether to set current_status explicitly.

    Returns
    -------
    object
        A populated VehiclePosition message.
    """
    vehicle = gtfs_realtime_pb2.VehiclePosition()
    vehicle.timestamp = 1758146400
    if with_status:
        vehicle.current_status = (
            gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
        )
    return vehicle


def test_optional_enum_unset_returns_none() -> None:
    """An unset enum must be None, never its zero-valued default.

    TfNSW never sets current_status. Reading it without HasField
    yields IN_TRANSIT_TO on every one of ~1.09 million entities an
    hour - rows that look like data and are not.
    """
    vehicle = build_entity(with_status=False)
    assert optional_enum(
        message=vehicle,
        name='current_status',
        names=gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name,
    ) is None


def test_optional_enum_set_returns_name() -> None:
    """A genuinely set enum returns its symbolic name, not an int."""
    vehicle = build_entity(with_status=True)
    assert optional_enum(
        message=vehicle,
        name='current_status',
        names=gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name,
    ) == 'STOPPED_AT'


def test_optional_enum_unknown_wire_value_is_none() -> None:
    """An enum value the bindings do not know cannot crash decoding.

    GTFS-realtime is proto2, where enums are closed: an unrecognised
    value never reaches the field, so ``HasField`` is False and
    ``Name`` is never called on it.
    """
    vehicle = gtfs_realtime_pb2.VehiclePosition()
    vehicle.ParseFromString(UNKNOWN_STATUS_WIRE)
    assert optional_enum(
        message=vehicle,
        name='current_status',
        names=gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name,
    ) is None


def test_optional_field_unset_returns_none() -> None:
    """An unset scalar is None rather than zero."""
    vehicle = build_entity(with_status=False)
    assert optional_field(
        message=vehicle, name='current_stop_sequence',
    ) is None


def test_optional_field_set_zero_returns_zero() -> None:
    """A field genuinely set to zero is zero, not None."""
    vehicle = gtfs_realtime_pb2.VehiclePosition()
    vehicle.current_stop_sequence = 0
    assert optional_field(
        message=vehicle, name='current_stop_sequence',
    ) == 0


def test_decode_feed_parses_valid_payload() -> None:
    """A well-formed gzipped feed decodes to a FeedMessage."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = '2.0'
    payload = gzip.compress(feed.SerializeToString())
    decoded = decode_feed(payload=payload)
    assert decoded.header.gtfs_realtime_version == '2.0'


def test_decode_feed_rejects_truncated_payload() -> None:
    """A malformed payload raises rather than returning an empty feed.

    A feed that changes shape must fail an invocation, not silently
    produce a month of empty days.
    """
    with pytest.raises(ValueError):
        decode_feed(payload=gzip.compress(b'not a protobuf at all'))
