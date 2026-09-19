"""Decoding GTFS-Realtime payloads without inventing data.

Protobuf returns a type default for any field that was never set, so an
unguarded read of ``current_status`` - which TfNSW never populates -
yields ``IN_TRANSIT_TO`` for every entity. Measured over one peak hour
that is 1,086,002 fabricated values. Every optional read therefore goes
through ``HasField``.

``gtfs-realtime-bindings`` ships no stubs and protobuf builds its
message classes at import, so attribute access on a decoded message
is unchecked by mypy and pylint alike. A mistyped field name surfaces
at runtime, not in the gates.
"""

import gzip
from collections.abc import Callable
from typing import Any

from aws_lambda_powertools import Logger
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

logger = Logger()


def optional_field(*, message: Any, name: str) -> Any | None:
    """Read a scalar field, distinguishing unset from zero.

    Parameters
    ----------
    message : Any
        Protobuf message to read from.
    name : str
        Field name.

    Returns
    -------
    Any | None
        The value, or None when the field was never set.

    Notes
    -----
    For singular, presence-tracked fields only. Protobuf raises
    ``ValueError`` for a repeated field such as ``entity`` or
    ``stop_time_update`` - iterate those directly rather than asking
    whether they are present.
    """
    if not message.HasField(name):
        return None
    return getattr(message, name)


def optional_enum(
    *,
    message: Any,
    name: str,
    names: Callable[[int], str],
) -> str | None:
    """Read an enum field as its symbolic name.

    Storing the name rather than the integer means an unfamiliar value
    surfaces in the data as itself instead of being silently coerced.

    Parameters
    ----------
    message : Any
        Protobuf message to read from.
    name : str
        Field name.
    names : Callable[[int], str]
        The enum's ``Name`` function.

    Returns
    -------
    str | None
        Symbolic name, or None when the field was never set.
    """
    value = optional_field(message=message, name=name)
    return None if value is None else names(value)


def decode_feed(*, payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Decompress and parse one stored raw feed object.

    Parameters
    ----------
    payload : bytes
        Gzipped protobuf bytes as stored under ``raw/``.

    Returns
    -------
    gtfs_realtime_pb2.FeedMessage
        Parsed feed.

    Raises
    ------
    ValueError
        If the payload is not valid gzipped protobuf.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(gzip.decompress(payload))
    except (
        DecodeError,
        # BadGzipFile named for readability; the bare OSError also
        # catches zlib errors surfaced while decompressing
        # corrupt-but-gzip-shaped input.
        gzip.BadGzipFile,
        OSError,
    ) as error:
        raise ValueError('unparseable feed payload') from error
    return feed
