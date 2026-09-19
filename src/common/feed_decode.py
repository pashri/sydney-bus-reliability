"""Reading GTFS-Realtime fields that may not have been sent.

Reading a protobuf field that was never sent does not fail. It returns
the field's default, which is indistinguishable from a value the
publisher really sent. ``current_status`` defaults to ``IN_TRANSIT_TO``
and TfNSW never sends it, so reading it directly would label every bus
in every poll as in transit, inventing a value for each one.

``HasField`` is the only way to tell "not sent" from "sent, and happens
to equal the default", so every optional read here goes through it and
returns ``None`` when the field is absent.

Take that care manually, because the tooling cannot help. Protobuf
builds its message classes as it imports them and ships no type stubs,
so neither mypy nor pylint can see these fields. A misspelled field
name gets past both gates and fails at runtime.
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
    except (DecodeError, gzip.BadGzipFile, OSError) as error:
        raise ValueError('unparseable feed payload') from error
    return feed
