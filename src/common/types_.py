"""Shared types for feed collection."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Final, TypedDict


class Feed(StrEnum):
    """A TfNSW GTFS-Realtime feed for buses."""

    VEHICLE_POSITIONS = 'vehiclepos'
    TRIP_UPDATES = 'tripupdates'


@dataclass(frozen=True, slots=True)
class FetchResult:
    """The outcome of one attempt to fetch one feed.

    A failed fetch is a result, not an exception: the collector must
    record failures and carry on, because a raised error would lose the
    other polls in the same invocation.
    """

    feed: Feed
    fetched_at_utc: datetime
    received_at_utc: datetime
    server_date_utc: datetime | None
    status_code: int | None
    body: bytes
    error: str | None


CRASHED_POLL_ERROR: Final[str] = 'poll worker crashed unexpectedly'
"""The ``error`` value a crashed poll's audit row carries.

A wire value, not a message for humans: it is written verbatim
into stored ``RunRecord`` JSONL rows and read back by
``scripts/check_collection.py`` to classify historical rows.
Changing this string changes how *already-written* records are
interpreted, so it is defined once here and imported everywhere
it is produced or matched, rather than duplicated as a literal.
"""


class RunRecord(TypedDict):
    """One line of the collector's per-invocation audit log.

    Timestamps are ISO 8601 strings rather than datetimes because this
    shape is serialised straight to JSON Lines. ``rtt_s`` and ``skew_s``
    are separate on purpose: round-trip time measures the network, skew
    measures the clock, and a single combined number would hide which of
    the two had moved.
    """

    feed: str
    fetched_at_utc: str
    received_at_utc: str
    rtt_s: float
    server_date_utc: str | None
    skew_s: float | None
    status_code: int | None
    body_bytes: int
    error: str | None


class CollectionCounts(TypedDict):
    """The collector's return value, one invocation's tallies."""

    fetched: int
    stored: int
    failed: int
