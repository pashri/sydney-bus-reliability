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

    A failed fetch is a result, not an exception. ``error`` is set and
    ``body`` is empty.
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

A wire value. It is written verbatim into stored ``RunRecord`` JSONL
rows and matched on read to classify historical rows, so changing the
string changes how already-written records are interpreted.
"""


class RunRecord(TypedDict):
    """One line of the collector's per-invocation audit log.

    Timestamps are ISO 8601 strings, not datetimes, because this shape
    is serialised straight to JSON Lines. ``rtt_s`` is the elapsed
    time between issuing the request and the response arriving.
    ``skew_s`` is the estimated difference between the local clock and
    the server's. They are separate numbers and not interchangeable.
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


class CurationJob(StrEnum):
    """One of the curation Lambdas."""

    SCHEDULE_LOADER = 'schedule_loader'
    COMPACTOR = 'compactor'
    MERGER = 'merger'


class CurationRecord(TypedDict):
    """One curation invocation's audit row.

    Written to a separate location from ``RunRecord``, which records
    collection rather than curation. The duplicate and unjoined
    counters record how many rows were collapsed or failed to join,
    so drift in those rates stays visible.
    """

    job: str
    invocation_id: str
    started_at_utc: str
    finished_at_utc: str
    partition: str
    objects_expected: int
    objects_read: int
    rows_in: int
    rows_out: int
    dupes_collapsed: int
    dupes_differing_position: int
    unjoined_route_ids: int
    unjoined_trip_ids: int
    unjoined_stop_ids: int
    peak_rss_mb: int
    error: str | None


class ScheduleCheck(TypedDict):
    """One daily static-GTFS fetch, changed or not.

    Written on every check, whether or not the timetable changed, so
    an unchanged day has a record rather than an absent partition.
    """

    checked_at_utc: str
    zip_sha256: str
    zip_filename: str
    changed: bool
    valid_from: str | None
