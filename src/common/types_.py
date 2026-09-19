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


class CurationJob(StrEnum):
    """One of the three Phase 2 curation Lambdas."""

    SCHEDULE_LOADER = 'schedule_loader'
    COMPACTOR = 'compactor'
    MERGER = 'merger'


class CurationRecord(TypedDict):
    """One curation invocation's audit row.

    Separate from ``RunRecord`` on purpose: ``collector_run`` answers
    "was the gap TfNSW or the collector?", and folding curation
    outcomes into it would make that question unanswerable.

    The three duplicate and unjoined counters exist because the
    exploration spike measured their rates (526 differing-position
    duplicates per peak hour, ~0.2% unjoined route ids); recording
    them keeps drift visible instead of silent.
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

    Written on every check so that "the timetable did not change
    between these dates" is an assertion backed by records rather
    than by absent partitions.
    """

    checked_at_utc: str
    zip_sha256: str
    zip_filename: str
    changed: bool
    valid_from: str | None
