"""Reduction of trip updates to one status row per trip.

A CANCELED trip update carries only its trip descriptor: no stop-time
updates, no vehicle and no timestamp. Stop-level reduction therefore
never sees it, and trip status is kept here instead.

Status can change within a day. A trip can be cancelled and later
reinstated, so the row keeps poll counts per status and the span of
its cancellation alongside the latest status, rather than a single
label.

Every time here is the poll's fetch time. CANCELED updates carry no
timestamp of their own.

The key ``(start_date, trip_id)`` is not unique within one poll. An
ADDED update can share it with a SCHEDULED one, so a status is counted
at most once per poll and a same-poll tie is broken by
``STATUS_PRECEDENCE``, never by message order.
"""

from collections.abc import Iterator
from datetime import datetime
from itertools import islice
from typing import Any, Final

import pyarrow as pa
from google.transit import gtfs_realtime_pb2

from common.feed_decode import optional_enum

BATCH_SIZE: Final[int] = 20_000  # rows

STATUS_PRECEDENCE: Final[tuple[str, ...]] = (
    'CANCELED', 'SCHEDULED', 'ADDED',
)
"""Which status wins when one poll reports a trip more than once."""

POLL_COLUMNS: Final[dict[str, str]] = {
    'SCHEDULED': 'scheduled_polls',
    'CANCELED': 'canceled_polls',
    'ADDED': 'added_polls',
}

TRIP_FIELDS: Final[list[pa.Field[Any]]] = [
    pa.field('start_date', pa.string()),
    pa.field('trip_id', pa.string()),
    pa.field('route_id', pa.string()),
    pa.field('start_time', pa.string()),
    pa.field('final_status', pa.string()),
    pa.field('final_status_at_utc', pa.timestamp('s', tz='UTC')),
    pa.field('scheduled_polls', pa.int32()),
    pa.field('canceled_polls', pa.int32()),
    pa.field('added_polls', pa.int32()),
    pa.field('first_seen_at_utc', pa.timestamp('s', tz='UTC')),
    pa.field('last_seen_at_utc', pa.timestamp('s', tz='UTC')),
    pa.field('first_canceled_at_utc', pa.timestamp('s', tz='UTC')),
    pa.field('last_canceled_at_utc', pa.timestamp('s', tz='UTC')),
    pa.field('had_vehicle', pa.bool_()),
]
TRIP_SCHEMA: Final[pa.Schema] = pa.schema(TRIP_FIELDS)

TripKey = tuple[str, str]


def rank(*, status: str | None) -> int:
    """Order a status for a same-poll tie, lowest wins.

    Parameters
    ----------
    status : str | None
        Symbolic trip-level relationship.

    Returns
    -------
    int
        Position in ``STATUS_PRECEDENCE``, with any other status and
        None after every listed one.
    """
    if status in STATUS_PRECEDENCE:
        return STATUS_PRECEDENCE.index(status)
    return len(STATUS_PRECEDENCE)


def blank(*, trip: Any, fetched_at: datetime) -> dict[str, Any]:
    """Create an empty row for one trip.

    Parameters
    ----------
    trip : Any
        A TripDescriptor message.
    fetched_at : datetime
        UTC time of the first poll the trip appeared in.

    Returns
    -------
    dict[str, Any]
        A row with no status recorded yet.
    """
    return {
        'start_date': trip.start_date,
        'trip_id': trip.trip_id,
        'route_id': trip.route_id,
        'start_time': trip.start_time,
        'final_status': None,
        'final_status_at_utc': None,
        **dict.fromkeys(POLL_COLUMNS.values(), 0),
        'first_seen_at_utc': fetched_at,
        'last_seen_at_utc': fetched_at,
        'first_canceled_at_utc': None,
        'last_canceled_at_utc': None,
        'had_vehicle': False,
    }


def note_seen(*, row: dict[str, Any], trip: Any, at: datetime) -> None:
    """Widen the seen span, keeping the latest poll's descriptor.

    Parameters
    ----------
    row : dict[str, Any]
        The running row.
    trip : Any
        A TripDescriptor message.
    at : datetime
        UTC fetch time of the poll.
    """
    row['first_seen_at_utc'] = min(row['first_seen_at_utc'], at)
    if at >= row['last_seen_at_utc']:
        row.update({
            'last_seen_at_utc': at,
            'route_id': trip.route_id,
            'start_time': trip.start_time,
        })


def note_status(*, row: dict[str, Any], status: str, at: datetime) -> None:
    """Record the status as final if it is the latest, or wins a tie.

    Parameters
    ----------
    row : dict[str, Any]
        The running row.
    status : str
        Symbolic trip-level relationship.
    at : datetime
        UTC fetch time of the poll.
    """
    current = row['final_status_at_utc']
    newer = current is None or at > current
    tie_won = at == current and rank(status=status) < rank(
        status=row['final_status'],
    )
    if newer or tie_won:
        row.update({'final_status': status, 'final_status_at_utc': at})


def note_canceled(*, row: dict[str, Any], at: datetime) -> None:
    """Widen the span of polls that reported the trip cancelled.

    Parameters
    ----------
    row : dict[str, Any]
        The running row.
    at : datetime
        UTC fetch time of the poll.
    """
    first = row['first_canceled_at_utc']
    last = row['last_canceled_at_utc']
    row['first_canceled_at_utc'] = at if first is None else min(first, at)
    row['last_canceled_at_utc'] = at if last is None else max(last, at)


class TripStatusReducer:
    """Accumulates trip-level status per (start_date, trip_id)."""

    def __init__(self) -> None:
        self.rows: dict[TripKey, dict[str, Any]] = {}
        self.counted: set[tuple[TripKey, str, datetime]] = set()

    def add(self, *, feed: Any, fetched_at: datetime) -> None:
        """Absorb every trip update in one poll.

        Parameters
        ----------
        feed : Any
            A parsed FeedMessage.
        fetched_at : datetime
            UTC time the poll was issued.
        """
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                self.absorb(update=entity.trip_update, at=fetched_at)

    def absorb(self, *, update: Any, at: datetime) -> None:
        """Merge one trip update into its trip's row.

        Parameters
        ----------
        update : Any
            A TripUpdate message.
        at : datetime
            UTC fetch time of the poll.
        """
        trip = update.trip
        if not trip.trip_id:
            return
        key: TripKey = (trip.start_date, trip.trip_id)
        row = self.rows.setdefault(key, blank(trip=trip, fetched_at=at))
        note_seen(row=row, trip=trip, at=at)
        row['had_vehicle'] = row['had_vehicle'] or update.HasField(
            'vehicle',
        )
        self.absorb_status(key=key, row=row, trip=trip, at=at)

    def absorb_status(
        self,
        *,
        key: TripKey,
        row: dict[str, Any],
        trip: Any,
        at: datetime,
    ) -> None:
        """Count and record the trip-level relationship, if sent.

        Parameters
        ----------
        key : TripKey
            The row's key.
        row : dict[str, Any]
            The running row.
        trip : Any
            A TripDescriptor message.
        at : datetime
            UTC fetch time of the poll.
        """
        status = optional_enum(
            message=trip,
            name='schedule_relationship',
            names=gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name,
        )
        if status is None:
            return
        note_status(row=row, status=status, at=at)
        if status == 'CANCELED':
            note_canceled(row=row, at=at)
        if status in POLL_COLUMNS and (key, status, at) not in self.counted:
            self.counted.add((key, status, at))
            row[POLL_COLUMNS[status]] += 1

    def batches(
        self,
        *,
        batch_size: int = BATCH_SIZE,
    ) -> Iterator[pa.RecordBatch]:
        """Emit reduced rows as bounded record batches.

        Parameters
        ----------
        batch_size : int
            Maximum rows per batch.

        Yields
        ------
        pa.RecordBatch
            A bounded batch conforming to TRIP_SCHEMA.
        """
        records = iter(self.rows.values())
        while chunk := list(islice(records, batch_size)):
            yield pa.RecordBatch.from_pylist(chunk, schema=TRIP_SCHEMA)
