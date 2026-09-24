"""Reduction of trip updates to one row per (trip, stop, stop_sequence).

The key includes ``stop_sequence``. Loop and shuttle routes really do
call the same ``stop_id`` twice on one trip, so keying on ``stop_id``
alone merges two separate calls. ``stop_sequence`` is populated on
every row in the feed.

Consecutive 60-second polls overlap almost completely, so nearly every
poll restates the last. Over a peak hour the updates collapse by
roughly 40:1. The reducer holds only the running last value per key,
never the inputs.

Three rules:

1. ``NO_DATA`` rows echo the static timetable rather than reporting
   anything. Their delay is almost always 0 and their arrival time
   almost always matches the schedule exactly. They are not
   observations, so their values are nulled.
2. A ``NO_DATA`` observation never overwrites a real one. It usually
   means tracking dropped mid-run, which is not information about the
   prediction.
3. ``n_updates`` counts real observations only. A trip is listed and
   echoed every 60 s for hours before it departs.

One kind of update is dropped whole. As a bus leaves its first stop
early in the morning, TfNSW sometimes sends a single update with every
time exactly a day late, the delays still right, and the first stop's
departure copied from the timetable. The next poll corrects the stops
ahead but never mentions the stops already passed, so latest-wins would
keep the day-late time for good. An update with any time twelve hours
or more after its trip's start is not read at all, not even as an
observation.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from itertools import islice
from typing import Any, Final

import pyarrow as pa
from aws_lambda_powertools import Logger
from google.transit import gtfs_realtime_pb2

from common.feed_decode import optional_field
from common.service_day import SYDNEY

logger = Logger()

BATCH_SIZE: Final[int] = 20_000  # rows
DAY_AHEAD: Final[timedelta] = timedelta(hours=12)
"""How far past its trip's start a time must be to read as a day late."""

StopRelationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate
REAL_RELATIONSHIPS: Final[frozenset[int]] = frozenset({
    StopRelationship.SCHEDULED,
    StopRelationship.SKIPPED,
})
"""Relationships that carry genuine information.

``NO_DATA`` is excluded because it is an echo, and ``UNSCHEDULED`` was
never observed in the sampled data.
"""

TRIP_STOP_FIELDS: Final[list[pa.Field[Any]]] = [
    pa.field('service_date', pa.string()),
    pa.field('trip_id', pa.string()),
    pa.field('stop_id', pa.string()),
    pa.field('stop_sequence', pa.int32()),
    pa.field('route_id', pa.string()),
    pa.field('final_predicted_arrival_utc', pa.timestamp('s', tz='UTC')),
    pa.field('delay_s', pa.int32()),
    pa.field(
        'final_predicted_departure_utc', pa.timestamp('s', tz='UTC'),
    ),
    pa.field('departure_delay_s', pa.int32()),
    pa.field('last_update_at_utc', pa.timestamp('s', tz='UTC')),
    pa.field('arrival_updated_at_utc', pa.timestamp('s', tz='UTC')),
    pa.field('n_updates', pa.int32()),
    pa.field('schedule_relationship', pa.string()),
    pa.field('had_vehicle', pa.bool_()),
    pa.field('lost_tracking', pa.bool_()),
    pa.field('last_observed_at_utc', pa.timestamp('s', tz='UTC')),
]
TRIP_STOP_SCHEMA: Final[pa.Schema] = pa.schema(TRIP_STOP_FIELDS)

Key = tuple[str, str, str, int | None]


def stop_event(*, stop: Any, name: str) -> tuple[Any, Any]:
    """Read one arrival or departure event.

    Parameters
    ----------
    stop : Any
        A StopTimeUpdate message.
    name : str
        Either ``arrival`` or ``departure``.

    Returns
    -------
    tuple[Any, Any]
        Predicted instant and delay, both None when absent. A time of
        0 also reads as absent, delay included: the feed blanks the
        first stop's arrival to 0 with a delay of 0 once the bus has
        left, and read literally that is a perfect arrival in 1970.
    """
    if not stop.HasField(name):
        return None, None
    event = getattr(stop, name)
    when = optional_field(message=event, name='time')
    if when == 0:
        return None, None
    return (
        None if when is None else datetime.fromtimestamp(when, tz=UTC),
        optional_field(message=event, name='delay'),
    )


def observation_time(*, update: Any, fetched_at: datetime) -> datetime:
    """Choose the best timestamp for one trip update.

    Parameters
    ----------
    update : Any
        A TripUpdate message.
    fetched_at : datetime
        UTC time the poll was issued.

    Returns
    -------
    datetime
        The update's own timestamp when present, else the poll time.
        TfNSW leaves ``TripUpdate.timestamp`` unset on many updates, so
        the fallback is the common case, not an edge case.
    """
    stamp = optional_field(message=update, name='timestamp')
    if stamp:
        return datetime.fromtimestamp(stamp, tz=UTC)
    return fetched_at


def trip_start(*, trip: Any) -> datetime | None:
    """Read when a trip starts, from its descriptor.

    Parameters
    ----------
    trip : Any
        A TripDescriptor message.

    Returns
    -------
    datetime | None
        ``start_date`` and ``start_time`` as a Sydney instant, or None
        when either is missing or malformed. For a trip timetabled after
        midnight the feed already sends the next date and a wrapped
        time, so this is its true start.
    """
    try:
        return datetime.strptime(
            f'{trip.start_date} {trip.start_time}', '%Y%m%d %H:%M:%S',
        ).replace(tzinfo=SYDNEY)
    except ValueError:
        return None


def dated_a_day_ahead(*, update: Any) -> bool:
    """Say whether an update carries a time a day past its trip's start.

    Parameters
    ----------
    update : Any
        A TripUpdate message.

    Returns
    -------
    bool
        True when any arrival or departure is at least ``DAY_AHEAD``
        after the trip's start. False when the start is unknown.
    """
    start = trip_start(trip=update.trip)
    if start is None:
        return False
    return any(
        when - start >= DAY_AHEAD
        for stop in update.stop_time_update
        for name in ('arrival', 'departure')
        if (when := stop_event(stop=stop, name=name)[0]) is not None
    )


class TripStopReducer:
    """Accumulates the last real prediction per (trip, stop)."""

    def __init__(self) -> None:
        self.rows: dict[Key, dict[str, Any]] = {}
        self.real_observations = 0
        self.day_ahead_updates = 0

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
                self.absorb_trip(
                    update=entity.trip_update, fetched_at=fetched_at,
                )

    def absorb_trip(self, *, update: Any, fetched_at: datetime) -> None:
        """Absorb every stop-time update on one trip.

        Parameters
        ----------
        update : Any
            A TripUpdate message.
        fetched_at : datetime
            UTC time the poll was issued.
        """
        if dated_a_day_ahead(update=update):
            self.day_ahead_updates += 1
            return
        seen_at = observation_time(
            update=update, fetched_at=fetched_at,
        )
        for stop in update.stop_time_update:
            self.absorb_stop(
                update=update, stop=stop, seen_at=seen_at,
            )

    def absorb_stop(
        self,
        *,
        update: Any,
        stop: Any,
        seen_at: datetime,
    ) -> None:
        """Merge one stop-time update into the running row.

        Parameters
        ----------
        update : Any
            The owning TripUpdate message.
        stop : Any
            A StopTimeUpdate message.
        seen_at : datetime
            When this observation was made.
        """
        key: Key = (
            update.trip.start_date,
            update.trip.trip_id,
            stop.stop_id,
            optional_field(message=stop, name='stop_sequence'),
        )
        row = self.rows.setdefault(
            key, self.blank(update=update, stop=stop),
        )
        row['had_vehicle'] = row['had_vehicle'] or update.HasField(
            'vehicle',
        )
        self.note_observation(row=row, stop=stop, seen_at=seen_at)
        if stop.schedule_relationship in REAL_RELATIONSHIPS:
            self.apply_real(row=row, stop=stop, seen_at=seen_at)

    @staticmethod
    def blank(*, update: Any, stop: Any) -> dict[str, Any]:
        """Create an empty row for one (trip, stop) key.

        Parameters
        ----------
        update : Any
            The owning TripUpdate message.
        stop : Any
            A StopTimeUpdate message.

        Returns
        -------
        dict[str, Any]
            A row with no prediction yet recorded.
        """
        return {
            'service_date': update.trip.start_date,
            'trip_id': update.trip.trip_id,
            'stop_id': stop.stop_id,
            'stop_sequence': optional_field(
                message=stop, name='stop_sequence',
            ),
            'route_id': update.trip.route_id,
            'final_predicted_arrival_utc': None,
            'delay_s': None,
            'final_predicted_departure_utc': None,
            'departure_delay_s': None,
            'last_update_at_utc': None,
            'arrival_updated_at_utc': None,
            'n_updates': 0,
            'schedule_relationship': None,
            'had_vehicle': False,
            'lost_tracking': False,
            'last_observed_at_utc': None,
        }

    @staticmethod
    def note_observation(
        *,
        row: dict[str, Any],
        stop: Any,
        seen_at: datetime,
    ) -> None:
        """Record that the key was observed, whatever its content.

        Before any real observation has been recorded, the row's
        ``schedule_relationship`` tracks whatever the feed last said,
        including ``NO_DATA`` - so an echo-only stop still records
        the fact that it was echoed, rather than leaving the column
        None. Once a real observation lands, ``apply_real`` owns the
        column instead.

        Parameters
        ----------
        row : dict[str, Any]
            The running row.
        stop : Any
            A StopTimeUpdate message.
        seen_at : datetime
            When this observation was made.
        """
        last = row['last_observed_at_utc']
        if last is None or seen_at >= last:
            row['last_observed_at_utc'] = seen_at
        if row['last_update_at_utc'] is None:
            row['schedule_relationship'] = (
                StopRelationship.ScheduleRelationship.Name(
                    stop.schedule_relationship,
                )
            )

    def apply_real(
        self,
        *,
        row: dict[str, Any],
        stop: Any,
        seen_at: datetime,
    ) -> None:
        """Apply a genuine observation, latest-wins.

        An observation with no arrival keeps the row's earlier
        arrival, its delay and ``arrival_updated_at_utc``, while its
        departure still lands and ``last_update_at_utc`` moves on.

        Parameters
        ----------
        row : dict[str, Any]
            The running row.
        stop : Any
            A StopTimeUpdate carrying real information.
        seen_at : datetime
            When this observation was made.
        """
        row['n_updates'] += 1
        self.real_observations += 1
        last = row['last_update_at_utc']
        if last is not None and seen_at < last:
            return
        arrival = stop_event(stop=stop, name='arrival')
        if arrival != (None, None):
            row['final_predicted_arrival_utc'], row['delay_s'] = arrival
            row['arrival_updated_at_utc'] = seen_at
        departure, departure_delay = stop_event(
            stop=stop, name='departure',
        )
        row.update({
            'final_predicted_departure_utc': departure,
            'departure_delay_s': departure_delay,
            'last_update_at_utc': seen_at,
            'schedule_relationship': StopRelationship
            .ScheduleRelationship.Name(stop.schedule_relationship),
        })

    def batches(
        self,
        *,
        batch_size: int = BATCH_SIZE,
    ) -> Iterator[pa.RecordBatch]:
        """Emit reduced rows as bounded record batches.

        ``lost_tracking`` is resolved here rather than incrementally,
        because it depends on the final ordering of observations.

        Parameters
        ----------
        batch_size : int
            Maximum rows per batch.

        Yields
        ------
        pa.RecordBatch
            A bounded batch conforming to TRIP_STOP_SCHEMA.
        """
        records = (
            self.finalise(row=row) for row in self.rows.values()
        )
        while chunk := list(islice(records, batch_size)):
            yield pa.RecordBatch.from_pylist(
                chunk, schema=TRIP_STOP_SCHEMA,
            )

    @staticmethod
    def finalise(*, row: dict[str, Any]) -> dict[str, Any]:
        """Set the dropout flag on a completed row.

        Parameters
        ----------
        row : dict[str, Any]
            The running row.

        Returns
        -------
        dict[str, Any]
            The row with ``lost_tracking`` resolved.
        """
        last_real = row['last_update_at_utc']
        last_any = row['last_observed_at_utc']
        row['lost_tracking'] = (
            last_real is not None
            and last_any is not None
            and last_any > last_real
        )
        return row
