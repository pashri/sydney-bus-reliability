"""Reduction of trip updates to one row per (trip, stop, stop_sequence).

The key includes ``stop_sequence`` because loop and shuttle routes
genuinely call the same ``stop_id`` twice on one trip; keying on
``stop_id`` alone collapses those two real calls into one.
``stop_sequence`` is 100% populated in the feed (measured), so it is
safe to use as part of the key.

Consecutive 60-second polls overlap 99.3%, so almost every poll is a
full restatement of the last. Measured over one peak hour, 13,041,479
StopTimeUpdates collapse to 338,825 keys - a 38.5:1 reduction - so the
reducer holds only the running last value per key, never the inputs.

Three rules, each earned by a measurement:

1. ``NO_DATA`` rows echo the static timetable verbatim (delay
   identically 0 on 99.9%, arrival time matching schedule to the exact
   second on 99.86%). They are not observations, so their values are
   nulled.
2. A ``NO_DATA`` observation never overwrites a real one. Absence of
   information must not displace information, and the ~3% in-progress
   dropout it represents clusters near end-of-run.
3. ``n_updates`` counts real observations only, because a trip is
   listed and echoed every 60 s for hours before it departs.
"""

from collections.abc import Iterator
from datetime import UTC, datetime
from itertools import islice
from typing import Any, Final

import pyarrow as pa
from aws_lambda_powertools import Logger
from google.transit import gtfs_realtime_pb2

from src.common.feed_decode import optional_enum, optional_field

logger = Logger()

BATCH_SIZE: Final[int] = 20_000

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
    pa.field('n_updates', pa.int32()),
    pa.field('schedule_relationship', pa.string()),
    pa.field('trip_schedule_relationship', pa.string()),
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
        Predicted instant and delay, both None when absent.
    """
    if not stop.HasField(name):
        return None, None
    event = getattr(stop, name)
    when = optional_field(message=event, name='time')
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
        TripUpdate.timestamp is populated on only 75.4% of updates.
    """
    stamp = optional_field(message=update, name='timestamp')
    if stamp:
        return datetime.fromtimestamp(stamp, tz=UTC)
    return fetched_at


class TripStopReducer:
    """Accumulates the last real prediction per (trip, stop)."""

    def __init__(self) -> None:
        self.rows: dict[Key, dict[str, Any]] = {}
        self.real_observations = 0

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
            'n_updates': 0,
            'schedule_relationship': None,
            'trip_schedule_relationship': optional_enum(
                message=update.trip,
                name='schedule_relationship',
                names=(
                    gtfs_realtime_pb2.TripDescriptor
                    .ScheduleRelationship.Name
                ),
            ),
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
        arrival, arrival_delay = stop_event(stop=stop, name='arrival')
        departure, departure_delay = stop_event(
            stop=stop, name='departure',
        )
        row.update({
            'final_predicted_arrival_utc': arrival,
            'delay_s': arrival_delay,
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
