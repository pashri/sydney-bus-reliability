"""Vehicle position extraction and cross-poll dedupe.

The feed refreshes a vehicle about every 10 s and is polled at the same
cadence, so roughly a quarter of consecutive samples restate the
previous timestamp. Most are identical and collapse safely. A few
hundred per peak hour carry the same timestamp with a different
position, so the dedupe key must include the position or that real
movement is discarded.
"""

from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from itertools import islice
from typing import Any, Final

import pyarrow as pa
from aws_lambda_powertools import Logger
from google.transit import gtfs_realtime_pb2

from src.common.feed_decode import optional_enum, optional_field

logger = Logger()

BATCH_SIZE: Final[int] = 20_000  # rows
NULL_ISLAND: Final[tuple[float, float]] = (0.0, 0.0)

POSITION_FIELDS: Final[list[pa.Field[Any]]] = [
    pa.field('observed_at_utc', pa.timestamp('s', tz='UTC')),
    pa.field('fetched_at_utc', pa.timestamp('s', tz='UTC')),
    pa.field('position_age_s', pa.float64()),
    pa.field('vehicle_id', pa.string()),
    pa.field('vehicle_label', pa.string()),
    pa.field('trip_id', pa.string()),
    pa.field('route_id', pa.string()),
    pa.field('lat', pa.float64()),
    pa.field('lon', pa.float64()),
    pa.field('bearing', pa.float64()),
    pa.field('speed', pa.float64()),
    pa.field('occupancy_status', pa.string()),
    pa.field('congestion_level', pa.string()),
    pa.field('schedule_relationship', pa.string()),
    pa.field('current_status', pa.string()),
    pa.field('null_island', pa.bool_()),
]
POSITION_SCHEMA: Final[pa.Schema] = pa.schema(POSITION_FIELDS)

Key = tuple[str, int, float | None, float | None]


def position_values(*, vehicle: Any) -> dict[str, Any]:
    """Extract the position sub-message, tolerating its absence.

    Parameters
    ----------
    vehicle : Any
        A VehiclePosition message.

    Returns
    -------
    dict[str, Any]
        Latitude, longitude, bearing and speed, any of which may be
        None when the position was not reported.
    """
    if not vehicle.HasField('position'):
        return {
            'lat': None, 'lon': None, 'bearing': None, 'speed': None,
        }
    position = vehicle.position
    return {
        'lat': position.latitude,
        'lon': position.longitude,
        'bearing': optional_field(message=position, name='bearing'),
        'speed': optional_field(message=position, name='speed'),
    }


def position_record(*, entity: Any, fetched_at: datetime) -> dict[str, Any]:
    """Build one ``fact_vehicle_position`` record.

    Parameters
    ----------
    entity : Any
        A FeedEntity carrying a vehicle.
    fetched_at : datetime
        UTC time the poll was issued.

    Returns
    -------
    dict[str, Any]
        One row, with every optional field read via HasField.
    """
    vehicle = entity.vehicle
    observed = datetime.fromtimestamp(vehicle.timestamp, tz=UTC)
    values = position_values(vehicle=vehicle)
    return {
        'observed_at_utc': observed,
        'fetched_at_utc': fetched_at,
        'position_age_s': (fetched_at - observed).total_seconds(),
        'vehicle_id': vehicle.vehicle.id,
        'vehicle_label': vehicle.vehicle.label,
        'trip_id': vehicle.trip.trip_id,
        'route_id': vehicle.trip.route_id,
        'occupancy_status': optional_enum(
            message=vehicle,
            name='occupancy_status',
            names=(
                gtfs_realtime_pb2.VehiclePosition
                .OccupancyStatus.Name
            ),
        ),
        'congestion_level': optional_enum(
            message=vehicle,
            name='congestion_level',
            names=(
                gtfs_realtime_pb2.VehiclePosition
                .CongestionLevel.Name
            ),
        ),
        'schedule_relationship': optional_enum(
            message=vehicle.trip,
            name='schedule_relationship',
            names=(
                gtfs_realtime_pb2.TripDescriptor
                .ScheduleRelationship.Name
            ),
        ),
        'current_status': optional_enum(
            message=vehicle,
            name='current_status',
            names=(
                gtfs_realtime_pb2.VehiclePosition
                .VehicleStopStatus.Name
            ),
        ),
        'null_island': (
            values['lat'], values['lon'],
        ) == NULL_ISLAND,
        **values,
    }


class PositionDeduper:
    """Filters vehicle positions for one hour without retaining rows.

    Holds only the keys seen, a 4-tuple of vehicle id, timestamp,
    latitude and longitude. Latitude and longitude are part of the key
    because a repeated timestamp does not mean a repeated position.
    ``rows`` yields accepted rows immediately rather than
    accumulating them.
    """

    def __init__(self) -> None:
        self.seen: set[Key] = set()
        self.seen_pairs: set[tuple[str, int]] = set()
        self.collapsed = 0
        self.differing_position = 0

    def rows(
        self,
        *,
        feed: Any,
        fetched_at: datetime,
    ) -> Iterator[dict[str, Any]]:
        """Yield each entity in one poll unless it repeats a prior one.

        Parameters
        ----------
        feed : Any
            A parsed FeedMessage.
        fetched_at : datetime
            UTC time the poll was issued.

        Yields
        ------
        dict[str, Any]
            One accepted row, conforming to POSITION_SCHEMA.
        """
        for entity in feed.entity:
            if not entity.HasField('vehicle'):
                continue
            record = position_record(entity=entity, fetched_at=fetched_at)
            key: Key = (
                record['vehicle_id'],
                entity.vehicle.timestamp,
                record['lat'],
                record['lon'],
            )
            if key in self.seen:
                self.collapsed += 1
                continue
            self.count_conflict(key=key)
            self.seen.add(key)
            yield record

    def count_conflict(self, *, key: Key) -> None:
        """Note a same-timestamp sample whose position moved.

        Parameters
        ----------
        key : Key
            Vehicle id, timestamp, latitude and longitude.
        """
        pair = (key[0], key[1])
        if pair in self.seen_pairs:
            self.differing_position += 1
        self.seen_pairs.add(pair)


def position_batches(
    *,
    records: Iterable[dict[str, Any]],
    batch_size: int = BATCH_SIZE,
) -> Iterator[pa.RecordBatch]:
    """Group a lazy stream of rows into bounded record batches.

    Parameters
    ----------
    records : Iterable[dict[str, Any]]
        Accepted rows, typically chained from several polls' worth
        of ``PositionDeduper.rows``.
    batch_size : int
        Maximum rows per batch.

    Yields
    ------
    pa.RecordBatch
        A bounded batch conforming to POSITION_SCHEMA.
    """
    stream = iter(records)
    while chunk := list(islice(stream, batch_size)):
        yield pa.RecordBatch.from_pylist(chunk, schema=POSITION_SCHEMA)
