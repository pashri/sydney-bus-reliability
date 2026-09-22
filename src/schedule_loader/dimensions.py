"""Transforms from static GTFS text rows to dimension record batches.

Every field in the bundle arrives double-quoted, including numerics, so
types are coerced here rather than inferred. Identifiers stay strings.
``stop_id`` is 5-7 digits with no fixed width (``200013`` sits beside
``2000100``), so reading it as a number breaks the join.

``stop_sequence`` and ``shape_pt_sequence`` are ordinals, not
identifiers, and are coerced to ``int32``. As strings they sort
lexicographically, ``['1', '10', '11', ..., '2', ...]``, which
silently reorders any route with ten or more stops or shape vertices.
"""

import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from itertools import islice
from typing import Any, Final

import pyarrow as pa
from aws_lambda_powertools import Logger

from common.gtfs_static import member_rows

logger = Logger()

BATCH_SIZE: Final[int] = 50_000  # rows
"""Rows per batch.

``shapes.txt`` holds millions of rows, so it is streamed in batches
rather than materialised in full.
"""

Row = dict[str, str]
Record = dict[str, str | float | None]


class Dimension(StrEnum):
    """A dimension table derived from the static bundle."""

    STOP = 'dim_stop'
    ROUTE = 'dim_route'
    TRIP = 'dim_trip'
    SCHEDULED_STOP_TIME = 'dim_scheduled_stop_time'
    SHAPE = 'dim_shape'
    CALENDAR = 'dim_calendar'
    CALENDAR_DATE = 'dim_calendar_dates'


def optional_float(*, value: str) -> float | None:
    """Parse a possibly-empty numeric field.

    Parameters
    ----------
    value : str
        Raw field value, which may be an empty string.

    Returns
    -------
    float | None
        Parsed value, or None when the field was blank.
    """
    return float(value) if value else None


def stop_record(*, row: Row) -> Record:
    """Build one ``dim_stop`` record.

    Parameters
    ----------
    row : Row
        One row of ``stops.txt``.

    Returns
    -------
    Record
        Stop identity, location and accessibility.
    """
    return {
        'stop_id': row['stop_id'],
        'stop_name': row['stop_name'],
        'stop_lat': optional_float(value=row['stop_lat']),
        'stop_lon': optional_float(value=row['stop_lon']),
        'wheelchair_boarding': (
            row.get('wheelchair_boarding') or None
        ),
    }


def route_record(*, row: Row) -> Record:
    """Build one ``dim_route`` record.

    Parameters
    ----------
    row : Row
        One row of ``routes.txt``.

    Returns
    -------
    Record
        Route identity and classification.
    """
    return {
        'route_id': row['route_id'],
        'agency_id': row['agency_id'],
        'route_short_name': row['route_short_name'],
        'route_long_name': row['route_long_name'],
        'route_type': row['route_type'],
    }


def trip_record(*, row: Row) -> Record:
    """Build one ``dim_trip`` record.

    Parameters
    ----------
    row : Row
        One row of ``trips.txt``.

    Returns
    -------
    Record
        Trip identity. ``direction_id`` comes only from here, as
        neither realtime feed populates it.
    """
    return {
        'trip_id': row['trip_id'],
        'route_id': row['route_id'],
        'service_id': row['service_id'],
        'direction_id': row['direction_id'],
        'trip_headsign': row['trip_headsign'],
        'shape_id': row['shape_id'],
    }


def stop_time_record(*, row: Row) -> Record:
    """Build one ``dim_scheduled_stop_time`` record.

    Times are kept as written. Resolving the past-24:00 convention
    needs the trip's ``start_date``, which lives in ``trips.txt``.

    Parameters
    ----------
    row : Row
        One row of ``stop_times.txt``.

    Returns
    -------
    Record
        Scheduled call at one stop.
    """
    return {
        'trip_id': row['trip_id'],
        'stop_id': row['stop_id'],
        'stop_sequence': int(row['stop_sequence']),
        'arrival_time': row['arrival_time'],
        'departure_time': row['departure_time'],
        'shape_dist_traveled': optional_float(
            value=row['shape_dist_traveled'],
        ),
    }


def shape_record(*, row: Row) -> Record:
    """Build one ``dim_shape`` record.

    Parameters
    ----------
    row : Row
        One row of ``shapes.txt``.

    Returns
    -------
    Record
        One vertex of a route polyline.
    """
    return {
        'shape_id': row['shape_id'],
        'shape_pt_sequence': int(row['shape_pt_sequence']),
        'shape_pt_lat': optional_float(value=row['shape_pt_lat']),
        'shape_pt_lon': optional_float(value=row['shape_pt_lon']),
        'shape_dist_traveled': optional_float(
            value=row.get('shape_dist_traveled', ''),
        ),
    }


def calendar_record(*, row: Row) -> Record:
    """Build one ``dim_calendar`` record.

    Parameters
    ----------
    row : Row
        One row of ``calendar.txt``.

    Returns
    -------
    Record
        Weekly service pattern and its validity window.
    """
    return {
        'service_id': row['service_id'],
        'monday': row['monday'],
        'tuesday': row['tuesday'],
        'wednesday': row['wednesday'],
        'thursday': row['thursday'],
        'friday': row['friday'],
        'saturday': row['saturday'],
        'sunday': row['sunday'],
        'start_date': row['start_date'],
        'end_date': row['end_date'],
    }


def calendar_date_record(*, row: Row) -> Record:
    """Build one ``dim_calendar_dates`` record.

    Parameters
    ----------
    row : Row
        One row of ``calendar_dates.txt``.

    Returns
    -------
    Record
        A single-date addition or removal of service.
    """
    return {
        'service_id': row['service_id'],
        'date': row['date'],
        'exception_type': row['exception_type'],
    }


@dataclass(frozen=True, slots=True)
class DimensionSpec:
    """How one dimension is read out of the bundle."""

    member: str
    schema: pa.Schema
    transform: Callable[..., Record]


STOP_FIELDS: Final[list[pa.Field[Any]]] = [
    pa.field('stop_id', pa.string()),
    pa.field('stop_name', pa.string()),
    pa.field('stop_lat', pa.float64()),
    pa.field('stop_lon', pa.float64()),
    pa.field('wheelchair_boarding', pa.string()),
]
STOP_SCHEMA: Final[pa.Schema] = pa.schema(STOP_FIELDS)
ROUTE_SCHEMA: Final[pa.Schema] = pa.schema([
    pa.field('route_id', pa.string()),
    pa.field('agency_id', pa.string()),
    pa.field('route_short_name', pa.string()),
    pa.field('route_long_name', pa.string()),
    pa.field('route_type', pa.string()),
])
TRIP_SCHEMA: Final[pa.Schema] = pa.schema([
    pa.field('trip_id', pa.string()),
    pa.field('route_id', pa.string()),
    pa.field('service_id', pa.string()),
    pa.field('direction_id', pa.string()),
    pa.field('trip_headsign', pa.string()),
    pa.field('shape_id', pa.string()),
])
SCHEDULED_STOP_TIME_FIELDS: Final[list[pa.Field[Any]]] = [
    pa.field('trip_id', pa.string()),
    pa.field('stop_id', pa.string()),
    pa.field('stop_sequence', pa.int32()),
    pa.field('arrival_time', pa.string()),
    pa.field('departure_time', pa.string()),
    pa.field('shape_dist_traveled', pa.float64()),
]
SCHEDULED_STOP_TIME_SCHEMA: Final[pa.Schema] = pa.schema(
    SCHEDULED_STOP_TIME_FIELDS,
)
SHAPE_FIELDS: Final[list[pa.Field[Any]]] = [
    pa.field('shape_id', pa.string()),
    pa.field('shape_pt_sequence', pa.int32()),
    pa.field('shape_pt_lat', pa.float64()),
    pa.field('shape_pt_lon', pa.float64()),
    pa.field('shape_dist_traveled', pa.float64()),
]
SHAPE_SCHEMA: Final[pa.Schema] = pa.schema(SHAPE_FIELDS)
CALENDAR_SCHEMA: Final[pa.Schema] = pa.schema([
    pa.field('service_id', pa.string()),
    pa.field('monday', pa.string()),
    pa.field('tuesday', pa.string()),
    pa.field('wednesday', pa.string()),
    pa.field('thursday', pa.string()),
    pa.field('friday', pa.string()),
    pa.field('saturday', pa.string()),
    pa.field('sunday', pa.string()),
    pa.field('start_date', pa.string()),
    pa.field('end_date', pa.string()),
])
CALENDAR_DATE_SCHEMA: Final[pa.Schema] = pa.schema([
    pa.field('service_id', pa.string()),
    pa.field('date', pa.string()),
    pa.field('exception_type', pa.string()),
])

SPECS: Final[dict[Dimension, DimensionSpec]] = {
    Dimension.STOP: DimensionSpec(
        member='stops.txt',
        schema=STOP_SCHEMA,
        transform=stop_record,
    ),
    Dimension.ROUTE: DimensionSpec(
        member='routes.txt',
        schema=ROUTE_SCHEMA,
        transform=route_record,
    ),
    Dimension.TRIP: DimensionSpec(
        member='trips.txt',
        schema=TRIP_SCHEMA,
        transform=trip_record,
    ),
    Dimension.SCHEDULED_STOP_TIME: DimensionSpec(
        member='stop_times.txt',
        schema=SCHEDULED_STOP_TIME_SCHEMA,
        transform=stop_time_record,
    ),
    Dimension.SHAPE: DimensionSpec(
        member='shapes.txt',
        schema=SHAPE_SCHEMA,
        transform=shape_record,
    ),
    Dimension.CALENDAR: DimensionSpec(
        member='calendar.txt',
        schema=CALENDAR_SCHEMA,
        transform=calendar_record,
    ),
    Dimension.CALENDAR_DATE: DimensionSpec(
        member='calendar_dates.txt',
        schema=CALENDAR_DATE_SCHEMA,
        transform=calendar_date_record,
    ),
}


def dimension_batches(
    *,
    archive: zipfile.ZipFile,
    dimension: Dimension,
    batch_size: int = BATCH_SIZE,
) -> Iterator[pa.RecordBatch]:
    """Stream one dimension out of the bundle as record batches.

    Parameters
    ----------
    archive : zipfile.ZipFile
        Open static GTFS archive.
    dimension : Dimension
        Which dimension to read.
    batch_size : int
        Maximum rows per batch.

    Yields
    ------
    pa.RecordBatch
        A bounded batch of dimension records.
    """
    spec = SPECS[dimension]
    rows = member_rows(archive=archive, name=spec.member)
    records = (spec.transform(row=row) for row in rows)
    while chunk := list(islice(records, batch_size)):
        yield pa.RecordBatch.from_pylist(chunk, schema=spec.schema)
