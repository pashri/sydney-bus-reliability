"""Tests for static GTFS dimension transforms."""

import io
import zipfile

import pytest

from schedule_loader.dimensions import SPECS, Dimension, dimension_batches

STOPS: str = (
    'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,'
    'wheelchair_boarding\n'
    '"200013","Alpha St","-33.8","151.2","","","1"\n'
    '"2000100","Beta Rd","-33.9","150.9","","",""\n'
)
STOPS_WITHOUT_ACCESSIBILITY: str = (
    'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n'
    '"200013","Alpha St","-33.8","151.2","",""\n'
)
ROUTES: str = (
    'route_id,agency_id,route_short_name,route_long_name,route_type\n'
    '"2449_130","2449","130","Nelson Bay to Newcastle","3"\n'
)
TRIPS: str = (
    'route_id,service_id,trip_id,shape_id,trip_headsign,direction_id\n'
    '"2449_130","3","1012281","80047","Nelson Bay","0"\n'
)
STOP_TIMES: str = (
    'trip_id,arrival_time,departure_time,stop_id,stop_sequence,'
    'shape_dist_traveled\n'
    '"1012281","25:15:00","25:15:30","200013","1","0.00"\n'
)
STOP_TIMES_WITH_RULES: str = (
    'trip_id,arrival_time,departure_time,stop_id,stop_sequence,'
    'stop_headsign,pickup_type,drop_off_type,shape_dist_traveled,'
    'timepoint,stop_note\n'
    '"1012281","06:50:00","06:50:00","231660","1","","0","1","0","1",""\n'
)
AGENCY: str = (
    'agency_id,agency_name,agency_url,agency_timezone,agency_lang,'
    'agency_phone\n'
    '"2448","Hunter Valley Buses","http://transportnsw.info",'
    '"Australia/Sydney","EN",""\n'
)
SHAPES: str = (
    'shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,'
    'shape_dist_traveled\n'
    '"80047","-32.7","152.1","1","0.00"\n'
)
CALENDAR: str = (
    'service_id,monday,tuesday,wednesday,thursday,friday,saturday,'
    'sunday,start_date,end_date\n'
    '"3","1","1","1","1","1","0","0","20260101","20261231"\n'
)
CALENDAR_DATES: str = (
    'service_id,date,exception_type\n'
    '"3","20260101","2"\n'
)


def build_zip(*, members: dict[str, str]) -> zipfile.ZipFile:
    """Build a multi-member zip archive in memory.

    Parameters
    ----------
    members : dict[str, str]
        Filename to text body.

    Returns
    -------
    zipfile.ZipFile
        Open archive.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return zipfile.ZipFile(io.BytesIO(buffer.getvalue()))


def test_stop_rows_keep_variable_width_ids() -> None:
    """Stop ids are 5-7 digits and must never be zero-padded."""
    archive = build_zip(members={'stops.txt': STOPS})
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.STOP, batch_size=10,
    ))
    assert batch.column('stop_id').to_pylist() == ['200013', '2000100']


def test_stop_rows_parse_coordinates_as_floats() -> None:
    """Latitude and longitude become floats, not strings."""
    archive = build_zip(members={'stops.txt': STOPS})
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.STOP, batch_size=10,
    ))
    assert batch.column('stop_lat').to_pylist() == [-33.8, -33.9]


def test_stop_rows_carry_wheelchair_boarding() -> None:
    """Accessibility is kept as the feed's code, blank becoming null."""
    archive = build_zip(members={'stops.txt': STOPS})
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.STOP, batch_size=10,
    ))
    assert batch.column('wheelchair_boarding').to_pylist() == ['1', None]


def test_stop_rows_tolerate_no_accessibility_column() -> None:
    """A bundle omitting the optional column still loads."""
    archive = build_zip(
        members={'stops.txt': STOPS_WITHOUT_ACCESSIBILITY},
    )
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.STOP, batch_size=10,
    ))
    assert batch.column('wheelchair_boarding').to_pylist() == [None]


def test_route_rows_carry_short_and_long_names() -> None:
    """Both route names are kept for display and joins."""
    archive = build_zip(members={'routes.txt': ROUTES})
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.ROUTE, batch_size=10,
    ))
    assert batch.column('route_short_name').to_pylist() == ['130']


def test_trip_rows_carry_shape_id() -> None:
    """shape_id links a trip to its geometry for the segment map."""
    archive = build_zip(members={'trips.txt': TRIPS})
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.TRIP, batch_size=10,
    ))
    assert batch.column('shape_id').to_pylist() == ['80047']


def test_stop_time_rows_keep_past_midnight_times_verbatim() -> None:
    """25:15:00 is stored as written, not coerced to 01:15.

    Resolving it needs the trip's start_date, which is not in this
    file, so the raw GTFS string is authoritative here.
    """
    archive = build_zip(members={'stop_times.txt': STOP_TIMES})
    batch = next(dimension_batches(
        archive=archive,
        dimension=Dimension.SCHEDULED_STOP_TIME,
        batch_size=10,
    ))
    assert batch.column('arrival_time').to_pylist() == ['25:15:00']


def test_stop_time_rows_coerce_stop_sequence_to_int() -> None:
    """stop_sequence is an ordinal and must sort numerically.

    A 12-stop trip written with a lexicographic string sort would put
    stop 10 before stop 2; stored as int32, ``sorted()`` and true
    numeric order agree.
    """
    header = (
        'trip_id,arrival_time,departure_time,stop_id,stop_sequence,'
        'shape_dist_traveled\n'
    )
    body = ''.join(
        f'"1012281","25:1{n}:00","25:1{n}:30","20001{n}","{n}","0.00"\n'
        for n in range(12)
    )
    archive = build_zip(members={'stop_times.txt': header + body})
    batch = next(dimension_batches(
        archive=archive,
        dimension=Dimension.SCHEDULED_STOP_TIME,
        batch_size=20,
    ))
    sequence = batch.column('stop_sequence').to_pylist()
    assert sequence == list(range(12))
    assert sequence == sorted(sequence)
    stringified = [str(n) for n in sequence]
    assert stringified != sorted(stringified)


def test_stop_time_rows_carry_shape_dist_traveled() -> None:
    """Distance along the shape places a stop exactly on the polyline."""
    archive = build_zip(members={'stop_times.txt': STOP_TIMES})
    batch = next(dimension_batches(
        archive=archive,
        dimension=Dimension.SCHEDULED_STOP_TIME,
        batch_size=10,
    ))
    assert batch.column('shape_dist_traveled').to_pylist() == [0.0]


def test_shape_rows_default_missing_dist_traveled_to_none() -> None:
    """shape_dist_traveled is optional in shapes.txt."""
    headers = 'shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n'
    body = '"80047","-32.7","152.1","1"\n'
    archive = build_zip(members={'shapes.txt': headers + body})
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.SHAPE, batch_size=10,
    ))
    assert batch.column('shape_dist_traveled').to_pylist() == [None]


def test_shape_rows_carry_present_dist_traveled() -> None:
    """When present, shape_dist_traveled becomes a float."""
    archive = build_zip(members={'shapes.txt': SHAPES})
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.SHAPE, batch_size=10,
    ))
    assert batch.column('shape_dist_traveled').to_pylist() == [0.0]


def test_shape_rows_coerce_shape_pt_sequence_to_int() -> None:
    """A 250-vertex polyline must come back in true numeric order.

    Sorted as strings, vertex 2 would land at index 111 instead of 1,
    turning the route geometry into a zigzag.
    """
    header = (
        'shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,'
        'shape_dist_traveled\n'
    )
    body = ''.join(
        f'"80047","-32.7","152.1","{n}","{float(n)}"\n'
        for n in range(250)
    )
    archive = build_zip(members={'shapes.txt': header + body})
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.SHAPE, batch_size=300,
    ))
    sequence = batch.column('shape_pt_sequence').to_pylist()
    assert sequence == list(range(250))
    assert sequence == sorted(sequence)
    stringified = [str(n) for n in sequence]
    assert stringified != sorted(stringified)


def test_calendar_rows_carry_validity_window() -> None:
    """start_date and end_date bound when the weekly pattern applies."""
    archive = build_zip(members={'calendar.txt': CALENDAR})
    batch = next(dimension_batches(
        archive=archive, dimension=Dimension.CALENDAR, batch_size=10,
    ))
    assert batch.column('end_date').to_pylist() == ['20261231']


def test_calendar_date_rows_carry_exception_type() -> None:
    """exception_type distinguishes an addition from a removal."""
    archive = build_zip(
        members={'calendar_dates.txt': CALENDAR_DATES},
    )
    batch = next(dimension_batches(
        archive=archive,
        dimension=Dimension.CALENDAR_DATE,
        batch_size=10,
    ))
    assert batch.column('exception_type').to_pylist() == ['2']


def test_batches_respect_batch_size() -> None:
    """Rows are emitted in bounded batches, never one giant table."""
    archive = build_zip(members={'stops.txt': STOPS})
    batches = list(dimension_batches(
        archive=archive, dimension=Dimension.STOP, batch_size=1,
    ))
    assert [batch.num_rows for batch in batches] == [1, 1]


def test_missing_member_raises_key_error_on_consumption() -> None:
    """A missing member fails loudly when the generator is drained."""
    archive = build_zip(members={})
    batches = dimension_batches(
        archive=archive, dimension=Dimension.STOP, batch_size=10,
    )
    with pytest.raises(KeyError):
        next(batches)


def test_every_dimension_has_a_spec() -> None:
    """No dimension can be added without a schema and transform."""
    assert set(SPECS) == set(Dimension)


def test_stop_time_rows_carry_timepoint_and_pickup_rules() -> None:
    """timepoint and the pickup and drop-off rules are kept as coded."""
    archive = build_zip(members={'stop_times.txt': STOP_TIMES_WITH_RULES})
    row = next(dimension_batches(
        archive=archive,
        dimension=Dimension.SCHEDULED_STOP_TIME,
        batch_size=10,
    )).to_pylist()[0]
    assert row['timepoint'] == '1'
    assert row['pickup_type'] == '0'
    assert row['drop_off_type'] == '1'


def test_stop_time_rows_tolerate_missing_rule_columns() -> None:
    """A bundle without the optional columns reads them as None."""
    archive = build_zip(members={'stop_times.txt': STOP_TIMES})
    row = next(dimension_batches(
        archive=archive,
        dimension=Dimension.SCHEDULED_STOP_TIME,
        batch_size=10,
    )).to_pylist()[0]
    assert row['timepoint'] is None
    assert row['pickup_type'] is None
    assert row['drop_off_type'] is None


def test_agency_rows_carry_operator_names() -> None:
    """dim_agency turns a route's agency_id into an operator name."""
    archive = build_zip(members={'agency.txt': AGENCY})
    row = next(dimension_batches(
        archive=archive, dimension=Dimension.AGENCY, batch_size=10,
    )).to_pylist()[0]
    assert row == {
        'agency_id': '2448',
        'agency_name': 'Hunter Valley Buses',
        'agency_url': 'http://transportnsw.info',
        'agency_timezone': 'Australia/Sydney',
    }
