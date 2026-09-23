"""Tests for the trip-update reduction."""

from datetime import UTC, datetime

from google.transit import gtfs_realtime_pb2

from compactor.trip_updates import TRIP_STOP_SCHEMA, TripStopReducer

FETCHED: datetime = datetime(2026, 9, 16, 21, 0, 34, tzinfo=UTC)
SCHEDULED = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED
NO_DATA = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.NO_DATA
SKIPPED = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED


def build_feed(
    *,
    relationship: int,
    arrival_time: int,
    delay: int,
    stamp: int,
    with_vehicle: bool = True,
    departure_time: int | None = None,
) -> object:
    """Build a FeedMessage holding one trip update at one stop.

    Parameters
    ----------
    relationship : int
        StopTimeUpdate schedule relationship.
    arrival_time : int
        Predicted arrival as a Unix timestamp.
    delay : int
        Predicted arrival delay in seconds.
    stamp : int
        TripUpdate timestamp.
    with_vehicle : bool
        Whether a vehicle descriptor is attached.
    departure_time : int | None
        Predicted departure as a Unix timestamp, or None to send no
        departure.

    Returns
    -------
    object
        A populated FeedMessage.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = '1.0'
    entity = feed.entity.add()
    entity.id = 'tu-1'
    update = entity.trip_update
    update.trip.trip_id = '1012281'
    update.trip.route_id = '2447_160'
    update.trip.start_date = '20260917'
    if with_vehicle:
        update.vehicle.id = '8183_a'
        update.vehicle.label = '8183'
        update.timestamp = stamp
    stop = update.stop_time_update.add()
    stop.stop_id = '200013'
    stop.stop_sequence = 1
    stop.schedule_relationship = relationship
    stop.arrival.time = arrival_time
    stop.arrival.delay = delay
    if departure_time is not None:
        stop.departure.time = departure_time
    return feed


def test_no_data_values_are_nulled() -> None:
    """Schedule echoes must not be stored as predictions.

    Measured: delay is identically 0 on 99.9% of NO_DATA rows and
    arrival.time matches the static schedule to the exact second.
    Keeping them would inject a fake perfect on-time reading onto
    ~24% of rows.
    """
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=NO_DATA,
            arrival_time=1789592400,
            delay=0,
            stamp=0,
            with_vehicle=False,
        ),
        fetched_at=FETCHED,
    )
    batch = next(reducer.batches())
    assert batch.column('delay_s').to_pylist() == [None]
    assert batch.column('final_predicted_arrival_utc').to_pylist() == [
        None,
    ]
    assert batch.column('had_vehicle').to_pylist() == [False]


def test_no_data_never_overwrites_a_real_observation() -> None:
    """A mid-trip dropout must not erase the last real prediction.

    This is the measured ~3% in-progress dropout, concentrated near
    end-of-run where the arrival proxy matters most.
    """
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=SCHEDULED,
            arrival_time=1789592700,
            delay=240,
            stamp=1789592400,
        ),
        fetched_at=FETCHED,
    )
    reducer.add(
        feed=build_feed(
            relationship=NO_DATA,
            arrival_time=1789592400,
            delay=0,
            stamp=0,
            with_vehicle=False,
        ),
        fetched_at=FETCHED,
    )
    batch = next(reducer.batches())
    assert batch.column('delay_s').to_pylist() == [240]
    assert batch.column('lost_tracking').to_pylist() == [True]


def test_later_real_observation_wins() -> None:
    """Among real observations, the latest one is retained."""
    reducer = TripStopReducer()
    for stamp, delay in ((1789592400, 60), (1789592500, 300)):
        reducer.add(
            feed=build_feed(
                relationship=SCHEDULED,
                arrival_time=1789592700,
                delay=delay,
                stamp=stamp,
            ),
            fetched_at=FETCHED,
        )
    batch = next(reducer.batches())
    assert batch.column('delay_s').to_pylist() == [300]


def test_n_updates_counts_only_real_observations() -> None:
    """Pre-departure echoes must not inflate the churn count.

    Counting them would make n_updates measure how early TfNSW
    publishes a trip rather than how much its prediction moved.
    """
    reducer = TripStopReducer()
    for _ in range(200):
        reducer.add(
            feed=build_feed(
                relationship=NO_DATA,
                arrival_time=1789592400,
                delay=0,
                stamp=0,
                with_vehicle=False,
            ),
            fetched_at=FETCHED,
        )
    for stamp in (1789592400, 1789592500, 1789592600):
        reducer.add(
            feed=build_feed(
                relationship=SCHEDULED,
                arrival_time=1789592700,
                delay=60,
                stamp=stamp,
            ),
            fetched_at=FETCHED,
        )
    batch = next(reducer.batches())
    assert batch.column('n_updates').to_pylist() == [3]


def test_out_of_order_real_observation_is_ignored() -> None:
    """A real observation older than the current one is dropped.

    Still counts toward n_updates, since it was a genuine
    observation, just not the freshest one.
    """
    reducer = TripStopReducer()
    for stamp, delay in ((1789592500, 300), (1789592400, 60)):
        reducer.add(
            feed=build_feed(
                relationship=SCHEDULED,
                arrival_time=1789592700,
                delay=delay,
                stamp=stamp,
            ),
            fetched_at=FETCHED,
        )
    batch = next(reducer.batches())
    assert batch.column('delay_s').to_pylist() == [300]
    assert batch.column('n_updates').to_pylist() == [2]


def test_no_data_only_row_records_the_relationship() -> None:
    """A stop that never reported keeps NO_DATA as its relationship.

    The row is retained so "no vehicle was reported" is a recorded
    fact rather than an inference from a null delay.
    """
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=NO_DATA,
            arrival_time=1789592400,
            delay=0,
            stamp=0,
            with_vehicle=False,
        ),
        fetched_at=FETCHED,
    )
    batch = next(reducer.batches())
    assert batch.column('schedule_relationship').to_pylist() == [
        'NO_DATA',
    ]


def test_real_observation_overwrites_an_earlier_no_data() -> None:
    """A trip that starts as an echo and then reports is SCHEDULED."""
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=NO_DATA,
            arrival_time=1789592400,
            delay=0,
            stamp=0,
            with_vehicle=False,
        ),
        fetched_at=FETCHED,
    )
    reducer.add(
        feed=build_feed(
            relationship=SCHEDULED,
            arrival_time=1789592700,
            delay=60,
            stamp=1789592400,
        ),
        fetched_at=FETCHED,
    )
    batch = next(reducer.batches())
    assert batch.column('schedule_relationship').to_pylist() == [
        'SCHEDULED',
    ]


def test_dropout_keeps_the_real_relationship() -> None:
    """SCHEDULED then NO_DATA stays SCHEDULED with lost_tracking set."""
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=SCHEDULED,
            arrival_time=1789592700,
            delay=240,
            stamp=1789592400,
        ),
        fetched_at=FETCHED,
    )
    reducer.add(
        feed=build_feed(
            relationship=NO_DATA,
            arrival_time=1789592400,
            delay=0,
            stamp=0,
            with_vehicle=False,
        ),
        fetched_at=FETCHED,
    )
    batch = next(reducer.batches())
    assert batch.column('schedule_relationship').to_pylist() == [
        'SCHEDULED',
    ]
    assert batch.column('lost_tracking').to_pylist() == [True]


def test_skipped_counts_as_a_real_observation() -> None:
    """SKIPPED is information, unlike NO_DATA, so it is retained."""
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=SKIPPED,
            arrival_time=1789592700,
            delay=0,
            stamp=1789592400,
        ),
        fetched_at=FETCHED,
    )
    batch = next(reducer.batches())
    assert batch.column('schedule_relationship').to_pylist() == [
        'SKIPPED',
    ]
    assert batch.column('n_updates').to_pylist() == [1]


def build_two_call_feed() -> object:
    """Build a FeedMessage of one trip calling the same stop twice.

    Returns
    -------
    object
        A populated FeedMessage with two StopTimeUpdates sharing a
        ``stop_id`` but carrying distinct ``stop_sequence`` values.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = '1.0'
    entity = feed.entity.add()
    entity.id = 'tu-loop'
    update = entity.trip_update
    update.trip.trip_id = '1012281'
    update.trip.route_id = '2447_160'
    update.trip.start_date = '20260917'
    update.vehicle.id = '8183_a'
    update.timestamp = 1789592400
    first = update.stop_time_update.add()
    first.stop_id = '200013'
    first.stop_sequence = 3
    first.schedule_relationship = SCHEDULED
    first.arrival.time = 1789592700
    first.arrival.delay = 60
    second = update.stop_time_update.add()
    second.stop_id = '200013'
    second.stop_sequence = 17
    second.schedule_relationship = SCHEDULED
    second.arrival.time = 1789595700
    second.arrival.delay = 300
    return feed


def test_loop_route_keeps_both_calls_at_the_same_stop() -> None:
    """A trip calling the same stop_id twice must not collapse to one row.

    The key must include stop_sequence, since keying on stop_id alone
    would overwrite the first call with the second.
    """
    reducer = TripStopReducer()
    reducer.add(feed=build_two_call_feed(), fetched_at=FETCHED)
    batches = list(reducer.batches())
    assert sum(batch.num_rows for batch in batches) == 2
    rows = {
        row['stop_sequence']: row['delay_s']
        for batch in batches
        for row in batch.to_pylist()
    }
    assert rows == {3: 60, 17: 300}


def test_start_date_comes_from_the_trip_descriptor() -> None:
    """Rows carry the feed's start date, not the poll's calendar date.

    It is not always the service date. The merger derives that.
    """
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=SCHEDULED,
            arrival_time=1789592700,
            delay=60,
            stamp=1789592400,
        ),
        fetched_at=FETCHED,
    )
    batch = next(reducer.batches())
    assert batch.column('service_date').to_pylist() == ['20260917']


def test_zero_arrival_time_is_not_a_prediction() -> None:
    """An explicitly sent arrival time of 0 is read as no arrival.

    The feed blanks the first stop's arrival to 0 once the bus has
    left, beside a real departure. Read literally it is 1970.
    """
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=SCHEDULED,
            arrival_time=0,
            delay=0,
            stamp=1789592400,
            departure_time=1789592460,
        ),
        fetched_at=FETCHED,
    )
    row = next(reducer.batches()).to_pylist()[0]
    assert row['final_predicted_arrival_utc'] is None
    assert row['delay_s'] is None
    assert row['final_predicted_departure_utc'] == datetime(
        2026, 9, 16, 21, 1, tzinfo=UTC,
    )


def test_zero_arrival_keeps_the_earlier_arrival() -> None:
    """A blanked arrival does not erase the last real one.

    The departure from the same later poll still lands.
    """
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=SCHEDULED,
            arrival_time=1789592400,
            delay=120,
            stamp=1789592300,
            departure_time=1789592410,
        ),
        fetched_at=FETCHED,
    )
    reducer.add(
        feed=build_feed(
            relationship=SCHEDULED,
            arrival_time=0,
            delay=0,
            stamp=1789592500,
            departure_time=1789592460,
        ),
        fetched_at=FETCHED,
    )
    row = next(reducer.batches()).to_pylist()[0]
    assert row['final_predicted_arrival_utc'] == datetime(
        2026, 9, 16, 21, 0, tzinfo=UTC,
    )
    assert row['delay_s'] == 120
    assert row['final_predicted_departure_utc'] == datetime(
        2026, 9, 16, 21, 1, tzinfo=UTC,
    )
    assert row['last_update_at_utc'] == datetime(
        2026, 9, 16, 21, 1, 40, tzinfo=UTC,
    )


def test_stop_rows_do_not_carry_trip_status() -> None:
    """Trip status lives in the trip partial alone."""
    assert 'trip_schedule_relationship' not in TRIP_STOP_SCHEMA.names


def test_arrival_update_time_marks_the_last_arrival_sent() -> None:
    """A later update without an arrival leaves its update time alone.

    Otherwise a stale arrival would be judged fresh by a departure-only
    update that came after it.
    """
    reducer = TripStopReducer()
    reducer.add(
        feed=build_feed(
            relationship=SCHEDULED, arrival_time=1789592400, delay=120,
            stamp=1789592300, departure_time=1789592410,
        ),
        fetched_at=FETCHED,
    )
    reducer.add(
        feed=build_feed(
            relationship=SCHEDULED, arrival_time=0, delay=0,
            stamp=1789592500, departure_time=1789592460,
        ),
        fetched_at=FETCHED,
    )
    row = next(reducer.batches()).to_pylist()[0]
    assert row['arrival_updated_at_utc'] == datetime(
        2026, 9, 16, 20, 58, 20, tzinfo=UTC,
    )
    assert row['last_update_at_utc'] == datetime(
        2026, 9, 16, 21, 1, 40, tzinfo=UTC,
    )
