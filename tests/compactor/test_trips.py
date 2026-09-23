"""Tests for the trip-level status reduction."""

from datetime import UTC, datetime, timedelta
from typing import Any

from google.transit import gtfs_realtime_pb2

from compactor.trips import TripStatusReducer

FETCHED: datetime = datetime(2026, 9, 16, 21, 0, 34, tzinfo=UTC)
TripRelationship = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship
SCHEDULED = TripRelationship.Value('SCHEDULED')
CANCELED = TripRelationship.Value('CANCELED')
ADDED = TripRelationship.Value('ADDED')


def add_update(
    *,
    feed: Any,
    relationship: int,
    trip_id: str = '1012281',
    with_stop: bool = False,
    with_vehicle: bool = False,
    start_time: str = '07:30:00',
) -> None:
    """Append one trip update to a FeedMessage.

    Parameters
    ----------
    feed : Any
        FeedMessage to append to.
    relationship : int
        Trip-level schedule relationship.
    trip_id : str
        Trip identifier, possibly empty.
    with_stop : bool
        Whether to attach one stop-time update.
    with_vehicle : bool
        Whether to attach a vehicle descriptor.
    start_time : str
        Feed start time of the trip.
    """
    entity = feed.entity.add()
    entity.id = f'tu-{len(feed.entity)}'
    update = entity.trip_update
    update.trip.trip_id = trip_id
    update.trip.route_id = '2447_160'
    update.trip.start_date = '20260917'
    update.trip.start_time = start_time
    update.trip.schedule_relationship = relationship
    if with_stop:
        stop = update.stop_time_update.add()
        stop.stop_id = '200013'
        stop.stop_sequence = 1
    if with_vehicle:
        update.vehicle.id = '8183_a'


def poll(*relationships: int, **options: Any) -> Any:
    """Build one FeedMessage holding a trip update per relationship.

    Parameters
    ----------
    *relationships : int
        Trip-level relationships, one update each.
    **options : Any
        Passed through to ``add_update``.

    Returns
    -------
    Any
        A populated FeedMessage.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = '1.0'
    for relationship in relationships:
        add_update(feed=feed, relationship=relationship, **options)
    return feed


def reduce_polls(*feeds: Any) -> list[dict[str, Any]]:
    """Reduce polls fetched a minute apart and return the rows.

    Parameters
    ----------
    *feeds : Any
        FeedMessages in fetch order.

    Returns
    -------
    list[dict[str, Any]]
        The reduced rows.
    """
    reducer = TripStatusReducer()
    for minute, feed in enumerate(feeds):
        reducer.add(
            feed=feed, fetched_at=FETCHED + timedelta(minutes=minute),
        )
    return [row for batch in reducer.batches() for row in batch.to_pylist()]


def test_canceled_update_without_stops_writes_a_row() -> None:
    """A CANCELED update carries no stops and must still be recorded."""
    rows = reduce_polls(poll(CANCELED))
    assert len(rows) == 1
    assert rows[0]['final_status'] == 'CANCELED'
    assert rows[0]['canceled_polls'] == 1
    assert rows[0]['first_canceled_at_utc'] == FETCHED


def test_final_status_is_the_latest_poll() -> None:
    """A reinstated trip ends SCHEDULED, but its cancellation is kept."""
    rows = reduce_polls(poll(SCHEDULED), poll(CANCELED), poll(SCHEDULED))
    row = rows[0]
    assert row['final_status'] == 'SCHEDULED'
    assert row['final_status_at_utc'] == FETCHED + timedelta(minutes=2)
    assert row['scheduled_polls'] == 2
    assert row['canceled_polls'] == 1
    assert row['first_canceled_at_utc'] == FETCHED + timedelta(minutes=1)
    assert row['last_canceled_at_utc'] == FETCHED + timedelta(minutes=1)


def test_seen_range_spans_every_poll() -> None:
    """First and last seen cover the trip's whole presence in the hour."""
    rows = reduce_polls(poll(SCHEDULED), poll(CANCELED), poll(CANCELED))
    assert rows[0]['first_seen_at_utc'] == FETCHED
    assert rows[0]['last_seen_at_utc'] == FETCHED + timedelta(minutes=2)
    assert rows[0]['last_canceled_at_utc'] == (
        FETCHED + timedelta(minutes=2)
    )


def test_same_poll_tie_prefers_scheduled_over_added() -> None:
    """ADDED and SCHEDULED can share a trip key in one poll.

    Both are counted, and the final status does not depend on the
    order the two updates appear in.
    """
    for order in ((ADDED, SCHEDULED), (SCHEDULED, ADDED)):
        row = reduce_polls(poll(*order))[0]
        assert row['final_status'] == 'SCHEDULED'
        assert row['scheduled_polls'] == 1
        assert row['added_polls'] == 1


def test_same_poll_tie_prefers_canceled() -> None:
    """CANCELED outranks SCHEDULED within one poll."""
    row = reduce_polls(poll(SCHEDULED, CANCELED))[0]
    assert row['final_status'] == 'CANCELED'


def test_status_is_counted_once_per_poll() -> None:
    """A trip repeated within one poll is still one poll."""
    row = reduce_polls(poll(SCHEDULED, SCHEDULED))[0]
    assert row['scheduled_polls'] == 1


def test_empty_trip_id_is_skipped() -> None:
    """An update with no trip_id cannot be keyed to a trip."""
    assert not reduce_polls(poll(SCHEDULED, trip_id=''))


def test_had_vehicle_is_set_by_any_poll() -> None:
    """One poll with a vehicle attached is enough."""
    rows = reduce_polls(
        poll(SCHEDULED, with_vehicle=True), poll(CANCELED),
    )
    assert rows[0]['had_vehicle'] is True


def test_descriptor_fields_come_from_the_latest_poll() -> None:
    """start_time is taken from the most recent poll."""
    rows = reduce_polls(
        poll(SCHEDULED, start_time='07:30:00'),
        poll(SCHEDULED, start_time='07:31:00'),
    )
    assert rows[0]['start_time'] == '07:31:00'
    assert rows[0]['start_date'] == '20260917'
    assert rows[0]['route_id'] == '2447_160'


def test_stop_updates_do_not_change_the_trip_row() -> None:
    """The trip row is the same whether or not stops are attached."""
    with_stops = reduce_polls(poll(SCHEDULED, with_stop=True))
    without = reduce_polls(poll(SCHEDULED))
    assert with_stops == without


def test_unset_status_is_seen_but_not_counted() -> None:
    """A trip with no relationship sent is recorded without a status."""
    feed = poll(SCHEDULED)
    feed.entity[0].trip_update.trip.ClearField('schedule_relationship')
    row = reduce_polls(feed)[0]
    assert row['final_status'] is None
    assert row['scheduled_polls'] == 0
    assert row['first_seen_at_utc'] == FETCHED


def test_out_of_order_poll_does_not_replace_the_final_status() -> None:
    """An older poll reduced after a newer one leaves the status alone."""
    reducer = TripStatusReducer()
    later = FETCHED + timedelta(minutes=5)
    reducer.add(feed=poll(CANCELED), fetched_at=later)
    reducer.add(feed=poll(SCHEDULED), fetched_at=FETCHED)
    row = next(reducer.batches()).to_pylist()[0]
    assert row['final_status'] == 'CANCELED'
    assert row['first_seen_at_utc'] == FETCHED
    assert row['last_seen_at_utc'] == later


def test_unlisted_status_loses_a_same_poll_tie() -> None:
    """A status outside the precedence list never outranks a listed one."""
    duplicated = TripRelationship.Value('DUPLICATED')
    for order in ((duplicated, ADDED), (ADDED, duplicated)):
        assert reduce_polls(poll(*order))[0]['final_status'] == 'ADDED'
