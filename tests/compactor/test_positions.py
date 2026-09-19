"""Tests for vehicle position extraction and dedupe."""

from datetime import UTC, datetime
from typing import Final

from google.transit import gtfs_realtime_pb2

from compactor.positions import PositionDeduper, position_batches

FETCHED: datetime = datetime(2026, 9, 16, 21, 0, 4, tzinfo=UTC)
OBSERVED: int = 1789592400
SAMPLE_COUNT: Final[int] = 50


def build_feed(
    *,
    samples: list[tuple[str, str, int, float, float]],
) -> object:
    """Build a FeedMessage of vehicle positions.

    Parameters
    ----------
    samples : list[tuple[str, str, int, float, float]]
        Tuples of vehicle id, vehicle label, timestamp, latitude and
        longitude.

    Returns
    -------
    object
        A populated FeedMessage.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = '1.0'
    for vehicle_id, label, stamp, lat, lon in samples:
        entity = feed.entity.add()
        entity.id = vehicle_id
        entity.vehicle.vehicle.id = vehicle_id
        entity.vehicle.vehicle.label = label
        entity.vehicle.trip.trip_id = '1012281'
        entity.vehicle.trip.route_id = '2447_160'
        entity.vehicle.timestamp = stamp
        entity.vehicle.position.latitude = lat
        entity.vehicle.position.longitude = lon
    return feed


def test_identical_repeat_is_collapsed() -> None:
    """A stale restatement of the same position yields one row."""
    deduper = PositionDeduper()
    rows = []
    for _ in range(2):
        rows.extend(deduper.rows(
            feed=build_feed(
                samples=[('8183_a', '8183', OBSERVED, -33.8, 151.2)],
            ),
            fetched_at=FETCHED,
        ))
    assert len(rows) == 1
    assert deduper.collapsed == 1


def test_same_timestamp_different_position_is_kept() -> None:
    """The measured 526-per-hour case must not be discarded.

    Same vehicle, same timestamp, moved position. Deduping on the
    pair alone would silently drop a real position change.
    """
    deduper = PositionDeduper()
    rows = list(deduper.rows(
        feed=build_feed(
            samples=[('8183_a', '8183', OBSERVED, -33.8, 151.2)],
        ),
        fetched_at=FETCHED,
    ))
    rows.extend(deduper.rows(
        feed=build_feed(
            samples=[('8183_a', '8183', OBSERVED, -33.9, 151.3)],
        ),
        fetched_at=FETCHED,
    ))
    assert len(rows) == 2
    assert deduper.differing_position == 1


def test_unset_current_status_is_null() -> None:
    """current_status is never set upstream and must never be invented."""
    deduper = PositionDeduper()
    rows = list(deduper.rows(
        feed=build_feed(
            samples=[('8183_a', '8183', OBSERVED, -33.8, 151.2)],
        ),
        fetched_at=FETCHED,
    ))
    assert rows[0]['current_status'] is None


def test_vehicle_label_is_separate_from_vehicle_id() -> None:
    """The fleet number is stored alongside the trip-instance token."""
    deduper = PositionDeduper()
    rows = list(deduper.rows(
        feed=build_feed(
            samples=[('8183_a', '8183', OBSERVED, -33.8, 151.2)],
        ),
        fetched_at=FETCHED,
    ))
    assert rows[0]['vehicle_id'] == '8183_a'
    assert rows[0]['vehicle_label'] == '8183'


def test_null_island_is_flagged_not_dropped() -> None:
    """Exactly (0,0) is recorded with a flag rather than silently kept.

    Measured at 0.12% of entities. position HasField is true, so a
    presence check will not catch it.
    """
    deduper = PositionDeduper()
    rows = list(deduper.rows(
        feed=build_feed(samples=[('8183_a', '8183', OBSERVED, 0.0, 0.0)]),
        fetched_at=FETCHED,
    ))
    assert rows[0]['null_island'] is True


def test_missing_position_yields_null_fields() -> None:
    """A vehicle with no position sub-message reports all-null fields."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = '1.0'
    entity = feed.entity.add()
    entity.id = '8183_a'
    entity.vehicle.vehicle.id = '8183_a'
    entity.vehicle.vehicle.label = '8183'
    entity.vehicle.trip.trip_id = '1012281'
    entity.vehicle.trip.route_id = '2447_160'
    entity.vehicle.timestamp = OBSERVED
    deduper = PositionDeduper()
    rows = list(deduper.rows(feed=feed, fetched_at=FETCHED))
    assert rows[0]['lat'] is None
    assert rows[0]['null_island'] is False


def test_non_vehicle_entity_is_skipped() -> None:
    """An entity without a vehicle sub-message yields no row."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = '1.0'
    entity = feed.entity.add()
    entity.id = 'trip_update_only'
    entity.trip_update.trip.trip_id = '1012281'
    deduper = PositionDeduper()
    rows = list(deduper.rows(feed=feed, fetched_at=FETCHED))
    assert rows == []


def test_position_age_measured_against_fetch_time() -> None:
    """Age is fetch time minus the entity's own timestamp."""
    deduper = PositionDeduper()
    rows = list(deduper.rows(
        feed=build_feed(
            samples=[('8183_a', '8183', OBSERVED, -33.8, 151.2)],
        ),
        fetched_at=FETCHED,
    ))
    assert rows[0]['position_age_s'] == 4.0


def test_position_batches_emit_arrow_batches() -> None:
    """Accepted rows arrive as bounded RecordBatches, not one list."""
    deduper = PositionDeduper()
    rows = deduper.rows(
        feed=build_feed(
            samples=[('8183_a', '8183', OBSERVED, -33.8, 151.2)],
        ),
        fetched_at=FETCHED,
    )
    batches = list(position_batches(records=rows, batch_size=1))
    assert sum(batch.num_rows for batch in batches) == 1
    assert batches[0].column('vehicle_id').to_pylist() == ['8183_a']


def test_deduper_holds_keys_not_rows() -> None:
    """Memory scales with distinct keys, not with row content.

    Holding a row dict per observation measured 778-1,048 bytes each,
    or 845-1,138 MB across a peak hour's 1,086,002 rows - enough to
    force a 2 GB Lambda tier permanently. Keys alone are ~256 bytes.
    """
    deduper = PositionDeduper()
    consumed = sum(
        1 for _ in deduper.rows(
            feed=build_feed(samples=[
                (f'v{index}', '8183', OBSERVED, -33.8, 151.2)
                for index in range(SAMPLE_COUNT)
            ]),
            fetched_at=FETCHED,
        )
    )
    assert consumed == SAMPLE_COUNT
    assert not hasattr(deduper, 'rows_by_key')
    assert len(deduper.seen) == SAMPLE_COUNT
