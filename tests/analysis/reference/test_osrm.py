"""Tests for reading OSRM table responses."""

from typing import Any

import pytest

from analysis.reference.osrm import (
    MAX_SNAP_M,
    Leg,
    Place,
    RoutingStatus,
    legs_from,
    origin_snap,
    snap_distances,
    status_for,
    table_url,
)

ORIGIN = Place(identifier='stop', latitude=-33.8732, longitude=151.2065)
NEAR = Place(identifier='mb1', latitude=-33.8688, longitude=151.2093)
FAR = Place(identifier='mb2', latitude=-33.8657, longitude=151.2058)


def _payload(
    *,
    distances: list[Any],
    snaps: list[float],
    durations: list[Any] | None = None,
) -> dict[str, Any]:
    """Build a table response in OSRM's shape.

    The origin is the first entry on the destination side, as OSRM
    returns it even when sources are restricted.
    """
    return {
        'code': 'Ok',
        'distances': [distances],
        'durations': [durations if durations is not None else distances],
        'sources': [{'distance': snaps[0]}],
        'destinations': [{'distance': snap} for snap in snaps],
    }


def test_table_url_puts_longitude_first() -> None:
    url = table_url(
        base_url='http://x', origin=ORIGIN, destinations=[NEAR],
    )
    assert '151.2065,-33.8732;151.2093,-33.8688' in url


def test_table_url_requests_one_source() -> None:
    url = table_url(
        base_url='http://x', origin=ORIGIN, destinations=[NEAR],
    )
    assert 'sources=0' in url
    assert 'annotations=distance,duration' in url


def test_table_url_without_destinations() -> None:
    with pytest.raises(ValueError, match='no destinations'):
        table_url(base_url='http://x', origin=ORIGIN, destinations=[])


def test_the_origin_is_skipped_on_the_destination_side() -> None:
    payload = _payload(distances=[0, 846.0, 913.7], snaps=[19.2, 19.2, 3.8])
    legs = legs_from(
        payload=payload, origin=ORIGIN, destinations=[NEAR, FAR],
    )
    assert [leg.distance_m for leg in legs] == [846.0, 913.7]


def test_a_routed_leg_is_marked_routed() -> None:
    payload = _payload(distances=[0, 846.0], snaps=[19.2, 3.8])
    legs = legs_from(payload=payload, origin=ORIGIN, destinations=[NEAR])
    assert legs[0].status is RoutingStatus.ROUTED


def test_an_unreachable_leg_is_unroutable() -> None:
    payload = _payload(distances=[0, None], snaps=[19.2, 3.8])
    legs = legs_from(payload=payload, origin=ORIGIN, destinations=[NEAR])
    assert legs[0].status is RoutingStatus.UNROUTABLE
    assert legs[0].distance_m is None


def test_a_far_snapped_destination_is_refused() -> None:
    payload = _payload(distances=[0, 846.0], snaps=[19.2, MAX_SNAP_M + 1])
    legs = legs_from(payload=payload, origin=ORIGIN, destinations=[NEAR])
    assert legs[0].status is RoutingStatus.SNAP_FAILED
    assert legs[0].distance_m is None


def test_a_far_snapped_origin_refuses_every_leg() -> None:
    payload = _payload(
        distances=[0, 846.0, 913.7],
        snaps=[MAX_SNAP_M + 50, 3.8, 4.1],
    )
    legs = legs_from(
        payload=payload, origin=ORIGIN, destinations=[NEAR, FAR],
    )
    assert {leg.status for leg in legs} == {RoutingStatus.SNAP_FAILED}


def test_a_refused_leg_still_records_how_far_it_snapped() -> None:
    payload = _payload(distances=[0, 846.0], snaps=[19.2, 250.0])
    legs = legs_from(payload=payload, origin=ORIGIN, destinations=[NEAR])
    assert legs[0].snap_distance_m == 250.0


def test_a_truncated_response_does_not_raise() -> None:
    payload = _payload(distances=[0], snaps=[19.2])
    legs = legs_from(
        payload=payload, origin=ORIGIN, destinations=[NEAR, FAR],
    )
    assert {leg.status for leg in legs} == {RoutingStatus.UNROUTABLE}


def test_legs_keep_their_identifiers() -> None:
    payload = _payload(distances=[0, 846.0, 913.7], snaps=[19.2, 3.8, 4.1])
    legs = legs_from(
        payload=payload, origin=ORIGIN, destinations=[NEAR, FAR],
    )
    assert [leg.destination_id for leg in legs] == ['mb1', 'mb2']
    assert {leg.origin_id for leg in legs} == {'stop'}


def test_snap_distances_reads_every_destination() -> None:
    payload = _payload(distances=[0, 1.0], snaps=[19.2, 3.8])
    assert snap_distances(payload=payload) == [19.2, 3.8]


def test_origin_snap_of_an_empty_response() -> None:
    assert origin_snap(payload={}) == 0.0


def test_status_prefers_snap_failure_over_unreachable() -> None:
    status = status_for(
        distance=None, snap_m=MAX_SNAP_M + 1, origin_snap_m=0.0,
    )
    assert status is RoutingStatus.SNAP_FAILED


def test_leg_is_hashable_and_frozen() -> None:
    leg = Leg(
        origin_id='a',
        destination_id='b',
        distance_m=1.0,
        duration_s=1.0,
        snap_distance_m=0.0,
        status=RoutingStatus.ROUTED,
    )
    with pytest.raises(AttributeError):
        leg.distance_m = 2.0  # type: ignore[misc]
