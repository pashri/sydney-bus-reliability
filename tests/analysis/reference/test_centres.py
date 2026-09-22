"""Tests for the major centre lookup and distance measures."""

import pytest

from analysis.reference.centres import (
    SYDNEY_CBD,
    Centre,
    CentreTier,
    distance_to_centre,
    find_centre,
    haversine_m,
    load_centres,
    nearest_centre,
)

PARRAMATTA_STOP = (-33.8150, 151.0050)
CIRCULAR_QUAY = (-33.8610, 151.2105)


def test_load_centres_has_the_sydney_cbd() -> None:
    centres = load_centres()
    assert find_centre(centres=centres, centre_id=SYDNEY_CBD).tier is (
        CentreTier.PRIMARY
    )


def test_load_centres_ids_are_unique() -> None:
    ids = [centre.centre_id for centre in load_centres()]
    assert len(ids) == len(set(ids))


def test_load_centres_coordinates_are_in_nsw() -> None:
    for centre in load_centres():
        assert -35.0 < centre.latitude < -32.0
        assert 150.0 < centre.longitude < 152.5


def test_find_centre_missing() -> None:
    with pytest.raises(KeyError, match='no centre with id'):
        find_centre(centres=load_centres(), centre_id='hobart')


def test_haversine_of_a_point_with_itself_is_zero() -> None:
    assert haversine_m(
        from_lat=-33.86,
        from_lon=151.21,
        to_lat=-33.86,
        to_lon=151.21,
    ) == 0.0


def test_haversine_is_symmetric() -> None:
    there = haversine_m(
        from_lat=-33.8688,
        from_lon=151.2093,
        to_lat=-33.8150,
        to_lon=151.0011,
    )
    back = haversine_m(
        from_lat=-33.8150,
        from_lon=151.0011,
        to_lat=-33.8688,
        to_lon=151.2093,
    )
    assert there == pytest.approx(back)


def test_haversine_sydney_to_parramatta_is_about_20km() -> None:
    metres = haversine_m(
        from_lat=-33.8688,
        from_lon=151.2093,
        to_lat=-33.8150,
        to_lon=151.0011,
    )
    assert 18_000 < metres < 22_000


def test_haversine_is_metres_not_degrees() -> None:
    one_degree_of_latitude = haversine_m(
        from_lat=-33.0,
        from_lon=151.0,
        to_lat=-34.0,
        to_lon=151.0,
    )
    assert 110_000 < one_degree_of_latitude < 112_000


def test_distance_to_centre_uses_the_given_centre() -> None:
    centres = load_centres()
    sydney = find_centre(centres=centres, centre_id=SYDNEY_CBD)
    metres = distance_to_centre(
        latitude=CIRCULAR_QUAY[0],
        longitude=CIRCULAR_QUAY[1],
        centre=sydney,
    )
    assert metres < 2_000


def test_nearest_centre_prefers_parramatta_over_sydney() -> None:
    found = nearest_centre(
        latitude=PARRAMATTA_STOP[0],
        longitude=PARRAMATTA_STOP[1],
        centres=load_centres(),
    )
    assert found.centre.centre_id == 'parramatta'
    assert found.distance_m < 1_000


def test_nearest_centre_for_a_city_stop_is_sydney() -> None:
    found = nearest_centre(
        latitude=CIRCULAR_QUAY[0],
        longitude=CIRCULAR_QUAY[1],
        centres=load_centres(),
    )
    assert found.centre.centre_id == SYDNEY_CBD


def test_nearest_centre_for_newcastle_is_not_sydney() -> None:
    found = nearest_centre(
        latitude=-32.9272,
        longitude=151.7761,
        centres=load_centres(),
    )
    assert found.centre.centre_id == 'newcastle'


def test_nearest_centre_without_candidates() -> None:
    with pytest.raises(ValueError, match='no centres'):
        nearest_centre(latitude=-33.0, longitude=151.0, centres=[])


def test_nearest_centre_returns_the_minimum() -> None:
    near = Centre(
        centre_id='near',
        centre_name='Near',
        tier=CentreTier.STRATEGIC,
        latitude=-33.0,
        longitude=151.0,
        source='test',
    )
    far = Centre(
        centre_id='far',
        centre_name='Far',
        tier=CentreTier.REGIONAL,
        latitude=-34.0,
        longitude=151.0,
        source='test',
    )
    found = nearest_centre(
        latitude=-33.01,
        longitude=151.0,
        centres=[far, near],
    )
    assert found.centre.centre_id == 'near'
