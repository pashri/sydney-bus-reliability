"""Major centres, and how far a stop is from one.

Distance from the Sydney CBD alone describes a monocentric city. This
one is not: Parramatta, Liverpool, Penrith and Chatswood are
destinations in their own right, and treating every stop as peripheral
in proportion to its distance from Martin Place misdescribes exactly
the areas a geographic comparison is about. Newcastle, Wollongong and
Gosford stops are further still, and their distance to Sydney says
nothing useful at all.

So a stop carries both: its distance to the Sydney CBD, and its
distance to whichever centre is nearest, named.

The list is hand-maintained. It is a judgement about which places are
centres, not a fact derived from data, and it belongs somewhere a
reader can see and argue with.

Each centre is placed at its main transport interchange rather than at
a survey datum such as the GPO, because the distance stands in for how
far a passenger is from somewhere the network takes them. The two
conventions sit a few hundred metres apart, which does not matter
against distances measured in tens of kilometres.
"""

import csv
from dataclasses import dataclass
from enum import StrEnum
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from typing import Final

SEED_PATH: Final[Path] = Path(__file__).parent / 'centres.csv'
"""The committed centre list."""

EARTH_RADIUS_M: Final[float] = 6_371_008.8
"""Mean Earth radius in metres, as used for spherical distances."""

SYDNEY_CBD: Final[str] = 'sydney'
"""Identifier of the centre treated as the Sydney CBD."""


class CentreTier(StrEnum):
    """How a centre is classified."""

    PRIMARY = 'primary'
    STRATEGIC = 'strategic'
    REGIONAL = 'regional'


@dataclass(frozen=True, slots=True)
class Centre:
    """One major centre."""

    centre_id: str
    centre_name: str
    tier: CentreTier
    latitude: float
    longitude: float
    source: str


@dataclass(frozen=True, slots=True)
class NearestCentre:
    """The closest centre to a point, and how far away it is."""

    centre: Centre
    distance_m: float


def load_centres(*, path: Path | None = None) -> list[Centre]:
    """Read the committed centre list.

    Parameters
    ----------
    path : Path | None, optional
        Seed file to read. Defaults to the committed one.

    Returns
    -------
    list[Centre]
        Centres in seed order.

    Raises
    ------
    FileNotFoundError
        If the seed file does not exist.
    ValueError
        If a row has an unrecognised tier or unparseable coordinates.
    """
    seed = path or SEED_PATH
    with seed.open(newline='', encoding='utf-8') as handle:
        return [_centre(row=row) for row in csv.DictReader(handle)]


def _centre(*, row: dict[str, str]) -> Centre:
    """Build one centre from a seed row.

    Parameters
    ----------
    row : dict[str, str]
        One row of the seed file.

    Returns
    -------
    Centre
        The parsed centre.
    """
    return Centre(
        centre_id=row['centre_id'],
        centre_name=row['centre_name'],
        tier=CentreTier(row['tier']),
        latitude=float(row['latitude']),
        longitude=float(row['longitude']),
        source=row['source'],
    )


def haversine_m(
    *,
    from_lat: float,
    from_lon: float,
    to_lat: float,
    to_lon: float,
) -> float:
    """Measure the great-circle distance between two points.

    This is a straight line over the surface of a sphere. It is not a
    walking distance, and in a city cut by water and motorways the two
    differ by much more than the usual detour factor.

    Parameters
    ----------
    from_lat : float
        Latitude of the first point, decimal degrees.
    from_lon : float
        Longitude of the first point, decimal degrees.
    to_lat : float
        Latitude of the second point, decimal degrees.
    to_lon : float
        Longitude of the second point, decimal degrees.

    Returns
    -------
    float
        Distance in metres.
    """
    lat_delta = radians(to_lat - from_lat)
    lon_delta = radians(to_lon - from_lon)
    chord = (
        sin(lat_delta / 2) ** 2
        + cos(radians(from_lat)) * cos(radians(to_lat))
        * sin(lon_delta / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * asin(sqrt(chord))


def distance_to_centre(
    *,
    latitude: float,
    longitude: float,
    centre: Centre,
) -> float:
    """Measure how far a point is from one centre.

    Parameters
    ----------
    latitude : float
        Latitude of the point, decimal degrees.
    longitude : float
        Longitude of the point, decimal degrees.
    centre : Centre
        Centre to measure to.

    Returns
    -------
    float
        Distance in metres.
    """
    return haversine_m(
        from_lat=latitude,
        from_lon=longitude,
        to_lat=centre.latitude,
        to_lon=centre.longitude,
    )


def nearest_centre(
    *,
    latitude: float,
    longitude: float,
    centres: list[Centre],
) -> NearestCentre:
    """Find the closest centre to a point.

    Parameters
    ----------
    latitude : float
        Latitude of the point, decimal degrees.
    longitude : float
        Longitude of the point, decimal degrees.
    centres : list[Centre]
        Centres to consider.

    Returns
    -------
    NearestCentre
        The closest centre and its distance in metres.

    Raises
    ------
    ValueError
        If no centres are given.
    """
    if not centres:
        raise ValueError('no centres to choose from')
    measured = (
        NearestCentre(
            centre=centre,
            distance_m=distance_to_centre(
                latitude=latitude,
                longitude=longitude,
                centre=centre,
            ),
        )
        for centre in centres
    )
    return min(measured, key=lambda found: found.distance_m)


def find_centre(*, centres: list[Centre], centre_id: str) -> Centre:
    """Look one centre up by identifier.

    Parameters
    ----------
    centres : list[Centre]
        Centres to search.
    centre_id : str
        Identifier to find.

    Returns
    -------
    Centre
        The matching centre.

    Raises
    ------
    KeyError
        If no centre has that identifier.
    """
    for centre in centres:
        if centre.centre_id == centre_id:
            return centre
    raise KeyError(f'no centre with id {centre_id!r}')
