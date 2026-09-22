"""Asking a local OSRM server for walking distances.

The table service answers one origin against many destinations in a
single request, so the work is one request per stop rather than one
per pair. That is what makes routing a few million pairs affordable.

Snapping is checked after the fact rather than constrained in the
request. OSRM accepts a ``radiuses`` parameter that rejects
coordinates too far from the network, but a single unsnappable
destination then fails the whole request, losing every other
destination with it. The response reports how far each coordinate
moved, so the same limit is applied to the answer instead.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

TABLE_PATH: Final[str] = '/table/v1/foot/'
DESTINATION_OFFSET: Final[int] = 1
"""Where the first real destination sits in the response arrays.

The request sends the origin as coordinate zero and every destination
after it. Restricting ``sources`` does not shorten the destination
side, so the arrays that come back still begin with the origin
measured against itself. Reading them from the start yields a
distance of zero for the first destination, which looks like an
adjacent mesh block rather than like a bug.
"""

MAX_SNAP_M: Final[float] = 100.0
"""How far a coordinate may move onto the network before it is refused.

Beyond this the returned distance describes a walk between two places
that are not the ones asked about.
"""


class RoutingStatus(StrEnum):
    """What happened when one pair was routed."""

    ROUTED = 'routed'
    UNROUTABLE = 'unroutable'
    SNAP_FAILED = 'snap_failed'
    NOT_ATTEMPTED = 'not_attempted'


@dataclass(frozen=True, slots=True)
class Place:
    """A point to route from or to."""

    identifier: str
    latitude: float
    longitude: float


@dataclass(frozen=True, slots=True)
class Measurement:
    """What OSRM reported for one destination."""

    distance_m: float | None
    duration_s: float | None
    snap_m: float
    origin_snap_m: float


@dataclass(frozen=True, slots=True)
class Leg:
    """One routed pair."""

    origin_id: str
    destination_id: str
    distance_m: float | None
    duration_s: float | None
    snap_distance_m: float | None
    status: RoutingStatus


def table_url(*, base_url: str, origin: Place, destinations: list[Place]) -> str:
    """Build a table request for one origin and its destinations.

    Parameters
    ----------
    base_url : str
        Root of the OSRM server, such as ``http://localhost:5001``.
    origin : Place
        The point to measure from.
    destinations : list[Place]
        Points to measure to.

    Returns
    -------
    str
        A complete request URL.

    Raises
    ------
    ValueError
        If there are no destinations.
    """
    if not destinations:
        raise ValueError(f'no destinations for {origin.identifier}')
    points = ';'.join(
        f'{place.longitude},{place.latitude}'
        for place in [origin, *destinations]
    )
    return (
        f'{base_url}{TABLE_PATH}{points}'
        '?sources=0&annotations=distance,duration'
    )


def snap_distances(*, payload: dict[str, Any]) -> list[float]:
    """Read how far each destination moved onto the network.

    Parameters
    ----------
    payload : dict[str, Any]
        Decoded table response.

    Returns
    -------
    list[float]
        Snap distance in metres, destination by destination.
    """
    return [
        float(point.get('distance', 0.0))
        for point in payload.get('destinations', [])
    ]


def origin_snap(*, payload: dict[str, Any]) -> float:
    """Read how far the origin moved onto the network.

    Parameters
    ----------
    payload : dict[str, Any]
        Decoded table response.

    Returns
    -------
    float
        Snap distance in metres, or zero when absent.
    """
    sources = payload.get('sources') or [{}]
    return float(sources[0].get('distance', 0.0))


def status_for(
    *,
    distance: float | None,
    snap_m: float,
    origin_snap_m: float,
) -> RoutingStatus:
    """Decide whether one answer can be trusted.

    Parameters
    ----------
    distance : float | None
        Distance OSRM returned, or None when it found no path.
    snap_m : float
        How far the destination moved onto the network.
    origin_snap_m : float
        How far the origin moved onto the network.

    Returns
    -------
    RoutingStatus
        Why the answer is or is not usable.
    """
    if max(snap_m, origin_snap_m) > MAX_SNAP_M:
        return RoutingStatus.SNAP_FAILED
    if distance is None:
        return RoutingStatus.UNROUTABLE
    return RoutingStatus.ROUTED


def legs_from(
    *,
    payload: dict[str, Any],
    origin: Place,
    destinations: list[Place],
) -> list[Leg]:
    """Turn one table response into routed pairs.

    Parameters
    ----------
    payload : dict[str, Any]
        Decoded table response.
    origin : Place
        The point measured from.
    destinations : list[Place]
        Points measured to, in request order.

    Returns
    -------
    list[Leg]
        One leg per destination.
    """
    distances = (payload.get('distances') or [[]])[0]
    durations = (payload.get('durations') or [[]])[0]
    snaps = snap_distances(payload=payload)
    from_snap = origin_snap(payload=payload)
    return [
        _leg(
            origin=origin,
            destination=destination,
            measured=Measurement(
                distance_m=_at(
                    values=distances, index=index + DESTINATION_OFFSET,
                ),
                duration_s=_at(
                    values=durations, index=index + DESTINATION_OFFSET,
                ),
                snap_m=_at(
                    values=snaps, index=index + DESTINATION_OFFSET,
                ) or 0.0,
                origin_snap_m=from_snap,
            ),
        )
        for index, destination in enumerate(destinations)
    ]


def _at(*, values: list[Any], index: int) -> float | None:
    """Read one value from a response array.

    A malformed or truncated response would otherwise raise rather
    than marking the pair unusable.

    Parameters
    ----------
    values : list[Any]
        Array from the response.
    index : int
        Position to read.

    Returns
    -------
    float | None
        The value, or None when absent or null.
    """
    if index >= len(values):
        return None
    value = values[index]
    return None if value is None else float(value)


def _leg(
    *,
    origin: Place,
    destination: Place,
    measured: Measurement,
) -> Leg:
    """Build one routed pair.

    Parameters
    ----------
    origin : Place
        The point measured from.
    destination : Place
        The point measured to.
    measured : Measurement
        What OSRM reported for this destination.

    Returns
    -------
    Leg
        The pair with its status.
    """
    status = status_for(
        distance=measured.distance_m,
        snap_m=measured.snap_m,
        origin_snap_m=measured.origin_snap_m,
    )
    usable = status is RoutingStatus.ROUTED
    return Leg(
        origin_id=origin.identifier,
        destination_id=destination.identifier,
        distance_m=measured.distance_m if usable else None,
        duration_s=measured.duration_s if usable else None,
        snap_distance_m=max(measured.snap_m, measured.origin_snap_m),
        status=status,
    )
