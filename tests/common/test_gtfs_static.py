"""Tests for static GTFS bundle handling."""

import io
import tracemalloc
import zipfile
from typing import Final

import pytest

from src.common.gtfs_static import member_rows, zip_sha256

ROUTES: str = (
    '"route_id","agency_id","route_short_name"\n'
    '"2447_160","2447","160"\n'
    '"2449_S513","2449","S513"\n'
)
ROW_COUNT: Final[int] = 50_000
STREAMING_MARGIN: Final[int] = 4
EXPECTED_ROWS: Final[int] = 3


def build_zip(*, content: str, name: str = 'routes.txt') -> bytes:
    """Build a one-member zip archive in memory.

    Parameters
    ----------
    content : str
        Text body of the member.
    name : str
        Member filename.

    Returns
    -------
    bytes
        Zip archive bytes.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        archive.writestr(name, content)
    return buffer.getvalue()


def test_zip_sha256_is_stable() -> None:
    """The same bytes always hash the same."""
    payload = build_zip(content=ROUTES)
    assert zip_sha256(payload=payload) == zip_sha256(payload=payload)


def test_zip_sha256_differs_on_content_change() -> None:
    """Changed content changes the hash."""
    first = zip_sha256(payload=build_zip(content=ROUTES))
    second = zip_sha256(payload=build_zip(content=ROUTES + '"x","y","z"\n'))
    assert first != second


def test_member_rows_strips_quotes() -> None:
    """Every GTFS field arrives double-quoted and must be unwrapped."""
    with zipfile.ZipFile(io.BytesIO(build_zip(content=ROUTES))) as archive:
        rows = list(member_rows(archive=archive, name='routes.txt'))
    assert rows[0]['route_id'] == '2447_160'
    assert rows[1]['route_short_name'] == 'S513'


def test_member_rows_missing_member_raises() -> None:
    """A missing member raises rather than yielding nothing.

    A feed that changes shape must fail loudly, not silently produce a
    month of empty dimension snapshots.
    """
    with zipfile.ZipFile(io.BytesIO(build_zip(content=ROUTES))) as archive:
        with pytest.raises(KeyError):
            list(member_rows(archive=archive, name='stops.txt'))


def test_member_rows_streams_rather_than_loading() -> None:
    """Rows are produced without reading the whole member.

    The real ``stop_times.txt`` is 248 MB across 3,634,337 rows and
    ``shapes.txt`` is 252 MB, against a 512 MB Lambda. A refactor to
    ``list(csv.DictReader(...))`` would pass every other test here
    and then exhaust memory on the real bundle.
    """
    body = 'trip_id,arrival_time\n' + ''.join(
        f'"{index}","07:30:00"\n' for index in range(ROW_COUNT)
    )
    archive = zipfile.ZipFile(io.BytesIO(build_zip(
        content=body, name='stop_times.txt',
    )))
    tracemalloc.start()
    try:
        first = next(member_rows(
            archive=archive, name='stop_times.txt',
        ))
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert first['trip_id'] == '0'
    assert peak < len(body) // STREAMING_MARGIN


def test_member_rows_handles_real_world_field_shapes() -> None:
    """Quoted commas, newlines and a BOM all parse correctly.

    Real GTFS ``stop_name`` and ``trip_headsign`` values contain
    commas, so a parser that split on commas naively would shear
    rows apart and shift every later column by one.
    """
    body = (
        'stop_id,stop_name\n'
        '"200013","Gordon Station, Stand A"\n'
        '"200014","Railway Pde opp\nthe park"\n'
        '"200015","O\'Riordan St"\n'
    )
    archive = zipfile.ZipFile(io.BytesIO(build_zip(
        content=f'﻿{body}', name='stops.txt',
    )))
    rows = list(member_rows(archive=archive, name='stops.txt'))
    assert len(rows) == EXPECTED_ROWS
    assert list(rows[0]) == ['stop_id', 'stop_name']
    assert rows[0]['stop_name'] == 'Gordon Station, Stand A'
    assert rows[1]['stop_name'] == 'Railway Pde opp\nthe park'
    assert rows[2]['stop_name'] == "O'Riordan St"
