"""Tests for the point-in-polygon views in ``stop_geography.sql``."""

from pathlib import Path

import duckdb
import pytest

SQL = (
    Path(__file__).parents[3]
    / 'analysis' / 'reference' / 'sql' / 'stop_geography.sql'
)

INSIDE = ('inside', -33.870, 151.205)
TIE = ('tie', -33.880, 151.190)
OUTSIDE = ('outside', -33.900, 151.300)


@pytest.fixture(name='con', scope='module')
def _con() -> duckdb.DuckDBPyConnection:
    """Build two adjacent mesh blocks, an LGA, and three stops."""
    con = duckdb.connect()
    con.execute('INSTALL spatial; LOAD spatial;')
    con.execute(
        'create table mesh_block as select * from (values '
        "('10001', '1000101', '100011', 'West', '10001', 'Inner', "
        "'100', 'Sydney', '1GSYD', 'Greater Sydney', "
        "st_geomfromtext('POLYGON((151.19 -33.89, 151.21 -33.89, "
        "151.21 -33.86, 151.19 -33.86, 151.19 -33.89))')), "
        "('10002', '1000102', '100011', 'West', '10001', 'Inner', "
        "'100', 'Sydney', '1GSYD', 'Greater Sydney', "
        "st_geomfromtext('POLYGON((151.17 -33.89, 151.19 -33.89, "
        "151.19 -33.86, 151.17 -33.86, 151.17 -33.89))'))"
        ') as t(mesh_block_code, sa1_code, sa2_code, sa2_name, '
        'sa3_code, sa3_name, sa4_code, sa4_name, gccsa_code, '
        'gccsa_name, geom)',
    )
    con.execute(
        'create table lga as select * from (values '
        "('17200', 'Sydney', "
        "st_geomfromtext('POLYGON((151.15 -33.90, 151.25 -33.90, "
        "151.25 -33.85, 151.15 -33.85, 151.15 -33.90))'))"
        ') as t(lga_code, lga_name, geom)',
    )
    rows = ', '.join(
        f"('{name}', {lat}, {lon})" for name, lat, lon in
        (INSIDE, TIE, OUTSIDE)
    )
    con.execute(
        f'create table dim_stop as select * from (values {rows}) '
        'as t(stop_id, stop_lat, stop_lon)',
    )
    con.execute(SQL.read_text(encoding='utf-8'))
    return con


def test_a_stop_inside_one_polygon_matches_it(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select mesh_block_code, boundary_tie from stop_mesh_block '
        "where stop_id = 'inside'",
    ).fetchone()
    assert row == ('10001', False)


def test_codes_keep_their_leading_zeros(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        "select sa2_code from stop_mesh_block where stop_id = 'inside'",
    ).fetchone()
    assert row is not None
    assert row[0] == '100011'


def test_a_stop_on_a_shared_edge_is_flagged(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select boundary_tie from stop_mesh_block '
        "where stop_id = 'tie'",
    ).fetchone()
    assert row == (True,)


def test_a_stop_outside_every_polygon_does_not_match(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        "select stop_id from stop_mesh_block where stop_id = 'outside'",
    ).fetchall()
    assert rows == []


def test_an_unmatched_stop_gets_its_nearest_polygon(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select mesh_block_code, distance_m from stop_nearest_mesh_block '
        "where stop_id = 'outside'",
    ).fetchone()
    assert row is not None
    assert row[0] == '10001'
    assert 8_000 < row[1] < 9_000


def test_spherical_distance_is_measured_latitude_first(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select st_distance_sphere('
        'st_point(-33.0, 151.0), st_point(-34.0, 151.0))',
    ).fetchone()
    assert row is not None
    assert 111_000 < row[0] < 111_400


def test_only_unmatched_stops_appear_in_the_nearest_view(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select stop_id from stop_nearest_mesh_block order by stop_id',
    ).fetchall()
    assert rows == [('outside',)]


def test_lga_is_assigned_separately(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select stop_id, lga_name from stop_lga order by stop_id',
    ).fetchall()
    assert rows == [('inside', 'Sydney'), ('tie', 'Sydney')]


def test_longitude_and_latitude_are_not_swapped(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select st_x(geom), st_y(geom) from stop_point '
        "where stop_id = 'inside'",
    ).fetchone()
    assert row == (151.205, -33.870)


def test_unmatched_stop_view_holds_only_unmatched_stops(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select stop_id from unmatched_stop order by stop_id',
    ).fetchall()
    assert rows == [('outside',)]
