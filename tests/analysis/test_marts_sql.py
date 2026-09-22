"""Tests for the analysis views in ``analysis/marts.sql``."""

from datetime import date
from pathlib import Path

import duckdb
import pytest

MARTS = Path(__file__).parents[2] / 'analysis' / 'marts.sql'
SEED = Path(__file__).parents[2] / 'analysis' / 'calendar_exclusions_2026.csv'


@pytest.fixture(name='con', scope='module')
def _con() -> duckdb.DuckDBPyConnection:
    """Run the views over the real seed and small route fixtures."""
    con = duckdb.connect()
    con.execute(
        'create view calendar_exclusion_seed as '
        f"select * from read_csv('{SEED}')",
    )
    con.execute(
        "create table dim_route as select * from (values "
        "('r_school', 'S265', 'Kirrawee PS to Kirrawee HS', '712'), "
        "('r_bus', '400', 'Bondi to Burwood', '700'), "
        "('r_named', '753', 'Warabrook to Corpus Christi School', '700')"
        ') as t(route_id, route_short_name, route_long_name, route_type)',
    )
    con.execute(
        "create table dim_trip as select * from (values "
        "('t1', 'r_school'), ('t2', 'r_bus'), ('t3', 'r_named')"
        ') as t(trip_id, route_id)',
    )
    con.execute(MARTS.read_text(encoding='utf-8'))
    return con


def test_calendar_exclusion_expands_ranges(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        "select count(*) from calendar_exclusion "
        "where reason = 'Spring holidays'",
    ).fetchone()
    assert rows is not None
    assert rows[0] == 12


def test_calendar_exclusion_keeps_both_reasons_for_a_date(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select exclusion_type from calendar_exclusion '
        "where day = '2026-10-05' order by exclusion_type",
    ).fetchall()
    assert rows == [('public_holiday',), ('school_holiday',)]


def test_term_weekday_excludes_the_spring_holidays(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select day from term_weekday '
        "where day between '2026-09-24' and '2026-10-14' order by day",
    ).fetchall()
    assert [row[0] for row in rows] == [
        date(2026, 9, 24),
        date(2026, 9, 25),
        date(2026, 10, 13),
        date(2026, 10, 14),
    ]


def test_term_weekday_counts_the_collection_window(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select count(*) from term_weekday '
        "where day between '2026-09-21' and '2026-11-06'",
    ).fetchone()
    assert rows is not None
    assert rows[0] == 24


def test_school_route_uses_the_declared_type(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute('select route_id from school_route').fetchall()
    assert rows == [('r_school',)]


def test_school_trip_follows_its_route(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute('select trip_id from school_trip').fetchall()
    assert rows == [('t1',)]
