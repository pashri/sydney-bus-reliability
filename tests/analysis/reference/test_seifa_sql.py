"""Tests for the SEIFA tidying views."""

from pathlib import Path

import duckdb
import pytest

SQL = (
    Path(__file__).parents[3]
    / 'analysis' / 'reference' / 'sql' / 'seifa.sql'
)

SCORED = '11111111111'
EXCLUDED = '22222222222'
PART_EXCLUDED = '33333333333'


def _index_rows(code: str, score: str, national: str, state: str) -> str:
    """Build one raw SEIFA row in the workbook's column layout."""
    return (
        f"('{code}', '100', '{score}', NULL, '500', '{national}', "
        f"'50', NULL, 'NSW', '200', '{state}', '48')"
    )


@pytest.fixture(name='con', scope='module')
def _con() -> duckdb.DuckDBPyConnection:
    """Build raw SEIFA sheets covering scored and excluded areas."""
    con = duckdb.connect()
    columns = (
        'as t(A, B, C, D, E, F, G, H, I, J, K, L)'
    )
    rows = ', '.join([
        _index_rows(SCORED, '1016.9', '5', '6'),
        _index_rows(PART_EXCLUDED, '900.5', '1', '2'),
        "('(c) Commonwealth of Australia', NULL, NULL, NULL, NULL, "
        'NULL, NULL, NULL, NULL, NULL, NULL, NULL)',
    ])
    for view in (
        'seifa_irsd_raw',
        'seifa_irsad_raw',
        'seifa_ier_raw',
        'seifa_ieo_raw',
    ):
        con.execute(
            f'create table {view} as select * from (values {rows}) {columns}',
        )
    con.execute(
        'create table seifa_excluded_raw as select * from (values '
        f"('{EXCLUDED}', '0', 'Y', 'Y', 'Y', 'Y'), "
        f"('{PART_EXCLUDED}', '18', 'Y', 'Y', 'Y', 'N')"
        ') as t(A, B, C, D, E, F)',
    )
    con.execute(SQL.read_text(encoding='utf-8'))
    return con


def test_a_scored_area_keeps_both_deciles(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select irsd_national_decile, irsd_state_decile from seifa_sa1 '
        f"where sa1_code = '{SCORED}'",
    ).fetchone()
    assert row == (5, 6)


def test_all_four_indexes_are_present(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select irsd_score, irsad_score, ier_score, ieo_score '
        f"from seifa_sa1 where sa1_code = '{SCORED}'",
    ).fetchone()
    assert row == (1016.9, 1016.9, 1016.9, 1016.9)


def test_the_copyright_footer_is_dropped(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        'select count(*) from seifa_sa1 '
        "where sa1_code not similar to '[0-9]{11}'",
    ).fetchone()
    assert rows == (0,)


def test_codes_stay_text(con: duckdb.DuckDBPyConnection) -> None:
    row = con.execute(
        'select typeof(sa1_code) from seifa_sa1 limit 1',
    ).fetchone()
    assert row == ('VARCHAR',)


def test_exclusion_is_recorded_per_index(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select irsd_excluded, ieo_excluded from seifa_sa1 '
        f"where sa1_code = '{PART_EXCLUDED}'",
    ).fetchone()
    assert row == (True, False)


def test_a_scored_area_is_not_marked_excluded(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select irsd_excluded from seifa_sa1 '
        f"where sa1_code = '{SCORED}'",
    ).fetchone()
    assert row == (False,)


def test_a_wholly_excluded_area_has_no_row(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.execute(
        f"select count(*) from seifa_sa1 where sa1_code = '{EXCLUDED}'",
    ).fetchone()
    assert rows == (0,)


def test_excluded_areas_keep_their_population(
    con: duckdb.DuckDBPyConnection,
) -> None:
    row = con.execute(
        'select usual_resident_population from seifa_excluded '
        f"where sa1_code = '{PART_EXCLUDED}'",
    ).fetchone()
    assert row == (18,)
