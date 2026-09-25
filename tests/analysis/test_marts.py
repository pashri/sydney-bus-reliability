"""Tests for opening the analysis views over a local copy."""

from pathlib import Path

import duckdb
import pytest

from analysis.marts import connect, main
from tests.analysis.world import build_inputs

SNAPSHOTS: tuple[str, ...] = (
    'dim_route', 'dim_trip', 'dim_stop', 'dim_scheduled_stop_time',
    'dim_calendar', 'dim_calendar_dates',
)


def export_world(*, root: Path) -> None:
    """Write the test world in the bucket's layout.

    Parameters
    ----------
    root : Path
        Destination, standing in for a pull.
    """
    (root / 'curated').mkdir()
    (root / 'reference').mkdir()
    con = duckdb.connect()
    build_inputs(con=con)
    for name in SNAPSHOTS:
        con.execute(
            f"copy {name}_all to '{root}/curated/{name}' "
            '(format parquet, partition_by (valid_from))',
        )
    for name in ('fact_trip', 'fact_trip_stop'):
        con.execute(
            f"copy {name} to '{root}/curated/{name}' (format parquet, "
            'partition_by (service_date), write_partition_columns true)',
        )
    for name in ('stop_geography', 'stop_meshblock'):
        con.execute(
            f"copy {name}_all to '{root}/reference/{name}' (format parquet, "
            'partition_by (vintage), write_partition_columns true)',
        )


@pytest.fixture(name='con', scope='module')
def _con(tmp_path_factory: pytest.TempPathFactory) -> (
    duckdb.DuckDBPyConnection
):
    """Open the views over the test world written to disk."""
    root = tmp_path_factory.mktemp('pull')
    export_world(root=root)
    return connect(root=root)


def test_connect_keeps_the_snapshot_label(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """valid_from comes from the path, as text."""
    rows = con.execute(
        'select distinct valid_from from dim_trip_all order by 1',
    ).fetchall()
    assert rows == [('2026-09-19T173455Z',), ('2026-09-22T020011Z',)]


def test_connect_builds_the_marts(con: duckdb.DuckDBPyConnection) -> None:
    """The trip mart over files matches the one over tables."""
    row = con.execute(
        "select count(*) from mart_trip where service_date = '2026-09-22'",
    ).fetchone()
    assert row == (8,)


def test_main_writes_a_database_other_clients_can_open(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """The file keeps the views, so a later connection sees the marts."""
    root = tmp_path_factory.mktemp('pull')
    export_world(root=root)
    database = root / 'marts.duckdb'
    database.write_bytes(b'stale')
    assert main(argv=[str(root), str(database)]) == 0
    con = duckdb.connect(str(database), read_only=True)
    con.execute('LOAD icu')
    row = con.execute(
        "select count(*) from mart_trip where service_date = '2026-09-22'",
    ).fetchone()
    assert row == (8,)


def test_connect_skips_tables_a_pull_has_not_fetched(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """dim_agency and fact_collector_run appear only when present."""
    views = {
        row[0] for row in con.execute(
            'select view_name from duckdb_views() where not internal',
        ).fetchall()
    }
    assert 'dim_agency_all' not in views
    assert 'mart_stop_hour' in views
