"""Open the analysis views over a local copy of the curated layer.

Pull the copy first with ``scripts.pull_curated``, then::

    from pathlib import Path
    from analysis.marts import connect

    con = connect(root=Path('build/data'))
    con.sql('select * from route_league limit 10')

Every view is lazy: each query reads the Parquet files again. Cache a
slow view with ``create table ... as`` on a DuckDB file of your own.
"""

from pathlib import Path
from typing import Final

import duckdb

ANALYSIS: Final[Path] = Path(__file__).parent
MARTS: Final[Path] = ANALYSIS / 'marts.sql'
SNAPSHOTS: Final[tuple[str, ...]] = (
    'dim_route',
    'dim_trip',
    'dim_stop',
    'dim_scheduled_stop_time',
    'dim_calendar',
    'dim_calendar_dates',
)
"""Dimensions read as every snapshot at once, with ``valid_from``."""
FACTS: Final[tuple[str, ...]] = ('fact_trip', 'fact_trip_stop')
REFERENCE: Final[tuple[str, ...]] = ('stop_geography', 'stop_meshblock')


def input_views(*, root: Path) -> list[str]:
    """Write the statements that expose a local copy as the inputs.

    Snapshots keep their ``valid_from`` from the path, as text. Facts
    and reference tables are read without hive partitioning: their
    ``service_date`` and ``vintage`` are columns in the files, and the
    path's copy would only shadow them.

    Parameters
    ----------
    root : Path
        Root of the local copy, holding ``curated/`` and ``reference/``.

    Returns
    -------
    list[str]
        ``create view`` statements.
    """
    seed = sorted(ANALYSIS.glob('calendar_exclusions_*.csv'))
    seeds = ', '.join(f"'{path}'" for path in seed)
    return [
        'create or replace view calendar_exclusion_seed as '
        f'select * from read_csv([{seeds}])',
        *(
            f'create or replace view {name}_all as select * from '
            f"read_parquet('{root}/curated/{name}/*/*.parquet', "
            'hive_partitioning = true, union_by_name = true)'
            for name in SNAPSHOTS
        ),
        *(
            f'create or replace view {name} as select * from '
            f"read_parquet('{root}/curated/{name}/*/*.parquet', "
            'hive_partitioning = false, union_by_name = true)'
            for name in FACTS
        ),
        *(
            f'create or replace view {name}_all as select * from '
            f"read_parquet('{root}/reference/{name}/*/*.parquet', "
            'hive_partitioning = false, union_by_name = true)'
            for name in REFERENCE
        ),
    ]


def connect(
    *,
    root: Path,
    database: str = ':memory:',
) -> duckdb.DuckDBPyConnection:
    """Open a connection with every analysis view created.

    Parameters
    ----------
    root : Path
        Root of the local copy, holding ``curated/`` and ``reference/``.
    database : str
        DuckDB database to open; in memory by default.

    Returns
    -------
    duckdb.DuckDBPyConnection
        The connection.
    """
    con = duckdb.connect(database)
    con.execute('INSTALL spatial; LOAD spatial; LOAD icu;')
    for statement in input_views(root=root.resolve()):
        con.execute(statement)
    con.execute(MARTS.read_text(encoding='utf-8'))
    return con
