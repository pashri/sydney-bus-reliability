"""Build the stop geography and catchment reference tables.

Run from the repo root::

    uv run python -m scripts.build_stop_geography \\
        --mesh-block-shp source/mb/MB_2021_AUST_GDA2020.shp \\
        --lga-shp source/lga/LGA_2021_AUST_GDA2020.shp \\
        --seifa-xlsx source/seifa_2021_sa1_indexes.xlsx \\
        --mesh-block-counts-xlsx source/mesh_block_counts_2021.xlsx

Reads the curated stops and the published ABS files, and writes two
Parquet files locally. Nothing is uploaded unless ``--publish`` is
given, so the output can be looked at before it becomes the vintage
every analysis will pick up.

Every input is a path on the command line. The ABS reorganises its
site between releases, and a moved file should not mean a code change.
"""

import argparse
import logging
from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from analysis.reference.publishing import add_publish_arguments, upload
from analysis.reference.vintage import ReferenceTable, vintage_prefix

SQL_DIR: Final[Path] = Path(__file__).parents[1] / 'analysis' / 'reference'
CENTRES_CSV: Final[Path] = SQL_DIR / 'centres.csv'
SEIFA_SHEETS: Final[dict[str, str]] = {
    'seifa_irsd_raw': 'Table 2',
    'seifa_irsad_raw': 'Table 3',
    'seifa_ier_raw': 'Table 4',
    'seifa_ieo_raw': 'Table 5',
}
SEIFA_RANGE: Final[str] = 'A7:L70000'
EXCLUDED_RANGE: Final[str] = 'A7:F40000'
COUNT_SHEETS: Final[tuple[str, ...]] = ('Table 1', 'Table 1.1')
COUNT_RANGE: Final[str] = 'A8:F800000'
ASGS_EDITION: Final[str] = 'ASGS2021'
SEIFA_RELEASE: Final[str] = 'SEIFA2021'
CENSUS_YEAR: Final[int] = 2021

logger = logging.getLogger(__name__)


def connect() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with the extensions this build needs.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A connection with spatial and excel loaded.
    """
    connection = duckdb.connect()
    connection.execute('INSTALL spatial; LOAD spatial;')
    connection.execute('INSTALL excel; LOAD excel;')
    return connection


def run_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    name: str,
) -> None:
    """Run one of the build's SQL files.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to run against.
    name : str
        File name inside the sql directory.
    """
    path = SQL_DIR / 'sql' / name
    connection.execute(path.read_text(encoding='utf-8'))


def load_boundaries(
    *,
    connection: duckdb.DuckDBPyConnection,
    mesh_block_shp: Path,
    lga_shp: Path,
) -> None:
    """Read the ABS boundary files and clip them to the extent.

    The clipped boundaries are materialised rather than left as views.
    They are read many times over by the joins that follow, and
    re-reading a shapefile each time dominates the run.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to load into.
    mesh_block_shp : Path
        Mesh block shapefile.
    lga_shp : Path
        Local government area shapefile.
    """
    connection.execute(
        'create view mesh_block_raw as '
        f"select * from st_read('{mesh_block_shp}')",
    )
    connection.execute(
        f"create view lga_raw as select * from st_read('{lga_shp}')",
    )
    run_sql(connection=connection, name='abs_sources.sql')
    connection.execute('create table mesh_block_t as select * from mesh_block')
    connection.execute('create table lga_t as select * from lga')
    connection.execute('drop view mesh_block; drop view lga')
    connection.execute('create view mesh_block as select * from mesh_block_t')
    connection.execute('create view lga as select * from lga_t')


def load_seifa(
    *,
    connection: duckdb.DuckDBPyConnection,
    workbook: Path,
) -> None:
    """Read every SEIFA sheet the build uses.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to load into.
    workbook : Path
        The SA1 indexes workbook.
    """
    for view, sheet in SEIFA_SHEETS.items():
        connection.execute(
            f'create view {view} as select * from read_xlsx('
            f"'{workbook}', sheet='{sheet}', range='{SEIFA_RANGE}', "
            'header=false, all_varchar=true)',
        )
    connection.execute(
        'create view seifa_excluded_raw as select * from read_xlsx('
        f"'{workbook}', sheet='Table 6', range='{EXCLUDED_RANGE}', "
        'header=false, all_varchar=true)',
    )
    run_sql(connection=connection, name='seifa.sql')


def load_counts(
    *,
    connection: duckdb.DuckDBPyConnection,
    workbook: Path,
) -> None:
    """Read the mesh block counts, both halves of New South Wales.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to load into.
    workbook : Path
        The mesh block counts workbook.
    """
    parts = ' union all '.join(
        'select A, B, C, D, E from read_xlsx('
        f"'{workbook}', sheet='{sheet}', range='{COUNT_RANGE}', "
        'header=false, all_varchar=true)'
        for sheet in COUNT_SHEETS
    )
    connection.execute(f'create view mesh_block_count_raw as {parts}')
    run_sql(connection=connection, name='mesh_block_counts.sql')


def load_build_row(
    *,
    connection: duckdb.DuckDBPyConnection,
    vintage: date,
) -> None:
    """Record the vintage and source editions for every output row.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to load into.
    vintage : date
        Date this build ran.
    """
    connection.execute(
        'create table build as select '
        f"date '{vintage.isoformat()}' as vintage, "
        f"'{ASGS_EDITION}' as asgs_edition, "
        f"'{SEIFA_RELEASE}' as seifa_release, "
        f'{CENSUS_YEAR} as census_year',
    )
    connection.execute(
        f"create table centre as select * from read_csv('{CENTRES_CSV}')",
    )


def report(*, connection: duckdb.DuckDBPyConnection) -> None:
    """Log the checks that catch a build gone wrong.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection holding the built tables.
    """
    for label, query in (
        ('stops', 'select count(*) from stop_geography'),
        ('unmatched', "select count(*) from stop_geography "
                      "where geography_match = 'nearest'"),
        ('no SEIFA', 'select count(*) from stop_geography '
                     'where irsd_score is null'),
        ('pairs', 'select count(*) from stop_meshblock_out'),
        ('people in NSW',
         'select sum(person_count) from mesh_block_count'),
    ):
        logger.info('%s: %s', label, connection.execute(query).fetchone())


def main(*, argv: list[str] | None = None) -> int:
    """Build and optionally publish the reference tables.

    Parameters
    ----------
    argv : list[str] | None, optional
        Arguments to parse. Defaults to the real command line.

    Returns
    -------
    int
        Process exit status.
    """
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    args = parse_args(argv=argv)
    connection = connect()
    connection.execute(
        'create view dim_stop as select * from '
        f"read_parquet('{args.dim_stop}')",
    )
    load_build_row(connection=connection, vintage=args.vintage)
    load_boundaries(
        connection=connection,
        mesh_block_shp=args.mesh_block_shp,
        lga_shp=args.lga_shp,
    )
    load_seifa(connection=connection, workbook=args.seifa_xlsx)
    load_counts(connection=connection, workbook=args.mesh_block_counts_xlsx)
    run_sql(connection=connection, name='stop_geography.sql')
    run_sql(connection=connection, name='stop_geography_table.sql')
    run_sql(connection=connection, name='stop_meshblock.sql')
    connection.execute(
        'create table stop_meshblock_out as select * from stop_meshblock',
    )
    report(connection=connection)
    write_outputs(connection=connection, args=args)
    return 0


def write_outputs(
    *,
    connection: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
) -> None:
    """Write both tables to Parquet, and upload when asked.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection holding the built tables.
    args : argparse.Namespace
        Parsed command line.
    """
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for table, source in (
        (ReferenceTable.STOP_GEOGRAPHY, 'stop_geography'),
        (ReferenceTable.STOP_MESHBLOCK, 'stop_meshblock_out'),
    ):
        path = args.output_dir / f'{table}.parquet'
        connection.execute(
            f"copy (select * from {source}) to '{path}' "
            "(format parquet, compression zstd)",
        )
        logger.info('wrote %s', path)
        if args.publish:
            upload(
                path=path,
                bucket=args.bucket,
                key=f'{vintage_prefix(table=table, vintage=args.vintage)}'
                    '/data.parquet',
                profile=args.profile,
            )


def parse_args(*, argv: list[str] | None = None) -> argparse.Namespace:
    """Read the command line.

    Parameters
    ----------
    argv : list[str] | None, optional
        Arguments to parse. Defaults to the real command line.

    Returns
    -------
    argparse.Namespace
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dim-stop', required=True)
    parser.add_argument('--mesh-block-shp', type=Path, required=True)
    parser.add_argument('--lga-shp', type=Path, required=True)
    parser.add_argument('--seifa-xlsx', type=Path, required=True)
    parser.add_argument('--mesh-block-counts-xlsx', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path,
                        default=Path('build/reference'))
    add_publish_arguments(parser=parser)
    return parser.parse_args(argv)


if __name__ == '__main__':
    raise SystemExit(main())
