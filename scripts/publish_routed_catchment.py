"""Fold routed distances into the catchment bridge and publish it.

Run after ``scripts.route_stop_meshblock`` has filled the routing
store::

    uv run python -m scripts.publish_routed_catchment \\
        --pairs build/reference/stop_meshblock.parquet \\
        --store build/routing.duckdb --vintage 2026-09-22 \\
        --bucket <bucket> --profile pashri-admin --publish

Writes the merged table locally and uploads only when asked, so the
result can be looked at before it replaces what analyses read.
"""

import argparse
import logging
from pathlib import Path
from typing import Final

import duckdb

from analysis.reference.publishing import add_publish_arguments, upload
from analysis.reference.vintage import ReferenceTable, vintage_prefix

MERGE_SQL: Final[Path] = (
    Path(__file__).parents[1]
    / 'analysis' / 'reference' / 'sql' / 'merge_routing.sql'
)
CHECKS: Final[tuple[tuple[str, str], ...]] = (
    ('rows', 'select count(*) from stop_meshblock_routed'),
    ('routed', "select count(*) from stop_meshblock_routed "
               "where routing_status = 'routed'"),
    ('snap failed', "select count(*) from stop_meshblock_routed "
                    "where routing_status = 'snap_failed'"),
    ('unroutable', "select count(*) from stop_meshblock_routed "
                   "where routing_status = 'unroutable'"),
    ('median detour', 'select round(median(detour_ratio), 3) '
                      'from stop_meshblock_routed '
                      "where routing_status = 'routed'"),
    ('impossible detour', 'select count(*) from stop_meshblock_routed '
                          'where detour_ratio < 0.999'),
)

logger = logging.getLogger(__name__)


def merge(*, pairs: Path, store: Path) -> duckdb.DuckDBPyConnection:
    """Join the routing results onto the built bridge.

    Parameters
    ----------
    pairs : Path
        The built catchment bridge.
    store : Path
        The routing store.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A connection holding the merged view.
    """
    con = duckdb.connect()
    con.execute(f"attach '{store}' as r (read_only)")
    con.execute(f"create view pair as select * from read_parquet('{pairs}')")
    con.execute('create view routed_leg as select * from r.routed_leg')
    con.execute(MERGE_SQL.read_text(encoding='utf-8'))
    return con


def report(*, con: duckdb.DuckDBPyConnection) -> None:
    """Log the checks that would catch a bad merge.

    A detour ratio below one is geometrically impossible and expected
    in small numbers: both ends of a pair snap independently onto the
    walking network, so at very short range the measured walk can come
    out under the straight line. A large share would mean something
    else is wrong.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection holding the merged view.
    """
    for label, query in CHECKS:
        logger.info('%s: %s', label, con.execute(query).fetchone())


def write(*, con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Write the merged bridge to Parquet.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection holding the merged view.
    path : Path
        Where to write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        'copy (select * from stop_meshblock_routed) '
        f"to '{path}' (format parquet, compression zstd)",
    )
    logger.info('wrote %s', path)


def main(*, argv: list[str] | None = None) -> int:
    """Merge and optionally publish.

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
    con = merge(pairs=args.pairs, store=args.store)
    report(con=con)
    write(con=con, path=args.output)
    if args.publish:
        prefix = vintage_prefix(
            table=ReferenceTable.STOP_MESHBLOCK, vintage=args.vintage,
        )
        upload(
            path=args.output,
            bucket=args.bucket,
            key=f'{prefix}/data.parquet',
            profile=args.profile,
        )
    return 0


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
    parser.add_argument('--pairs', type=Path, required=True)
    parser.add_argument('--store', type=Path, required=True)
    parser.add_argument(
        '--output', type=Path,
        default=Path('build/reference/stop_meshblock_routed.parquet'),
    )
    add_publish_arguments(parser=parser)
    return parser.parse_args(argv)


if __name__ == '__main__':
    raise SystemExit(main())
