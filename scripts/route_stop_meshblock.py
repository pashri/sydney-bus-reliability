"""Fill walking distances on the stop catchment bridge.

Start a local OSRM server first::

    docker run -d --name osrm -p 5001:5000 -v "$PWD/source:/data" \\
        ghcr.io/project-osrm/osrm-backend osrm-routed \\
        --algorithm mld --max-table-size 2000 /data/corridor.osrm

then::

    uv run python -m scripts.route_stop_meshblock \\
        --pairs build/reference/stop_meshblock.parquet \\
        --mesh-block-shp source/mb/MB_2021_AUST_GDA2020.shp \\
        --dim-stop <dim_stop parquet>

Only pairs with residents are routed, since a mesh block with nobody
in it contributes to no population figure. Results are written to a
local store as they arrive, so an interrupted run resumes rather than
starting again.
"""

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Final

import duckdb
import requests

from analysis.reference.osrm import Leg, Place, legs_from, table_url

DEFAULT_BASE_URL: Final[str] = 'http://localhost:5001'
DEFAULT_RADIUS_M: Final[int] = 800
DEFAULT_WORKERS: Final[int] = 8
REQUEST_TIMEOUT_S: Final[float] = 120
COMMIT_EVERY: Final[int] = 200

logger = logging.getLogger(__name__)


def open_store(*, path: Path) -> duckdb.DuckDBPyConnection:
    """Open the local store that lets a run resume.

    Parameters
    ----------
    path : Path
        Where the store lives.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A connection with the results table present.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    store = duckdb.connect(str(path))
    store.execute(
        'create table if not exists routed_leg ('
        'stop_id varchar, mesh_block_code varchar, '
        'network_distance_m double, network_duration_s double, '
        'snap_distance_m double, routing_status varchar, '
        'primary key (stop_id, mesh_block_code))',
    )
    return store


def load_work(
    *,
    pairs: Path,
    mesh_block_shp: Path,
    dim_stop: str,
    radius_m: int,
    store: duckdb.DuckDBPyConnection,
) -> dict[str, tuple[Place, list[Place]]]:
    """Gather the pairs still needing a walking distance.

    Parameters
    ----------
    pairs : Path
        The built catchment bridge.
    mesh_block_shp : Path
        Mesh block shapefile, for the interior anchor points.
    dim_stop : str
        Parquet glob for the curated stops.
    radius_m : int
        Only pairs at or inside this straight-line distance.
    store : duckdb.DuckDBPyConnection
        Store holding anything already routed.

    Returns
    -------
    dict[str, tuple[Place, list[Place]]]
        Origin and destinations, keyed by stop.
    """
    con = duckdb.connect()
    con.execute('INSTALL spatial; LOAD spatial;')
    con.execute(
        'create view anchor as select MB_CODE21 as mesh_block_code, '
        'st_x(st_pointonsurface(geom)) as lon, '
        'st_y(st_pointonsurface(geom)) as lat '
        f"from st_read('{mesh_block_shp}') where STE_CODE21 = '1'",
    )
    con.execute(f"create view stop as select * from read_parquet('{dim_stop}')")
    con.execute(f"create view pair as select * from read_parquet('{pairs}')")
    con.execute('create table done as select stop_id, mesh_block_code from '
                f"read_parquet('{store_export(store=store)}')")
    rows = con.execute(
        'select p.stop_id, s.stop_lat, s.stop_lon, '
        'p.mesh_block_code, a.lat, a.lon '
        'from pair as p '
        'join stop as s on s.stop_id = p.stop_id '
        'join anchor as a on a.mesh_block_code = p.mesh_block_code '
        'anti join done as d on d.stop_id = p.stop_id '
        'and d.mesh_block_code = p.mesh_block_code '
        f'where p.person_count > 0 and p.straight_line_distance_m <= {radius_m}'
        ' order by p.stop_id',
    ).fetchall()
    return _group(rows=rows)


def store_export(*, store: duckdb.DuckDBPyConnection) -> str:
    """Write what is already routed where another connection can read it.

    Parameters
    ----------
    store : duckdb.DuckDBPyConnection
        Store holding earlier results.

    Returns
    -------
    str
        Path to a Parquet file of routed pairs.
    """
    path = '/tmp/routed_done.parquet'
    store.execute(
        'copy (select stop_id, mesh_block_code from routed_leg) '
        f"to '{path}' (format parquet)",
    )
    return path


def _group(
    *,
    rows: list[tuple[str, float, float, str, float, float]],
) -> dict[str, tuple[Place, list[Place]]]:
    """Collect destination lists by origin.

    Parameters
    ----------
    rows : list[tuple[str, float, float, str, float, float]]
        Stop, its coordinates, mesh block, and its anchor.

    Returns
    -------
    dict[str, tuple[Place, list[Place]]]
        Origin and destinations, keyed by stop.
    """
    work: dict[str, tuple[Place, list[Place]]] = {}
    for stop_id, stop_lat, stop_lon, block, block_lat, block_lon in rows:
        origin = Place(
            identifier=stop_id, latitude=stop_lat, longitude=stop_lon,
        )
        entry = work.setdefault(stop_id, (origin, []))
        entry[1].append(
            Place(identifier=block, latitude=block_lat, longitude=block_lon),
        )
    return work


def route_one(
    *,
    origin: Place,
    destinations: list[Place],
    base_url: str,
    client: requests.Session,
) -> list[Leg]:
    """Ask OSRM for one stop's distances.

    Parameters
    ----------
    origin : Place
        The stop.
    destinations : list[Place]
        Mesh block anchors within the radius.
    base_url : str
        Root of the OSRM server.
    client : requests.Session
        Session used to make the request.

    Returns
    -------
    list[Leg]
        One leg per destination.
    """
    url = table_url(
        base_url=base_url, origin=origin, destinations=destinations,
    )
    response = client.get(url, timeout=REQUEST_TIMEOUT_S)
    response.raise_for_status()
    return legs_from(
        payload=response.json(), origin=origin, destinations=destinations,
    )


def save(*, store: duckdb.DuckDBPyConnection, legs: list[Leg]) -> None:
    """Record routed pairs.

    Parameters
    ----------
    store : duckdb.DuckDBPyConnection
        Store to write to.
    legs : list[Leg]
        Pairs to record.
    """
    store.executemany(
        'insert or replace into routed_leg values (?, ?, ?, ?, ?, ?)',
        [
            (
                leg.origin_id, leg.destination_id, leg.distance_m,
                leg.duration_s, leg.snap_distance_m, str(leg.status),
            )
            for leg in legs
        ],
    )


def run(
    *,
    work: dict[str, tuple[Place, list[Place]]],
    store: duckdb.DuckDBPyConnection,
    base_url: str,
    workers: int,
) -> None:
    """Route every outstanding stop.

    Parameters
    ----------
    work : dict[str, tuple[Place, list[Place]]]
        Origin and destinations, keyed by stop.
    store : duckdb.DuckDBPyConnection
        Store to write results to.
    base_url : str
        Root of the OSRM server.
    workers : int
        How many requests to have in flight.
    """
    client = requests.Session()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                route_one,
                origin=origin,
                destinations=destinations,
                base_url=base_url,
                client=client,
            )
            for origin, destinations in work.values()
        ]
        for future in futures:
            save(store=store, legs=future.result())
            done += 1
            if done % (COMMIT_EVERY * 10) == 0:
                logger.info('routed %s of %s stops', done, len(work))


def main(*, argv: list[str] | None = None) -> int:
    """Route the outstanding catchment pairs.

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
    store = open_store(path=args.store)
    work = load_work(
        pairs=args.pairs,
        mesh_block_shp=args.mesh_block_shp,
        dim_stop=args.dim_stop,
        radius_m=args.radius_m,
        store=store,
    )
    logger.info('stops to route: %s', len(work))
    run(
        work=work,
        store=store,
        base_url=args.base_url,
        workers=args.workers,
    )
    logger.info(
        'routed legs held: %s',
        store.execute('select count(*) from routed_leg').fetchone(),
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
    parser.add_argument('--mesh-block-shp', type=Path, required=True)
    parser.add_argument('--dim-stop', required=True)
    parser.add_argument('--store', type=Path,
                        default=Path('build/routing.duckdb'))
    parser.add_argument('--base-url', default=DEFAULT_BASE_URL)
    parser.add_argument('--radius-m', type=int, default=DEFAULT_RADIUS_M)
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS)
    return parser.parse_args(argv)


if __name__ == '__main__':
    raise SystemExit(main())
