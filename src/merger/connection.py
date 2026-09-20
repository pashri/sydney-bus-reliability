"""Preparing a DuckDB connection for the service-day merge.

DuckDB sizes its threads and memory from the machine it detects, which
in a container can be the host rather than the slice the function was
given. Left alone it can run more workers than there is CPU for, and
believe it has memory it does not have, so it never spills before the
runtime kills the process.
"""

from pathlib import Path
from typing import Final

import duckdb
from aws_lambda_powertools import Logger

logger = Logger()

HTTPFS_EXTENSION: Final[Path] = Path(
    '/opt/python/duckdb_extensions/httpfs.duckdb_extension',
)
"""Where the ``layer`` make target puts httpfs in the layer."""

AWS_EXTENSION: Final[Path] = Path(
    '/opt/python/duckdb_extensions/aws.duckdb_extension',
)
"""Where the ``layer`` make target puts aws in the layer.

``CREATE SECRET ... PROVIDER credential_chain`` lives in this
extension, not in httpfs. Loading httpfs from the layer turns
autoloading off, so aws has to be loaded explicitly or creating the
secret fails and every merge dies before reading a row.
"""

DUCKDB_THREADS: Final[int] = 2
"""Worker threads.

The function's memory allocation buys it about one vCPU, and DuckDB
sizes its own pool from the machine it detects rather than from that.
"""

DUCKDB_MEMORY_LIMIT: Final[str] = '2200MB'
"""Headroom below the function's allocation, so DuckDB spills first."""

DUCKDB_TEMP_DIRECTORY: Final[str] = '/tmp'
"""Where spilled data goes. The only writable path on Lambda."""



def configure(
    *,
    connection: duckdb.DuckDBPyConnection,
    endpoint: str | None = None,
) -> None:
    """Prepare a DuckDB connection for S3 access.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to configure.
    endpoint : str | None
        Override S3 endpoint, host[:port] only. A test seam, never
        set in production. DuckDB's httpfs opens its own sockets, so
        ``mock_aws()`` cannot intercept it and tests must point it at
        a real local moto server instead.
    """
    load_extensions(connection=connection)
    apply_limits(connection=connection)
    create_s3_secret(connection=connection, endpoint=endpoint)


def load_extensions(*, connection: duckdb.DuckDBPyConnection) -> None:
    """Load the extensions the merge SQL needs.

    ``httpfs`` backs every ``s3://`` read and write, and ``aws``
    supplies the ``credential_chain`` secret provider. ``icu`` backs
    ``AT TIME ZONE`` with a named zone, which resolves
    ``scheduled_arrival_utc``; it is compiled into the DuckDB wheel and
    loads without a download.

    The layer ships both so that a run never depends on DuckDB's
    extension repository being reachable. Off Lambda the layer is
    absent and they are fetched from that repository instead.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to load into.
    """
    if HTTPFS_EXTENSION.exists():
        connection.execute('SET autoinstall_known_extensions = false')
        connection.execute('SET autoload_known_extensions = false')
        connection.execute(f"LOAD '{HTTPFS_EXTENSION}';")
        connection.execute(f"LOAD '{AWS_EXTENSION}';")
    else:
        connection.execute('INSTALL httpfs; LOAD httpfs;')
        connection.execute('INSTALL aws; LOAD aws;')
    connection.execute('LOAD icu;')


def apply_limits(*, connection: duckdb.DuckDBPyConnection) -> None:
    """Bound DuckDB's threads and memory to the function's allocation.

    DuckDB sizes both from the machine it detects, which in a container
    can be the host rather than the slice the function was given. Left
    alone it can run more workers than there is CPU for, and believe it
    has memory it does not have, so it never spills before the runtime
    kills the process.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to bound.
    """
    connection.execute(f'SET threads = {DUCKDB_THREADS}')
    connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    connection.execute(
        f"SET temp_directory = '{DUCKDB_TEMP_DIRECTORY}'",
    )
    logger.info(
        'DuckDB limits applied',
        extra=effective_limits(connection=connection),
    )


def effective_limits(
    *,
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, str]:
    """Read back the limits DuckDB is actually running under.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to interrogate.

    Returns
    -------
    dict[str, str]
        Each setting's name and its current value.
    """
    settings = ('threads', 'memory_limit', 'temp_directory')
    return {
        name: setting_value(connection=connection, name=name)
        for name in settings
    }


def setting_value(
    *,
    connection: duckdb.DuckDBPyConnection,
    name: str,
) -> str:
    """Read one DuckDB setting's current value.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to interrogate.
    name : str
        Setting to read.

    Returns
    -------
    str
        The setting's value, or an empty string if it has none.
    """
    result = connection.execute(
        f"SELECT current_setting('{name}')",
    ).fetchone()
    return str(result[0]) if result else ''


def create_s3_secret(
    *,
    connection: duckdb.DuckDBPyConnection,
    endpoint: str | None = None,
) -> None:
    """Give a connection credentials for S3.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to configure.
    endpoint : str | None
        Override S3 endpoint, host[:port] only.
    """
    # CHAIN 'env' pins credential resolution to environment variables,
    # which is what both the Lambda runtime and the test fixtures set,
    # rather than DuckDB's default order which checks a local
    # ~/.aws/credentials profile first and can pick up stale keys.
    options = "PROVIDER credential_chain, CHAIN 'env'"
    if endpoint:
        options += (
            f", ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false"
        )
    connection.execute(f'CREATE SECRET (TYPE s3, {options});')
