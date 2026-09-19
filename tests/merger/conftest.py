"""Merger-specific fixtures.

DuckDB's httpfs extension makes its own socket connections, bypassing
the boto3/botocore interception ``moto.mock_aws`` relies on. Merger
tests that read S3 through DuckDB instead run against a real local
moto server. Plain ``boto3.client('s3')`` calls are pointed at it via
``AWS_ENDPOINT_URL``, a real botocore feature - test-only use, since
production code never reads it. DuckDB is instead pointed at it via
the ``endpoint`` test seam on ``configure``/``merge_collector_run``/
``handler``, exposed here as the ``_s3_endpoint`` fixture.
"""

import os
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import boto3
import pytest
import requests
from moto.server import ThreadedMotoServer

from tests.conftest import REGION

BUCKET = 'test-bucket'


@dataclass(frozen=True, slots=True)
class _MotoBackend:
    """A running local moto server with one empty bucket."""

    bucket: str
    endpoint: str


@pytest.fixture
def _moto_backend(_credentials: None) -> Iterator[_MotoBackend]:
    """Start a real local moto server with one empty bucket.

    Parameters
    ----------
    _credentials : None
        Ensures dummy AWS credentials are set first.

    Yields
    ------
    _MotoBackend
        The bucket name and the server's ``host:port``.
    """
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f'{host}:{port}'
    previous = os.environ.get('AWS_ENDPOINT_URL')
    os.environ['AWS_ENDPOINT_URL'] = f'http://{endpoint}'
    client = boto3.client('s3', region_name=REGION)
    _reset_backend(client=client)
    client.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={'LocationConstraint': REGION},
    )
    try:
        yield _MotoBackend(bucket=BUCKET, endpoint=endpoint)
    finally:
        server.stop()
        _restore_endpoint(previous=previous)


@pytest.fixture
def _bucket(_moto_backend: _MotoBackend) -> str:
    """Provide the bucket name on the local moto server.

    Overrides the module-level ``_bucket`` fixture for this directory,
    because ``mock_aws()`` alone is invisible to DuckDB's httpfs.

    Parameters
    ----------
    _moto_backend : _MotoBackend
        The running local moto server.

    Returns
    -------
    str
        The bucket name.
    """
    return _moto_backend.bucket


@pytest.fixture
def _s3_endpoint(_moto_backend: _MotoBackend) -> str:
    """Provide the local moto server's endpoint for DuckDB.

    Parameters
    ----------
    _moto_backend : _MotoBackend
        The running local moto server.

    Returns
    -------
    str
        The server's ``host:port``, with no scheme.
    """
    return _moto_backend.endpoint


def _reset_backend(*, client: Any) -> None:
    """Clear state left over by an earlier moto server instance.

    Moto's backend state is process-global, so a bucket created by a
    previous test's server survives into a fresh one.

    Parameters
    ----------
    client : Any
        The S3 client whose endpoint hosts the moto server.
    """
    endpoint = client.meta.endpoint_url
    requests.post(f'{endpoint}/moto-api/reset', timeout=5)


def _restore_endpoint(*, previous: str | None) -> None:
    """Restore or clear ``AWS_ENDPOINT_URL`` after a test.

    Parameters
    ----------
    previous : str | None
        The value to restore, or ``None`` to clear it.
    """
    if previous is None:
        del os.environ['AWS_ENDPOINT_URL']
    else:
        os.environ['AWS_ENDPOINT_URL'] = previous
