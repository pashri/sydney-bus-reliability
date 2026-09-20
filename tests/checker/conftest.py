"""Checker-specific fixtures.

The moto-server fixtures are shared: DuckDB cannot see ``mock_aws()``,
so any test reading S3 through it needs a real local server.
"""

# pylint: disable=unused-import
from tests.moto_backend import (  # noqa: F401
    _bucket,
    _moto_backend,
    _s3_endpoint,
)
