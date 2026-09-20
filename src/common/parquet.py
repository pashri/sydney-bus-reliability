"""Streaming Parquet writes to S3.

Batches are written one at a time. The whole dataset is never held in
memory at once.
"""

import io
from collections.abc import Iterable
from typing import Final, Literal

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from aws_lambda_powertools import Logger

logger = Logger()

COMPRESSION: Final[Literal['snappy']] = 'snappy'
"""Not zstd, however much smaller it would be.

These objects are written by the pyarrow in the AWS SDK-for-pandas
layer, which is built without the zstd codec, and asking for one it
does not have fails at the first write rather than at import. A local
pyarrow does have it, so no test catches this.

The merger writes its Parquet through DuckDB instead, and does use
zstd.
"""
CONTENT_TYPE: Final[str] = 'application/vnd.apache.parquet'


class ParquetRepository:
    """Writes Arrow record batches to S3 as Parquet.

    Parameters
    ----------
    bucket : str
        S3 bucket name.
    session : boto3.Session | None
        Optional boto3 session. Defaults to a new session.
    """

    def __init__(
        self,
        *,
        bucket: str,
        session: boto3.Session | None = None,
    ) -> None:
        session = session or boto3.Session()
        self.bucket = bucket
        self.client = session.client('s3')

    def put_batches(
        self,
        *,
        key: str,
        schema: pa.Schema,
        batches: Iterable[pa.RecordBatch],
    ) -> int:
        """Stream record batches into one Parquet object.

        Parameters
        ----------
        key : str
            Destination S3 key.
        schema : pa.Schema
            Schema every batch conforms to.
        batches : Iterable[pa.RecordBatch]
            Batches to write, consumed lazily.

        Returns
        -------
        int
            Number of rows written.
        """
        buffer = io.BytesIO()
        rows = self.write_buffer(
            buffer=buffer, schema=schema, batches=batches,
        )
        buffer.seek(0)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer,
            ContentType=CONTENT_TYPE,
        )
        return rows

    @staticmethod
    def write_buffer(
        *,
        buffer: io.BytesIO,
        schema: pa.Schema,
        batches: Iterable[pa.RecordBatch],
    ) -> int:
        """Write batches into an in-memory Parquet buffer.

        Parameters
        ----------
        buffer : io.BytesIO
            Destination buffer.
        schema : pa.Schema
            Schema every batch conforms to.
        batches : Iterable[pa.RecordBatch]
            Batches to write, consumed lazily.

        Returns
        -------
        int
            Number of rows written.
        """
        rows = 0
        with pq.ParquetWriter(
            buffer, schema, compression=COMPRESSION,
        ) as writer:
            for batch in batches:
                writer.write_batch(batch)
                rows += batch.num_rows
        return rows
