"""Tests for streaming Parquet writes."""

import io
from collections.abc import Iterator
from typing import Final

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from common.parquet import COMPRESSION, ParquetRepository

SCHEMA: pa.Schema = pa.schema([
    pa.field('vehicle_id', pa.string()),
    pa.field('lat', pa.float64()),
])
BATCH_COUNT: Final[int] = 5


def batches(*, count: int) -> Iterator[pa.RecordBatch]:
    """Yield small record batches.

    Parameters
    ----------
    count : int
        How many single-row batches to yield.

    Yields
    ------
    pa.RecordBatch
        One row per batch.
    """
    for index in range(count):
        yield pa.RecordBatch.from_pylist(
            [{'vehicle_id': f'v{index}', 'lat': -33.8}], schema=SCHEMA,
        )


def test_put_batches_writes_all_rows(_bucket: str) -> None:
    """Every batch lands in one Parquet object."""
    repository = ParquetRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    written = repository.put_batches(
        key='curated/x/test.parquet',
        schema=SCHEMA,
        batches=batches(count=3),
    )
    assert written == 3


def test_put_batches_round_trips(_bucket: str) -> None:
    """The object reads back as valid Parquet with the same values."""
    repository = ParquetRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    repository.put_batches(
        key='curated/x/test.parquet',
        schema=SCHEMA,
        batches=batches(count=2),
    )
    body = boto3.client('s3').get_object(
        Bucket=_bucket, Key='curated/x/test.parquet',
    )['Body'].read()
    table = pq.read_table(io.BytesIO(body))
    assert table.column('vehicle_id').to_pylist() == ['v0', 'v1']


def test_put_batches_empty_writes_header_only(_bucket: str) -> None:
    """Zero batches still produces a readable, empty Parquet file.

    An hour with no data must not leave a missing object, because a
    missing object is indistinguishable from a failed run.
    """
    repository = ParquetRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    written = repository.put_batches(
        key='curated/x/empty.parquet',
        schema=SCHEMA,
        batches=iter(()),
    )
    assert written == 0
    body = boto3.client('s3').get_object(
        Bucket=_bucket, Key='curated/x/empty.parquet',
    )['Body'].read()
    table = pq.read_table(io.BytesIO(body))
    assert table.num_rows == 0
    assert table.schema.names == ['vehicle_id', 'lat']


def test_put_batches_consumes_lazily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Batches are written one at a time, never materialised first.

    This is the module's reason to exist. Holding a full hour of
    vehicle positions in memory measured 4,007 MB peak against
    1,527 MB when streaming - the difference between a 1 GB and a
    2 GB Lambda tier, paid every hour of every day. A refactor that
    inserted ``list(batches)`` would pass every other test here.
    """
    produced = 0

    def counted() -> Iterator[pa.RecordBatch]:
        nonlocal produced
        for index in range(BATCH_COUNT):
            produced += 1
            yield pa.RecordBatch.from_pylist(
                [{'vehicle_id': f'v{index}', 'lat': -33.8}],
                schema=SCHEMA,
            )

    seen: list[int] = []
    original = pq.ParquetWriter.write_batch

    def spy(self: pq.ParquetWriter, batch: pa.RecordBatch) -> None:
        seen.append(produced)
        original(self, batch)

    monkeypatch.setattr(pq.ParquetWriter, 'write_batch', spy)
    ParquetRepository.write_buffer(
        buffer=io.BytesIO(), schema=SCHEMA, batches=counted(),
    )
    assert seen == list(range(1, BATCH_COUNT + 1))


def test_put_batches_schema_mismatch_raises(_bucket: str) -> None:
    """A batch that disagrees with the declared schema must raise.

    Silently writing a malformed object would produce a month of
    unusable days before anyone noticed.
    """
    wrong = pa.RecordBatch.from_pylist(
        [{'unexpected': 1}], schema=pa.schema([
            pa.field('unexpected', pa.int64()),
        ]),
    )
    repository = ParquetRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    with pytest.raises(ValueError, match='schema does not match'):
        repository.put_batches(
            key='curated/x/bad.parquet',
            schema=SCHEMA,
            batches=iter((wrong,)),
        )


def test_put_batches_writes_nothing_when_a_batch_fails(
    _bucket: str,
) -> None:
    """A failed write leaves no object behind.

    The buffer is assembled fully in memory before it is shipped, so
    a partial or footerless Parquet file can never land in S3 - where
    it would look like valid data and fail only at read time.
    """
    wrong = pa.RecordBatch.from_pylist(
        [{'unexpected': 1}], schema=pa.schema([
            pa.field('unexpected', pa.int64()),
        ]),
    )
    repository = ParquetRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    with pytest.raises(ValueError, match='schema does not match'):
        repository.put_batches(
            key='curated/x/bad.parquet',
            schema=SCHEMA,
            batches=iter((wrong,)),
        )
    listing = boto3.client('s3').list_objects_v2(
        Bucket=_bucket, Prefix='curated/x/bad.parquet',
    )
    assert listing['KeyCount'] == 0


def test_compression_is_one_the_lambda_layer_can_write() -> None:
    """These objects must use a codec the deployed pyarrow has.

    A tripwire, not a real check. The pyarrow in the AWS
    SDK-for-pandas layer is built without zstd, and a local pyarrow
    has it, so nothing in this suite can tell the difference. The
    failure is at the first write in production, not at import.

    Changing this constant means proving the layer's build supports
    the new codec first.
    """
    assert COMPRESSION == 'snappy'
