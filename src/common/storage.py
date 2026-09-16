"""Persistence of raw feed payloads and per-invocation audit records."""

import gzip
import json
from datetime import datetime
from typing import Final

import boto3
from aws_lambda_powertools import Logger

from src.common.clock import round_trip_seconds, skew_seconds
from src.common.types_ import Feed, FetchResult, RunRecord

logger = Logger()

CONTENT_TYPE: Final[str] = 'application/x-google-protobuf'


def raw_key(*, feed: Feed, fetched_at: datetime) -> str:
    """Build the S3 key for one raw feed payload.

    The key is derived from the actual fetch time, never from the
    intended schedule slot, so a late retry files itself honestly
    instead of overwriting or mislabelling the scheduled poll.

    Parameters
    ----------
    feed : Feed
        The feed the payload came from.
    fetched_at : datetime
        UTC time the request was issued.

    Returns
    -------
    str
        S3 key, partitioned by date and hour.
    """
    return (
        f'raw/{feed.value}/dt={fetched_at:%Y-%m-%d}/'
        f'hour={fetched_at:%H}/{fetched_at:%H%M%S}.pb.gz'
    )


def run_key(*, fetched_at: datetime, invocation_id: str) -> str:
    """Build the S3 key for one invocation's audit record.

    Parameters
    ----------
    fetched_at : datetime
        Any UTC timestamp from the invocation, used for
        partitioning.
    invocation_id : str
        Lambda request id, unique per invocation.

    Returns
    -------
    str
        S3 key for a JSON Lines object.
    """
    return (
        f'curated/collector_run/dt={fetched_at:%Y-%m-%d}/'
        f'{invocation_id}.jsonl'
    )


def run_record(*, result: FetchResult) -> RunRecord:
    """Summarise one fetch for the audit log.

    Parameters
    ----------
    result : FetchResult
        The fetch to summarise.

    Returns
    -------
    RunRecord
        JSON-serialisable record. ``skew_s`` is None when the
        server sent no usable Date header.
    """
    skew = None
    if result.server_date_utc is not None:
        skew = skew_seconds(
            server_time=result.server_date_utc,
            sent_at=result.fetched_at_utc,
            received_at=result.received_at_utc,
        )
    return RunRecord(
        feed=result.feed.value,
        fetched_at_utc=result.fetched_at_utc.isoformat(),
        received_at_utc=result.received_at_utc.isoformat(),
        rtt_s=round_trip_seconds(
            sent_at=result.fetched_at_utc,
            received_at=result.received_at_utc,
        ),
        server_date_utc=(
            result.server_date_utc.isoformat()
            if result.server_date_utc
            else None
        ),
        skew_s=skew,
        status_code=result.status_code,
        body_bytes=len(result.body),
        error=result.error,
    )


class RawFeedRepository:
    """Writes raw payloads and audit records to one S3 bucket.

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

    def put_raw(self, *, result: FetchResult) -> str | None:
        """Store one payload, gzipped.

        Parameters
        ----------
        result : FetchResult
            The fetch to store.

        Returns
        -------
        str | None
            The key written, or None when the body was empty
            because the fetch failed.
        """
        if not result.body:
            return None
        key = raw_key(
            feed=result.feed, fetched_at=result.fetched_at_utc,
        )
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=gzip.compress(result.body),
            ContentType=CONTENT_TYPE,
            ContentEncoding='gzip',
        )
        return key

    def put_run_record(
        self,
        *,
        records: list[RunRecord],
        invocation_id: str,
    ) -> str:
        """Store one JSON Lines audit record per invocation.

        Takes already-summarised records rather than ``FetchResult``
        objects so that the caller can discard each payload as soon
        as it is stored, instead of holding every body alive until
        the audit record is written.

        Parameters
        ----------
        records : list[RunRecord]
            One summary per fetch attempted in this invocation.
        invocation_id : str
            Lambda request id.

        Returns
        -------
        str
            The key written.
        """
        key = run_key(
            fetched_at=datetime.fromisoformat(
                records[0]['fetched_at_utc'],
            ),
            invocation_id=invocation_id,
        )
        lines = '\n'.join(
            json.dumps(record) for record in records
        )
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=f'{lines}\n'.encode(),
            ContentType='application/x-ndjson',
        )
        return key
