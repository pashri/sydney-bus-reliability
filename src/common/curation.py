"""Persistence of per-invocation curation audit records."""

import json
from datetime import datetime

import boto3
from aws_lambda_powertools import Logger

from src.common.types_ import CurationRecord

logger = Logger()


def curation_key(*, started_at: datetime, invocation_id: str) -> str:
    """Build the S3 key for one curation audit record.

    Parameters
    ----------
    started_at : datetime
        UTC start of the invocation, used for partitioning.
    invocation_id : str
        Lambda request id, unique per invocation.

    Returns
    -------
    str
        S3 key for a JSON Lines object.

    Notes
    -----
    The partition is the date the invocation *started*, so a run
    spanning UTC midnight files under the day it began.
    """
    return (
        f'curated/curation_run/dt={started_at:%Y-%m-%d}/'
        f'{invocation_id}.jsonl'
    )


# One public method by design: this is the repository pattern, and
# curation has exactly one write. pylint's min-public-methods=2
# default does not fit that shape.
class CurationRepository:  # pylint: disable=too-few-public-methods
    """Writes curation audit records to one S3 bucket.

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

    def put_record(self, *, record: CurationRecord) -> str:
        """Store one audit record as a single JSON Lines object.

        Parameters
        ----------
        record : CurationRecord
            The invocation summary to store.

        Returns
        -------
        str
            The key written.
        """
        key = curation_key(
            started_at=datetime.fromisoformat(
                record['started_at_utc'],
            ),
            invocation_id=record['invocation_id'],
        )
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=f'{json.dumps(record)}\n'.encode(),
            ContentType='application/x-ndjson',
        )
        return key
