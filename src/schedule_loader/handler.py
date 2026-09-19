"""Daily capture of the TfNSW static GTFS bundle.

The bundle is fetched every day but written only when its content hash
changes, so a ``valid_from`` partition marks a day the timetable
actually changed.

The bundle is forward-looking. It describes tomorrow onward and barely
covers its own generation day, so it cannot reconstruct a past day. A
day this does not run is a day whose timetable is lost for good.
"""

import io
import json
import os
import zipfile
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Any, Final

import boto3
import requests
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from common.gtfs_static import StaticBundle, zip_sha256
from common.parquet import ParquetRepository
from common.types_ import ScheduleCheck
from schedule_loader.dimensions import SPECS, Dimension, dimension_batches

logger = Logger()

BUNDLE_URL: Final[str] = (
    'https://api.transport.nsw.gov.au/v1/gtfs/schedule/buses'
)
TIMEOUT: Final[float] = 120.0  # seconds
"""Seconds. The bundle is about 95 MiB and takes tens of seconds."""

CHECK_PREFIX: Final[str] = 'curated/schedule_check/'


def schedule_check_key(
    *,
    checked_at_iso: str,
    invocation_id: str,
) -> str:
    """Build the S3 key for one schedule-check record.

    Parameters
    ----------
    checked_at_iso : str
        ISO 8601 UTC timestamp of the check.
    invocation_id : str
        Lambda request id.

    Returns
    -------
    str
        S3 key for a JSON Lines object.
    """
    checked_at = datetime.fromisoformat(checked_at_iso)
    return (
        f'{CHECK_PREFIX}dt={checked_at:%Y-%m-%d}/'
        f'{checked_at:%H%M%S}-{invocation_id}.jsonl'
    )


def read_filename(*, response: requests.Response) -> str:
    """Extract the bundle filename from the response headers.

    The name carries a generation timestamp that changes on every
    rebuild whether or not the contents differ. Record it for
    provenance, but never use it for change detection.

    Parameters
    ----------
    response : requests.Response
        Completed bundle response.

    Returns
    -------
    str
        Filename, or an empty string when the header is absent or
        uses the RFC 5987 ``filename*=`` form, which is not
        supported.
    """
    disposition = response.headers.get('Content-Disposition', '')
    segments = (part.strip() for part in disposition.split(';'))
    return next(
        (
            segment.removeprefix('filename=').strip('"')
            for segment in segments
            if segment.startswith('filename=')
        ),
        '',
    )


def fetch_bundle(*, api_key: str) -> StaticBundle:
    """Download the static GTFS bundle.

    Parameters
    ----------
    api_key : str
        TfNSW API key.

    Returns
    -------
    StaticBundle
        Payload, content hash and server-supplied filename.

    Raises
    ------
    RuntimeError
        If the server returns anything other than 200. The day's
        bundle cannot be recovered later, so a failure must be loud.
    """
    with requests.get(
        BUNDLE_URL,
        headers={'Authorization': f'apikey {api_key}'},
        timeout=TIMEOUT,
    ) as response:
        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(
                f'static GTFS fetch returned {response.status_code}',
            )
        payload = response.content
        filename = read_filename(response=response)
    return StaticBundle(
        payload=payload,
        sha256=zip_sha256(payload=payload),
        filename=filename,
    )


def latest_sha256(*, client: Any, bucket: str) -> str | None:
    """Read the most recent check's content hash.

    Parameters
    ----------
    client : Any
        Boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.

    Returns
    -------
    str | None
        Previous hash, or None when no check has ever been written.
    """
    paginator = client.get_paginator('list_objects_v2')
    keys = [
        item['Key']
        for page in paginator.paginate(
            Bucket=bucket, Prefix=CHECK_PREFIX,
        )
        for item in page.get('Contents', ())
    ]
    if not keys:
        return None
    return read_hash(client=client, bucket=bucket, key=max(keys))


def read_hash(*, client: Any, bucket: str, key: str) -> str:
    """Read the ``zip_sha256`` field out of one check record.

    Parameters
    ----------
    client : Any
        Boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.
    key : str
        Key of the check record.

    Returns
    -------
    str
        The recorded content hash.
    """
    body = client.get_object(Bucket=bucket, Key=key)['Body'].read()
    record: ScheduleCheck = json.loads(body.decode())
    return record['zip_sha256']


def write_dimensions(
    *,
    bundle: StaticBundle,
    repository: ParquetRepository,
    valid_from: str,
) -> dict[str, int]:
    """Write every dimension snapshot for one bundle.

    Parameters
    ----------
    bundle : StaticBundle
        The downloaded bundle.
    repository : ParquetRepository
        Destination for Parquet objects.
    valid_from : str
        Partition value, as ``YYYY-MM-DD``.

    Returns
    -------
    dict[str, int]
        Rows written per dimension.

    Raises
    ------
    zipfile.BadZipFile
        If the bundle payload is corrupt or truncated. The bundle's
        filename and hash are logged first, since the traceback alone
        does not identify which bundle failed.
    """
    written: dict[str, int] = {}
    try:
        archive = zipfile.ZipFile(io.BytesIO(bundle.payload))
    except zipfile.BadZipFile:
        logger.exception(
            'Static GTFS bundle is not a valid zip',
            extra={
                'zip_filename': bundle.filename,
                'zip_sha256': bundle.sha256,
            },
        )
        raise
    with archive:
        for dimension in Dimension:
            written[dimension.value] = repository.put_batches(
                key=(
                    f'curated/{dimension.value}/'
                    f'valid_from={valid_from}/data.parquet'
                ),
                schema=SPECS[dimension].schema,
                batches=dimension_batches(
                    archive=archive, dimension=dimension,
                ),
            )
    return written


def read_api_key(*, parameter_name: str, session: boto3.Session) -> str:
    """Read the TfNSW API key from SSM Parameter Store.

    Parameters
    ----------
    parameter_name : str
        SSM parameter name, which must carry a leading slash.
    session : boto3.Session
        Session to create the SSM client from.

    Returns
    -------
    str
        Decrypted API key.
    """
    client = session.client('ssm')
    response = client.get_parameter(
        Name=parameter_name, WithDecryption=True,
    )
    return str(response['Parameter']['Value'])


@logger.inject_lambda_context
def handler(
    event: dict[str, Any],  # pylint: disable=unused-argument
    context: LambdaContext,
) -> ScheduleCheck:
    """Fetch the static bundle and snapshot it only when it changed.

    Parameters
    ----------
    event : dict[str, Any]
        EventBridge event, unused.
    context : LambdaContext
        Lambda context, used for the invocation id.

    Returns
    -------
    ScheduleCheck
        The check record written for this run.
    """
    session = boto3.Session()
    bucket = os.environ['BUCKET_NAME']
    checked_at = datetime.now(tz=UTC)
    bundle = fetch_bundle(
        api_key=read_api_key(
            parameter_name=os.environ['API_KEY_PARAMETER_NAME'],
            session=session,
        ),
    )
    client = session.client('s3')
    previous = latest_sha256(client=client, bucket=bucket)
    changed = previous != bundle.sha256
    valid_from = f'{checked_at:%Y-%m-%d}' if changed else None
    if changed:
        logger.info(
            'timetable changed, writing snapshot',
            extra={'valid_from': valid_from},
        )
        write_dimensions(
            bundle=bundle,
            repository=ParquetRepository(
                bucket=bucket, session=session,
            ),
            valid_from=str(valid_from),
        )
    check = ScheduleCheck(
        checked_at_utc=checked_at.isoformat(),
        zip_sha256=bundle.sha256,
        zip_filename=bundle.filename,
        changed=changed,
        valid_from=valid_from,
    )
    put_check(
        client=client,
        bucket=bucket,
        check=check,
        invocation_id=context.aws_request_id,
    )
    return check


def put_check(
    *,
    client: Any,
    bucket: str,
    check: ScheduleCheck,
    invocation_id: str,
) -> str:
    """Store one schedule-check record, changed or not.

    Parameters
    ----------
    client : Any
        Boto3 S3 client.
    bucket : str
        Destination bucket.
    check : ScheduleCheck
        The record to store.
    invocation_id : str
        Lambda request id.

    Returns
    -------
    str
        The key written.
    """
    key = schedule_check_key(
        checked_at_iso=check['checked_at_utc'],
        invocation_id=invocation_id,
    )
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=f'{json.dumps(check)}\n'.encode(),
        ContentType='application/x-ndjson',
    )
    return key
