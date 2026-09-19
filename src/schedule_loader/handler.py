"""Daily capture of the TfNSW static GTFS bundle.

The bundle is fetched every day but written only when its content hash
changes. Writing unconditionally would produce ~365 identical snapshots
a year and destroy the meaning of ``valid_from``, which exists to record
*when the timetable changed* — the question you need answered when a
reliability metric moves and you have to say whether the service changed
or the buses got worse.

Urgency note: the bundle is forward-looking. Only 3 service_ids were
active on the generation day of the bundle measured on 19 September,
against 98 the next day. A bundle fetched today describes tomorrow
onward and cannot reconstruct a past day, so every day this does not run
is a day whose timetable is permanently lost.

The `handler` entry point itself is written in a later task, together
with its template resources, because a handler with nowhere to run
cannot be verified.
"""

import io
import json
import zipfile
from datetime import datetime
from http import HTTPStatus
from typing import Any, Final

import requests
from aws_lambda_powertools import Logger

from src.common.gtfs_static import StaticBundle, zip_sha256
from src.common.parquet import ParquetRepository
from src.common.types_ import ScheduleCheck
from src.schedule_loader.dimensions import SPECS, Dimension, dimension_batches

logger = Logger()

BUNDLE_URL: Final[str] = (
    'https://api.transport.nsw.gov.au/v1/gtfs/schedule/buses'
)
TIMEOUT: Final[float] = 120.0
"""Seconds. The bundle is ~95 MiB and took 21.5 s on a home connection."""

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
    rebuild whether or not the contents differ, so it is recorded for
    provenance but never used for change detection.

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
        bundle cannot be recovered later, so this must alarm rather
        than be swallowed.
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
        filename and hash are logged first, since a bare traceback
        would otherwise carry no bundle identity.
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
