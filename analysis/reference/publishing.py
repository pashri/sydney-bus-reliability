"""Putting a built reference table where analyses will read it.

A table is written locally first and uploaded only when asked, so what
replaces the vintage every analysis reads can be looked at before it
does.

A single object needs no staging prefix. The put either completes or
leaves nothing, so a reader taking the latest vintage cannot find a
half-written one. A table spread over several files would need the
staging step, since the vintage can then be partly there.
"""

import argparse
import logging
from datetime import date
from pathlib import Path

import boto3

logger = logging.getLogger(__name__)


def add_publish_arguments(*, parser: argparse.ArgumentParser) -> None:
    """Add the arguments every publishing script shares.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Parser to add them to.
    """
    parser.add_argument(
        '--vintage', type=date.fromisoformat, default=date.today(),
    )
    parser.add_argument('--bucket', default=None)
    parser.add_argument('--profile', default=None)
    parser.add_argument('--publish', action='store_true')


def upload(
    *,
    path: Path,
    bucket: str,
    key: str,
    profile: str | None = None,
) -> None:
    """Put one built file in its published place.

    Parameters
    ----------
    path : Path
        Local file to upload.
    bucket : str
        Destination bucket.
    key : str
        Destination key.
    profile : str | None, optional
        Named AWS profile. Writing needs one with write access.
    """
    session = boto3.Session(profile_name=profile)
    session.client('s3').upload_file(str(path), bucket, key)
    logger.info('published s3://%s/%s', bucket, key)
