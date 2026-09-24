"""Reading the TfNSW static GTFS bundle without extracting it.

The bundle is about 95 MiB zipped and 500 MiB extracted, so it does not
fit in a default Lambda ``/tmp``. Members are streamed one at a time
out of the archive and never written to disk.

Change detection hashes the zip bytes, never the filename. The server
names each download with a generation timestamp
(``buses_GTFS_PROD_20260918103100.zip``) that changes on every rebuild
whether or not the contents differ.
"""

import csv
import hashlib
import io
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Final

from aws_lambda_powertools import Logger

logger = Logger()

ENCODING: Final[str] = 'utf-8-sig'
"""GTFS text files are UTF-8 and may carry a byte-order mark."""

SNAPSHOT_LABEL_FORMAT: Final[str] = '%Y-%m-%dT%H%M%SZ'
"""How a timetable snapshot's ``valid_from`` is written: its check time.

UTC, to the second, with no colons. DuckDB reads the value from the
path as text rather than guessing a timestamp type, and sorting the
text sorts the snapshots in time order.
"""


@dataclass(frozen=True, slots=True)
class StaticBundle:
    """One downloaded static GTFS bundle."""

    payload: bytes
    sha256: str
    filename: str


def zip_sha256(*, payload: bytes) -> str:
    """Hash the raw bytes of a zip archive.

    Parameters
    ----------
    payload : bytes
        Complete archive bytes.

    Returns
    -------
    str
        Hex-encoded SHA-256 digest.
    """
    return hashlib.sha256(payload).hexdigest()


def member_rows(
    *,
    archive: zipfile.ZipFile,
    name: str,
) -> Iterator[dict[str, str]]:
    """Stream one member's rows as dictionaries.

    Parameters
    ----------
    archive : zipfile.ZipFile
        Open archive.
    name : str
        Member filename, e.g. ``stop_times.txt``.

    Yields
    ------
    dict[str, str]
        One row, keyed by column name, with quotes already stripped by
        the CSV reader.

    Raises
    ------
    KeyError
        If the member is absent, which means the feed changed shape.
    """
    with archive.open(name) as member:
        stream = io.TextIOWrapper(member, encoding=ENCODING)
        yield from csv.DictReader(stream)
