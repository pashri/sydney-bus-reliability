"""Naming and locating published reference tables.

Reference tables are partitioned by the date their build ran, not by
the date their sources were released. The ABS release identifiers are
columns instead, because a table can mix them: a build can pair the
2021 boundaries with a later SEIFA, and pinning one in the path would
imply they move together.

Selection differs from the curated layer. A fact row joins to the
timetable that applied on its own service day, but reference data has
no such correspondence: the newest boundaries are the best answer for
every day, including days already collected. Latest vintage wins.
"""

from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Final

PREFIX: Final[str] = 'reference'
"""Top-level S3 prefix holding every reference table."""

PENDING_PREFIX: Final[str] = 'reference/_pending'
"""Where a build writes before it is complete.

A reader takes the latest vintage, so a half-written partition would
be selected as though it were finished. Files land here and are
promoted only once all of them have arrived.
"""

MANIFEST_NAME: Final[str] = 'manifest.json'
"""Marks a vintage as complete, and records where it came from."""


class ReferenceTable(StrEnum):
    """A table published under the reference prefix.

    The name carries the geographic grain where the table has one.
    Every other census table is at SA1, but the Working Population
    Profile is not published that finely: SA2 is the finest available,
    and an SA2 is large enough that treating its figures as if they
    described a walking catchment would be wrong. The name is the
    warning.
    """

    STOP_GEOGRAPHY = 'stop_geography'
    STOP_MESHBLOCK = 'stop_meshblock'
    CENSUS_SA1 = 'census_sa1'
    CENSUS_SA1_COLUMNS = 'census_sa1_columns'
    WPP_SA2 = 'wpp_sa2'
    STATION = 'station'
    STOP_INTERCHANGE = 'stop_interchange'
    BUS_PRIORITY = 'bus_priority'


@dataclass(frozen=True, slots=True)
class SourceRelease:
    """Which published editions a build drew on."""

    asgs_edition: str
    seifa_release: str
    census_year: int


def table_prefix(*, table: ReferenceTable, pending: bool = False) -> str:
    """Locate a table's prefix.

    Parameters
    ----------
    table : ReferenceTable
        Table to locate.
    pending : bool, optional
        True for the staging prefix used before promotion.

    Returns
    -------
    str
        Prefix with no trailing slash.
    """
    root = PENDING_PREFIX if pending else PREFIX
    return f'{root}/{table}'


def vintage_prefix(
    *,
    table: ReferenceTable,
    vintage: date,
    pending: bool = False,
) -> str:
    """Locate one vintage of a table.

    Parameters
    ----------
    table : ReferenceTable
        Table to locate.
    vintage : date
        Date the build ran.
    pending : bool, optional
        True for the staging prefix used before promotion.

    Returns
    -------
    str
        Prefix with no trailing slash.
    """
    base = table_prefix(table=table, pending=pending)
    return f'{base}/vintage={vintage.isoformat()}'


def partition_prefix(
    *,
    table: ReferenceTable,
    vintage: date,
    partition: str,
    pending: bool = False,
) -> str:
    """Locate a sub-partition of one vintage.

    The census profiles keep one partition per published table rather
    than widening into a single schema of several thousand columns.

    Parameters
    ----------
    table : ReferenceTable
        Table to locate.
    vintage : date
        Date the build ran.
    partition : str
        Sub-partition value, such as an ABS table code.
    pending : bool, optional
        True for the staging prefix used before promotion.

    Returns
    -------
    str
        Prefix with no trailing slash.

    Raises
    ------
    ValueError
        If the partition value is empty or contains a path separator.
    """
    if not partition or '/' in partition or '=' in partition:
        raise ValueError(f'unusable partition value: {partition!r}')
    base = vintage_prefix(table=table, vintage=vintage, pending=pending)
    return f'{base}/table={partition}'


def manifest_key(*, vintage: date, pending: bool = False) -> str:
    """Locate the manifest marking a vintage complete.

    Parameters
    ----------
    vintage : date
        Date the build ran.
    pending : bool, optional
        True for the staging prefix used before promotion.

    Returns
    -------
    str
        Full object key.
    """
    root = PENDING_PREFIX if pending else PREFIX
    return f'{root}/_manifest/vintage={vintage.isoformat()}/{MANIFEST_NAME}'
