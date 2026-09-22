"""Dates on which NSW bus demand is not ordinary term-time demand.

The seed CSV holds closed date ranges, one row per named event, and is
expanded here into one record per calendar date. A date can carry more
than one exclusion: a public holiday inside the school holidays
produces a record of each kind, so the table is keyed on date and
exclusion type together, never on date alone.

Ranges include both endpoints. A school holiday that runs on past 31
December is clipped to the year, so it appears in the seed for each
year it touches.
"""

import calendar
import csv
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Final

SEED_DIRECTORY: Final[Path] = Path(__file__).parent
"""Directory holding the per-year seed files."""


class ExclusionType(StrEnum):
    """Why a date is excluded from term-time comparisons."""

    PUBLIC_HOLIDAY = 'public_holiday'
    SCHOOL_HOLIDAY = 'school_holiday'
    SCHOOL_DEVELOPMENT_DAY = 'school_development_day'


@dataclass(frozen=True, slots=True)
class Exclusion:
    """One excluded calendar date."""

    date: date
    exclusion_type: ExclusionType
    reason: str
    source: str


def seed_path(*, year: int) -> Path:
    """Locate the seed file for one year.

    Parameters
    ----------
    year : int
        Calendar year the seed covers.

    Returns
    -------
    Path
        Path to the seed CSV, which may not exist.
    """
    return SEED_DIRECTORY / f'calendar_exclusions_{year}.csv'


def dates_in_range(*, start: date, end: date) -> Iterator[date]:
    """Yield every date in a closed range.

    Parameters
    ----------
    start : date
        First date, included.
    end : date
        Last date, included.

    Returns
    -------
    Iterator[date]
        Dates in ascending order.

    Raises
    ------
    ValueError
        If the range ends before it starts.
    """
    if end < start:
        raise ValueError(f'range ends before it starts: {start} to {end}')
    for offset in range((end - start).days + 1):
        yield start + timedelta(days=offset)


def expand_row(*, row: dict[str, str]) -> Iterator[Exclusion]:
    """Expand one seed row into its individual dates.

    Parameters
    ----------
    row : dict[str, str]
        One row of a seed CSV.

    Returns
    -------
    Iterator[Exclusion]
        One exclusion per date in the row's range.

    Raises
    ------
    ValueError
        If a date cannot be parsed, the range is reversed, or the
        exclusion type is not recognised.
    """
    start = date.fromisoformat(row['start_date'])
    end = date.fromisoformat(row['end_date'])
    exclusion_type = ExclusionType(row['exclusion_type'])
    for day in dates_in_range(start=start, end=end):
        yield Exclusion(
            date=day,
            exclusion_type=exclusion_type,
            reason=row['reason'],
            source=row['source'],
        )


def read_seed(*, path: Path) -> Iterator[Exclusion]:
    """Read and expand every row of a seed file.

    Parameters
    ----------
    path : Path
        Path to a seed CSV.

    Returns
    -------
    Iterator[Exclusion]
        Exclusions in seed order.

    Raises
    ------
    ValueError
        If any row is malformed.
    """
    with path.open(newline='', encoding='utf-8') as handle:
        for row in csv.DictReader(handle):
            yield from expand_row(row=row)


def load_exclusions(*, year: int) -> list[Exclusion]:
    """Build the exclusion table for one year.

    Parameters
    ----------
    year : int
        Calendar year to load.

    Returns
    -------
    list[Exclusion]
        Records sorted by date then exclusion type, with exact
        duplicates removed.

    Raises
    ------
    FileNotFoundError
        If no seed file exists for the year.
    ValueError
        If any row is malformed.
    """
    path = seed_path(year=year)
    if not path.is_file():
        raise FileNotFoundError(f'no calendar exclusion seed: {path}')
    records = set(read_seed(path=path))
    return sorted(records, key=lambda item: (item.date, item.exclusion_type))


def excluded_dates(
    *,
    exclusions: list[Exclusion],
    exclusion_types: frozenset[ExclusionType] | None = None,
) -> frozenset[date]:
    """Reduce exclusion records to the set of dates they cover.

    Parameters
    ----------
    exclusions : list[Exclusion]
        Records to reduce.
    exclusion_types : frozenset[ExclusionType] | None, optional
        Types to keep. All types are kept when None.

    Returns
    -------
    frozenset[date]
        Every date carrying at least one of the wanted types.
    """
    wanted = exclusion_types or frozenset(ExclusionType)
    return frozenset(
        item.date for item in exclusions if item.exclusion_type in wanted
    )


def is_term_weekday(*, day: date, excluded: frozenset[date]) -> bool:
    """Test whether a date is an ordinary school-term weekday.

    Parameters
    ----------
    day : date
        Date to test.
    excluded : frozenset[date]
        Dates covered by any exclusion.

    Returns
    -------
    bool
        True when the date is a weekday and carries no exclusion.
    """
    return day.weekday() < calendar.SATURDAY and day not in excluded
