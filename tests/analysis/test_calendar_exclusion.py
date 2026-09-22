"""Tests for the calendar exclusion seed and its loader."""

from datetime import date
from pathlib import Path

import pytest

from analysis.calendar_exclusion import (
    Exclusion,
    ExclusionType,
    dates_in_range,
    excluded_dates,
    expand_row,
    is_term_weekday,
    load_exclusions,
    read_seed,
    seed_path,
)

SEED_YEAR = 2026


def _row(**overrides: str) -> dict[str, str]:
    """Build a seed row with defaults."""
    row = {
        'start_date': '2026-10-05',
        'end_date': '2026-10-05',
        'exclusion_type': 'public_holiday',
        'reason': 'Labour Day',
        'source': 'https://www.nsw.gov.au/about-nsw/public-holidays',
    }
    row.update(overrides)
    return row


def test_seed_path_names_the_year() -> None:
    assert seed_path(year=SEED_YEAR).name == 'calendar_exclusions_2026.csv'


def test_dates_in_range_includes_both_endpoints() -> None:
    days = list(dates_in_range(start=date(2026, 10, 5), end=date(2026, 10, 7)))
    assert days == [date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7)]


def test_dates_in_range_single_day() -> None:
    days = list(dates_in_range(start=date(2026, 10, 5), end=date(2026, 10, 5)))
    assert days == [date(2026, 10, 5)]


def test_dates_in_range_rejects_reversed_range() -> None:
    with pytest.raises(ValueError, match='ends before it starts'):
        list(dates_in_range(start=date(2026, 10, 7), end=date(2026, 10, 5)))


def test_expand_row_single_date() -> None:
    expanded = list(expand_row(row=_row()))
    assert expanded == [
        Exclusion(
            date=date(2026, 10, 5),
            exclusion_type=ExclusionType.PUBLIC_HOLIDAY,
            reason='Labour Day',
            source='https://www.nsw.gov.au/about-nsw/public-holidays',
        ),
    ]


def test_expand_row_expands_a_range() -> None:
    row = _row(
        start_date='2026-09-28',
        end_date='2026-10-09',
        exclusion_type='school_holiday',
        reason='Spring holidays',
    )
    expanded = list(expand_row(row=row))
    assert len(expanded) == 12
    assert expanded[0].date == date(2026, 9, 28)
    assert expanded[-1].date == date(2026, 10, 9)
    assert {item.exclusion_type for item in expanded} == {
        ExclusionType.SCHOOL_HOLIDAY,
    }


def test_expand_row_rejects_unknown_type() -> None:
    with pytest.raises(ValueError):
        list(expand_row(row=_row(exclusion_type='pupil_free')))


def test_expand_row_rejects_unparseable_date() -> None:
    with pytest.raises(ValueError):
        list(expand_row(row=_row(start_date='5/10/2026')))


def test_read_seed_expands_every_row(tmp_path: Path) -> None:
    path = tmp_path / 'seed.csv'
    path.write_text(
        'start_date,end_date,exclusion_type,reason,source\n'
        '2026-10-05,2026-10-05,public_holiday,Labour Day,nsw.gov.au\n'
        '2026-10-12,2026-10-13,school_development_day,SDD,edu.nsw\n',
        encoding='utf-8',
    )
    assert [item.date for item in read_seed(path=path)] == [
        date(2026, 10, 5),
        date(2026, 10, 12),
        date(2026, 10, 13),
    ]


def test_load_exclusions_missing_year() -> None:
    with pytest.raises(FileNotFoundError, match='no calendar exclusion seed'):
        load_exclusions(year=1999)


def test_load_exclusions_is_sorted_and_deduplicated() -> None:
    records = load_exclusions(year=SEED_YEAR)
    keys = [(item.date, item.exclusion_type) for item in records]
    assert keys == sorted(keys)
    assert len(keys) == len(set(keys))


def test_load_exclusions_stays_within_the_year() -> None:
    records = load_exclusions(year=SEED_YEAR)
    assert {item.date.year for item in records} == {SEED_YEAR}


def test_seed_holds_every_nsw_public_holiday() -> None:
    records = load_exclusions(year=SEED_YEAR)
    holidays = excluded_dates(
        exclusions=records,
        exclusion_types=frozenset({ExclusionType.PUBLIC_HOLIDAY}),
    )
    assert holidays == {
        date(2026, 1, 1),
        date(2026, 1, 26),
        date(2026, 4, 3),
        date(2026, 4, 4),
        date(2026, 4, 5),
        date(2026, 4, 6),
        date(2026, 4, 25),
        date(2026, 6, 8),
        date(2026, 10, 5),
        date(2026, 12, 25),
        date(2026, 12, 26),
        date(2026, 12, 28),
    }


def test_seed_covers_the_spring_holidays() -> None:
    records = load_exclusions(year=SEED_YEAR)
    holidays = excluded_dates(
        exclusions=records,
        exclusion_types=frozenset({ExclusionType.SCHOOL_HOLIDAY}),
    )
    assert date(2026, 9, 25) not in holidays
    assert date(2026, 9, 28) in holidays
    assert date(2026, 10, 9) in holidays
    assert date(2026, 10, 12) not in holidays


def test_labour_day_carries_two_exclusions() -> None:
    records = load_exclusions(year=SEED_YEAR)
    kinds = {
        item.exclusion_type
        for item in records
        if item.date == date(2026, 10, 5)
    }
    assert kinds == {
        ExclusionType.PUBLIC_HOLIDAY,
        ExclusionType.SCHOOL_HOLIDAY,
    }


def test_excluded_dates_defaults_to_every_type() -> None:
    records = load_exclusions(year=SEED_YEAR)
    assert excluded_dates(exclusions=records) == excluded_dates(
        exclusions=records,
        exclusion_types=frozenset(ExclusionType),
    )


def test_is_term_weekday_accepts_an_ordinary_weekday() -> None:
    excluded = excluded_dates(exclusions=load_exclusions(year=SEED_YEAR))
    assert is_term_weekday(day=date(2026, 9, 25), excluded=excluded)


def test_is_term_weekday_rejects_a_weekend() -> None:
    weekend = date(2026, 9, 26)
    assert is_term_weekday(day=weekend, excluded=frozenset()) is False


def test_is_term_weekday_rejects_an_excluded_day() -> None:
    excluded = excluded_dates(exclusions=load_exclusions(year=SEED_YEAR))
    assert is_term_weekday(day=date(2026, 10, 5), excluded=excluded) is False


def test_collection_window_usable_weekdays() -> None:
    excluded = excluded_dates(exclusions=load_exclusions(year=SEED_YEAR))
    usable = [
        day
        for day in dates_in_range(
            start=date(2026, 9, 21), end=date(2026, 11, 6),
        )
        if is_term_weekday(day=day, excluded=excluded)
    ]
    assert len(usable) == 24
    assert usable[0] == date(2026, 9, 21)
    assert date(2026, 10, 5) not in usable
    assert date(2026, 10, 12) not in usable
    assert usable[-1] == date(2026, 11, 6)
