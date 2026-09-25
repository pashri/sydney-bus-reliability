"""Tests for the checks run on each merged service day."""

import json

import duckdb
import pytest

from merger.anomalies import (
    DayMeasures,
    breaches,
    fact_path,
    fetch,
    measure_day,
    report,
)

CLEAN: DayMeasures = DayMeasures(
    times_far_from_schedule=0,
    times_before_2000=0,
    duplicate_stop_keys=0,
    duplicate_trip_keys=0,
    inconsistent_trips=0,
    unscheduled_share=0.002,
    reliable_share=0.87,
    trips=44_000,
)

STOP_ROWS: str = """
select * from (values
    ('t1', 's1', 1, TIMESTAMPTZ '2026-09-22 08:00:00+10',
     TIMESTAMPTZ '2026-09-22 08:00:30+10',
     TIMESTAMPTZ '2026-09-23 08:00:00+10', 'SCHEDULED', true),
    ('t1', 's2', 2, TIMESTAMPTZ '2026-09-22 08:10:00+10',
     TIMESTAMPTZ '2026-09-23 08:10:00+10',
     TIMESTAMPTZ '2026-09-22 08:10:00+10', 'SCHEDULED', false),
    ('t2', 's1', 1, TIMESTAMPTZ '2026-09-22 09:00:00+10',
     TIMESTAMPTZ '1970-01-01 00:00:00+00',
     TIMESTAMPTZ '2026-09-22 09:00:00+10', 'SCHEDULED', false),
    ('t2', 's1', 1, TIMESTAMPTZ '2026-09-22 09:00:00+10', null, null,
     'NO_DATA', false),
    ('t3', 's9', 1, null, null, null, 'NO_DATA', false),
    ('', 's1', 1, null, TIMESTAMPTZ '2026-09-22 10:00:00+10', null,
     'SCHEDULED', false),
    ('', 's1', 1, null, TIMESTAMPTZ '2026-09-22 10:01:00+10', null,
     'SCHEDULED', false)
) as rows(trip_id, stop_id, stop_sequence, scheduled_arrival_utc,
          final_predicted_arrival_utc, final_predicted_departure_utc,
          schedule_relationship, is_reliable)
"""

TRIP_ROWS: str = """
select * from (values
    (DATE '2026-09-22', 't1', '20260922', 'SCHEDULED', 0, null, null,
     TIMESTAMPTZ '2026-09-22 07:00:00+10',
     TIMESTAMPTZ '2026-09-22 09:00:00+10'),
    (DATE '2026-09-22', 't2', '20260923', 'CANCELED', 0, null, null,
     TIMESTAMPTZ '2026-09-22 07:00:00+10',
     TIMESTAMPTZ '2026-09-22 09:00:00+10'),
    (DATE '2026-09-22', 't2', '20260923', 'SCHEDULED', 0, null, null,
     TIMESTAMPTZ '2026-09-22 07:00:00+10',
     TIMESTAMPTZ '2026-09-22 09:00:00+10'),
    (DATE '2026-09-22', 't3', '20260925', 'SCHEDULED', 0, null, null,
     TIMESTAMPTZ '2026-09-22 07:00:00+10',
     TIMESTAMPTZ '2026-09-22 09:00:00+10')
) as rows(service_date, trip_id, start_date, final_status, canceled_polls,
          first_canceled_at_utc, last_canceled_at_utc, first_seen_at_utc,
          last_seen_at_utc)
"""


@pytest.fixture(name='measures', scope='module')
def _measures(tmp_path_factory: pytest.TempPathFactory) -> DayMeasures:
    """Measure a small day planted with one of each fault."""
    root = tmp_path_factory.mktemp('facts')
    stops, trips = root / 'trip_stop.parquet', root / 'trip.parquet'
    con = duckdb.connect()
    con.execute('LOAD icu')
    con.execute(f"copy ({STOP_ROWS}) to '{stops}' (format parquet)")
    con.execute(f"copy ({TRIP_ROWS}) to '{trips}' (format parquet)")
    return measure_day(
        connection=con, trip_stop_path=str(stops), trip_path=str(trips),
    )


def test_measure_day_counts_times_a_day_off_the_schedule(
    measures: DayMeasures,
) -> None:
    """A day-late departure and a day-late arrival; 1970 counts apart."""
    assert measures.times_far_from_schedule == 2


def test_measure_day_counts_epoch_times(measures: DayMeasures) -> None:
    """A 1970 arrival is a zero read literally."""
    assert measures.times_before_2000 == 1


def test_measure_day_ignores_duplicates_without_a_trip(
    measures: DayMeasures,
) -> None:
    """Keyless feed rows share a key legitimately; a real trip must not."""
    assert measures.duplicate_stop_keys == 1


def test_measure_day_checks_the_trip_table(measures: DayMeasures) -> None:
    """One duplicated trip, one cancelled with no cancelled poll, one
    whose start date is neither the service day nor the next."""
    assert (
        measures.duplicate_trip_keys, measures.inconsistent_trips,
        measures.trips,
    ) == (1, 2, 4)


def test_measure_day_takes_shares_of_real_rows(
    measures: DayMeasures,
) -> None:
    """Unscheduled share ignores keyless rows; reliable share counts only
    SCHEDULED rows."""
    assert measures.unscheduled_share == pytest.approx(1 / 5)
    assert measures.reliable_share == pytest.approx(1 / 5)


def test_breaches_passes_an_ordinary_day() -> None:
    """Nothing is flagged on a day like the ones collected so far."""
    assert not breaches(measures=CLEAN)


@pytest.mark.parametrize(('field', 'value'), [
    ('times_far_from_schedule', 1),
    ('times_before_2000', 1),
    ('duplicate_stop_keys', 1),
    ('duplicate_trip_keys', 1),
    ('inconsistent_trips', 1),
    ('unscheduled_share', 0.031),
    ('reliable_share', 0.79),
    ('trips', 14_999),
])
def test_breaches_flags_each_check(field: str, value: float) -> None:
    """Each check trips on its own."""
    faulty = DayMeasures(**{**CLEAN.__dict__, field: value})
    assert [breach.name for breach in breaches(measures=faulty)] == [field]


def test_report_emits_the_number_of_breaches(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """One EMF line per day, zero included, so the alarm has data."""
    faulty = DayMeasures(**{**CLEAN.__dict__, 'times_before_2000': 3})
    assert report(measures=faulty, service_date='2026-09-22') == 1
    emitted = [
        json.loads(line) for line in capsys.readouterr().out.splitlines()
        if '"_aws"' in line
    ]
    assert [line['Anomalies'] for line in emitted] == [[1.0]]
    assert emitted[0]['service'] == 'merger'


def test_report_emits_zero_on_a_clean_day(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A clean day still reports, as zero."""
    assert report(measures=CLEAN, service_date='2026-09-22') == 0
    emitted = [
        json.loads(line) for line in capsys.readouterr().out.splitlines()
        if '"_aws"' in line
    ]
    assert [line['Anomalies'] for line in emitted] == [[0.0]]


def test_fact_path_names_the_merged_file() -> None:
    """The checks read what the merger just wrote."""
    assert fact_path(
        bucket='b', table='trip_stop', service_date='2026-09-22',
    ) == 's3://b/curated/fact_trip_stop/service_date=2026-09-22/data.parquet'


def test_fetch_refuses_a_query_with_no_row() -> None:
    """A check that returns nothing is an error, not a pass."""
    with pytest.raises(RuntimeError, match='no result'):
        fetch(
            connection=duckdb.connect(),
            sql='select 1 where ? is null', path='x',
        )
