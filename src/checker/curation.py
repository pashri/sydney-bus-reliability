"""Summarising the curation Lambdas' audit rows for a Sydney day."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Final

import boto3
import duckdb
from aws_lambda_powertools import Logger

from common.service_day import SYDNEY
from common.types_ import CurationJob

logger = Logger()

CURATION_PREFIX: Final[str] = 'curated/curation_run/'
CHECK_PREFIX: Final[str] = 'curated/schedule_check/'

CURATION_COLUMNS: Final[str] = (
    "{job: 'VARCHAR', invocation_id: 'VARCHAR',"
    " started_at_utc: 'VARCHAR', finished_at_utc: 'VARCHAR',"
    " partition: 'VARCHAR', objects_expected: 'BIGINT',"
    " objects_read: 'BIGINT', rows_in: 'BIGINT', rows_out: 'BIGINT',"
    " dupes_collapsed: 'BIGINT', dupes_differing_position: 'BIGINT',"
    " unjoined_route_ids: 'BIGINT', unjoined_trip_ids: 'BIGINT',"
    " unjoined_stop_ids: 'BIGINT', peak_rss_mb: 'BIGINT',"
    " error: 'VARCHAR'}"
)
"""Every ``CurationRecord`` field, typed for DuckDB's JSON reader.

Declared rather than inferred, so a column that is null for a whole
day does not change type between one read and the next.
"""

CHECK_COLUMNS: Final[str] = (
    "{checked_at_utc: 'VARCHAR', zip_sha256: 'VARCHAR',"
    " zip_filename: 'VARCHAR', changed: 'BOOLEAN',"
    " valid_from: 'VARCHAR'}"
)
"""Every ``ScheduleCheck`` field, typed for DuckDB's JSON reader."""

SUMMED_FIELDS: Final[tuple[str, ...]] = (
    'objects_expected',
    'objects_read',
    'rows_in',
    'rows_out',
    'dupes_collapsed',
    'dupes_differing_position',
    'unjoined_route_ids',
    'unjoined_trip_ids',
    'unjoined_stop_ids',
)
"""Counters totalled across a day's runs."""


@dataclass(frozen=True, slots=True)
class JobDay:
    """One curation job's runs across one Sydney day.

    Attributes
    ----------
    runs_expected : int
        Partitions the job's schedule should have produced.
    runs_seen : int
        Distinct partitions actually recorded.
    missing_partitions : list[str]
        Expected partitions with no record, in order.
    totals : dict[str, int]
        Counters summed across the day's runs.
    peak_rss_mb : int
        Largest peak memory seen across the day's runs.
    errors : int
        Runs that recorded a non-null error.
    """

    runs_expected: int
    runs_seen: int
    missing_partitions: list[str]
    totals: dict[str, int]
    peak_rss_mb: int
    errors: int


@dataclass(frozen=True, slots=True)
class ScheduleDay:
    """The schedule loader's daily timetable check.

    The loader records a ``ScheduleCheck`` rather than a
    ``CurationRecord``, so only attendance and whether the timetable
    moved can be reported for it.

    Attributes
    ----------
    checks_expected : int
        Checks the loader's schedule should have produced.
    checks_seen : int
        Checks actually recorded.
    changed : bool
        Whether any of the day's checks saw a new timetable.
    """

    checks_expected: int
    checks_seen: int
    changed: bool


@dataclass(frozen=True, slots=True)
class CurationDay:
    """Every curation job's health across one Sydney day.

    Attributes
    ----------
    day : date
        The Sydney calendar date summarised.
    hours_in_day : int
        Real hours the day held, 23 or 25 across a daylight-saving
        transition and 24 otherwise.
    jobs : dict[str, JobDay]
        Per-job summary, keyed by job name.
    schedule : ScheduleDay
        The schedule loader's own summary.
    """

    day: date
    hours_in_day: int
    jobs: dict[str, JobDay]
    schedule: ScheduleDay


def sydney_day_bounds(*, day: date) -> tuple[datetime, datetime]:
    """Find a Sydney day's start and end as instants.

    Both are returned in UTC. Subtracting two datetimes that share a
    ``tzinfo`` ignores the zone and measures wall-clock time, which
    reports every day as 24 hours however the offset moved.

    Parameters
    ----------
    day : date
        Sydney calendar date.

    Returns
    -------
    tuple[datetime, datetime]
        Midnight opening the day and midnight closing it, in UTC.
    """
    start = datetime.combine(day, time.min, tzinfo=SYDNEY)
    end = datetime.combine(
        day + timedelta(days=1), time.min, tzinfo=SYDNEY,
    )
    return start.astimezone(UTC), end.astimezone(UTC)


def hours_in_day(*, day: date) -> int:
    """Count the real hours a Sydney day holds.

    A day is 23 or 25 hours long across a daylight-saving transition,
    so a fixed 24 would report a phantom missing hour twice a year.

    Parameters
    ----------
    day : date
        Sydney calendar date.

    Returns
    -------
    int
        Hours between this day's midnight and the next.
    """
    start, end = sydney_day_bounds(day=day)
    return int((end - start).total_seconds() // 3600)


def expected_hour_partitions(*, day: date) -> list[str]:
    """List the UTC hour partitions a Sydney day covers.

    The compactor names its partition after the UTC hour it read, so
    a Sydney day maps to a run of UTC hours rather than to a date.

    Parameters
    ----------
    day : date
        Sydney calendar date.

    Returns
    -------
    list[str]
        One ``YYYY-MM-DDTHH`` label per hour, in order.
    """
    opening, _ = sydney_day_bounds(day=day)
    return [
        f'{opening + timedelta(hours=offset):%Y-%m-%dT%H}'
        for offset in range(hours_in_day(day=day))
    ]


def summarize_job(
    records: list[dict[str, Any]], *, expected: list[str],
) -> JobDay:
    """Summarise one job's runs against the partitions it owed.

    Parameters
    ----------
    records : list[dict[str, Any]]
        The job's ``CurationRecord`` rows, from any partition.
    expected : list[str]
        Partitions the job's schedule should have produced.

    Returns
    -------
    JobDay
        Attendance, totals and errors for the day.
    """
    wanted = set(expected)
    mine = [r for r in records if r['partition'] in wanted]
    seen = {record['partition'] for record in mine}
    totals = {
        field: sum(int(record[field]) for record in mine)
        for field in SUMMED_FIELDS
    }
    return JobDay(
        runs_expected=len(expected),
        runs_seen=len(seen),
        missing_partitions=[
            partition for partition in expected if partition not in seen
        ],
        totals=totals,
        peak_rss_mb=max(
            (int(record['peak_rss_mb']) for record in mine), default=0,
        ),
        errors=sum(1 for record in mine if record['error']),
    )


def summarize_curation(
    records: list[dict[str, Any]],
    *,
    day: date,
    checks: list[dict[str, Any]],
) -> CurationDay:
    """Summarise every curation job's day.

    Parameters
    ----------
    records : list[dict[str, Any]]
        ``CurationRecord`` rows from the days surrounding this one.
    day : date
        Sydney calendar date summarised.
    checks : list[dict[str, Any]]
        The schedule loader's checks for the day.

    Returns
    -------
    CurationDay
        Per-job attendance and health.
    """
    hours = expected_hour_partitions(day=day)
    expected = {
        CurationJob.COMPACTOR.value: hours,
        CurationJob.MERGER.value: [f'{day:%Y-%m-%d}'],
    }
    jobs = {
        job: summarize_job(
            [record for record in records if record['job'] == job],
            expected=partitions,
        )
        for job, partitions in expected.items()
    }
    return CurationDay(
        day=day,
        hours_in_day=hours_in_day(day=day),
        jobs=jobs,
        schedule=ScheduleDay(
            checks_expected=1,
            checks_seen=len(checks),
            changed=any(check['changed'] for check in checks),
        ),
    )


def day_globs(
    *,
    client: Any,
    bucket: str,
    prefix: str,
    day: date,
) -> list[str]:
    """List globs for the UTC days around one Sydney day.

    Three UTC dates are named because a run is filed under the date it
    started, and the compactor reads an hour more than an hour after
    it ends, so a Sydney day's last hours are recorded on the next UTC
    day. Only days with at least one object are returned, since DuckDB
    raises on a glob-list entry matching no files.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.
    prefix : str
        Prefix of the record type to read.
    day : date
        Sydney calendar date being summarised.

    Returns
    -------
    list[str]
        One ``s3://`` glob per UTC day that exists.
    """
    days = (day - timedelta(days=1), day, day + timedelta(days=1))
    prefixes = (f'{prefix}dt={one:%Y-%m-%d}/' for one in days)
    return [
        f's3://{bucket}/{one}*.jsonl'
        for one in prefixes
        if client.list_objects_v2(
            Bucket=bucket, Prefix=one, MaxKeys=1,
        ).get('KeyCount')
    ]


def records_query(*, globs: list[str], columns: str) -> str:
    """Build a query reading whole audit rows from JSONL.

    Parameters
    ----------
    globs : list[str]
        One ``s3://`` glob per UTC day to read.
    columns : str
        DuckDB column declaration for the record type.

    Returns
    -------
    str
        A query selecting every column of every row.
    """
    source = ', '.join(f"'{glob}'" for glob in globs)
    return (
        f'SELECT * FROM read_json([{source}], columns = {columns})'
    )


# One read method by design: this is the repository pattern, and
# reading a day is the only thing a caller asks of it.
class CurationRepository:  # pylint: disable=too-few-public-methods
    """Reads curation and schedule-check audit rows.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to read through, already configured for S3.
    bucket : str
        Bucket holding the curated layer.
    session : boto3.Session | None
        Optional boto3 session, used to find which partitions exist.
        Defaults to a new session.
    """

    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection,
        bucket: str,
        session: boto3.Session | None = None,
    ) -> None:
        self._connection = connection
        self._bucket = bucket
        self._client = (session or boto3.Session()).client('s3')

    def fetch_day(self, *, day: date) -> CurationDay:
        """Summarise one Sydney day across every curation job.

        Parameters
        ----------
        day : date
            Sydney calendar date to read.

        Returns
        -------
        CurationDay
            Per-job attendance and health for the day.
        """
        records = self._records(
            prefix=CURATION_PREFIX, day=day, cols=CURATION_COLUMNS,
        )
        return summarize_curation(
            records, day=day, checks=self._checks(day=day),
        )

    def _records(
        self, *, prefix: str, day: date, cols: str,
    ) -> list[Any]:
        """Read every row of one record type around a Sydney day.

        Parameters
        ----------
        prefix : str
            Prefix of the record type to read.
        day : date
            Sydney calendar date being summarised.
        cols : str
            DuckDB column declaration for the record type.

        Returns
        -------
        list[Any]
            One dict per row, empty when no partition exists.
        """
        globs = day_globs(
            client=self._client, bucket=self._bucket,
            prefix=prefix, day=day,
        )
        if not globs:
            logger.warning(
                'No audit records found',
                extra={'prefix': prefix, 'day': f'{day:%Y-%m-%d}'},
            )
            return []
        result = self._connection.execute(
            records_query(globs=globs, columns=cols),
        )
        names = [column[0] for column in result.description or []]
        return [dict(zip(names, row)) for row in result.fetchall()]

    def _checks(self, *, day: date) -> list[dict[str, Any]]:
        """Read the schedule checks that fall on one Sydney day.

        Parameters
        ----------
        day : date
            Sydney calendar date to read.

        Returns
        -------
        list[dict[str, Any]]
            One dict per check made that Sydney day.
        """
        rows = self._records(
            prefix=CHECK_PREFIX, day=day, cols=CHECK_COLUMNS,
        )
        return [row for row in rows if _check_day(row=row) == day]


def _check_day(*, row: dict[str, Any]) -> date:
    """Find the Sydney date a schedule check was made on.

    Parameters
    ----------
    row : dict[str, Any]
        One schedule-check row.

    Returns
    -------
    date
        The Sydney calendar date of the check.
    """
    checked_at = datetime.fromisoformat(row['checked_at_utc'])
    return checked_at.astimezone(SYDNEY).date()
