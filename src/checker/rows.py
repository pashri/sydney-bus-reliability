"""Fetching collector audit rows for one Sydney day."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any, Final

import boto3
import duckdb
from aws_lambda_powertools import Logger

from common.collector_run import collector_run_query, source_day_globs
from common.types_ import RunRecord

logger = Logger()

FACT_PREFIX: Final[str] = 'curated/fact_collector_run/'


def as_text(*, query: str) -> str:
    """Re-project a query so its timestamps come back as text.

    The shared query types its timestamps for the merger, which writes
    them to Parquet and never reads them into Python. Fetching one
    here instead would make DuckDB import ``pytz``, which no function
    packages.

    Parameters
    ----------
    query : str
        A query selecting audit-row columns in order.

    Returns
    -------
    str
        The same rows, with every timestamp rendered as text.
    """
    return f"""
    SELECT
        feed,
        CAST(fetched_at_utc AS VARCHAR) AS fetched_at_utc,
        CAST(received_at_utc AS VARCHAR) AS received_at_utc,
        rtt_s,
        CAST(server_date_utc AS VARCHAR) AS server_date_utc,
        skew_s, status_code, body_bytes, error
    FROM ({query})
    """


class RowSource(StrEnum):
    """Which store a day's rows were read from."""

    MERGED = 'merged'
    LIVE = 'live'


@dataclass(frozen=True, slots=True)
class DayRows:
    """One day's audit rows and where they came from.

    Attributes
    ----------
    rows : list[RunRecord]
        Every audit row whose fetch time falls in the Sydney day.
    source : RowSource
        ``merged`` if read from the fact table, ``live`` if read from
        the collector's JSONL.
    """

    rows: list[RunRecord]
    source: RowSource


def to_utc_iso(value: str | None) -> str | None:
    """Normalise a rendered timestamp to a UTC ISO 8601 string.

    Timestamps arrive as text, never as a ``TIMESTAMPTZ``: fetching
    one of those into Python makes DuckDB import ``pytz``, which is a
    development dependency and is not packaged into any function.
    DuckDB renders with a space separator and a two-digit offset, and
    the collector writes a different spelling of the same instant, so
    both are parsed and rendered again to one form.

    Parameters
    ----------
    value : str | None
        Timestamp as rendered by DuckDB or written by the collector.

    Returns
    -------
    str | None
        The timestamp in UTC, or None if `value` is None.
    """
    if not value:
        return None
    return datetime.fromisoformat(value).astimezone(UTC).isoformat()


def run_record(row: tuple[Any, ...]) -> RunRecord:
    """Rebuild one audit row from a DuckDB result row.

    Parameters
    ----------
    row : tuple[Any, ...]
        One result row, in the column order both the JSONL query and
        the fact table select.

    Returns
    -------
    RunRecord
        The row in the shape the collector originally wrote.
    """
    feed, fetched, received, rtt, server, skew, status, size, err = row
    return RunRecord(
        feed=feed,
        fetched_at_utc=str(to_utc_iso(fetched)),
        received_at_utc=str(to_utc_iso(received)),
        rtt_s=rtt,
        server_date_utc=to_utc_iso(server),
        skew_s=skew,
        status_code=status,
        body_bytes=size,
        error=err,
    )


def fact_day_query(*, bucket: str, day: date) -> str:
    """Build the query reading one merged day from the fact table.

    Parameters
    ----------
    bucket : str
        Bucket holding the curated layer.
    day : date
        Sydney calendar date to read.

    Returns
    -------
    str
        A query selecting that day's rows in audit-row column order.
    """
    target = (
        f's3://{bucket}/{FACT_PREFIX}'
        f'collection_date={day:%Y-%m-%d}/data.parquet'
    )
    return f"""
    SELECT
        feed,
        CAST(fetched_at_utc AS VARCHAR) AS fetched_at_utc,
        CAST(received_at_utc AS VARCHAR) AS received_at_utc,
        rtt_s,
        CAST(server_date_utc AS VARCHAR) AS server_date_utc,
        skew_s, status_code, body_bytes, error
    FROM read_parquet('{target}')
    ORDER BY fetched_at_utc, feed
    """


# One read method by design: this is the repository pattern, and
# reading a day is the only thing a caller asks of it.
class CollectorRunRepository:  # pylint: disable=too-few-public-methods
    """Reads collector audit rows for a Sydney day.

    A merged day is read from ``fact_collector_run`` as one Parquet
    file. A day the merger has not reached yet is read from the
    collector's JSONL, which is cut to the same Sydney bounds, so both
    sources yield the same rows for the same day.

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

    def fetch_day(self, *, day: date) -> DayRows:
        """Read one Sydney day's audit rows.

        Parameters
        ----------
        day : date
            Sydney calendar date to read.

        Returns
        -------
        DayRows
            The day's rows and which store they came from.
        """
        if self._merged_exists(day=day):
            query = fact_day_query(bucket=self._bucket, day=day)
            return DayRows(
                rows=self._rows(query=query), source=RowSource.MERGED,
            )
        return DayRows(
            rows=self._live_rows(day=day), source=RowSource.LIVE,
        )

    def _merged_exists(self, *, day: date) -> bool:
        """Check whether the merger has written this day yet.

        Parameters
        ----------
        day : date
            Sydney calendar date to look for.

        Returns
        -------
        bool
            True if the day's Parquet file is present.
        """
        prefix = f'{FACT_PREFIX}collection_date={day:%Y-%m-%d}/'
        listing = self._client.list_objects_v2(
            Bucket=self._bucket, Prefix=prefix, MaxKeys=1,
        )
        return bool(listing.get('KeyCount'))

    def _live_rows(self, *, day: date) -> list[RunRecord]:
        """Read one Sydney day straight from the collector's JSONL.

        Parameters
        ----------
        day : date
            Sydney calendar date to read.

        Returns
        -------
        list[RunRecord]
            The day's rows, empty if neither UTC partition exists.
        """
        globs = source_day_globs(
            client=self._client, bucket=self._bucket, service_date=day,
        )
        if not globs:
            logger.warning(
                'No collector audit records for this day',
                extra={'day': f'{day:%Y-%m-%d}'},
            )
            return []
        return self._rows(
            query=as_text(query=collector_run_query(globs=globs)),
            parameters={'day': f'{day:%Y-%m-%d}'},
        )

    def _rows(
        self,
        *,
        query: str,
        parameters: dict[str, str] | None = None,
    ) -> list[RunRecord]:
        """Run a query and rebuild its rows as audit records.

        Parameters
        ----------
        query : str
            A query selecting audit-row columns in order.
        parameters : dict[str, str] | None
            Named parameters for the query, if it takes any.

        Returns
        -------
        list[RunRecord]
            One record per result row.
        """
        result = self._connection.execute(query, parameters or {})
        return [run_record(row) for row in result.fetchall()]
