"""On-demand health check over the curated layer.

Reads only. Summarises the collector's polling and the curation
Lambdas' runs for a range of Sydney days, and returns the figures as
JSON for a caller to render or eyeball. It forms no opinion about
whether a figure is good.
"""

import os
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, TypedDict, cast

import boto3
import duckdb
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from checker.collection import summarize_day
from checker.curation import CurationRepository
from checker.rows import CollectorRunRepository
from common.connection import DuckDbLimits, configure
from common.service_day import SYDNEY

logger = Logger()

SCHEMA_VERSION: Final[int] = 1
"""Version of the response shape.

The renderer is deployed separately from this function, so the two
drift. A reader can refuse a shape it does not know.
"""

DEFAULT_COLLECTION_DAYS: Final[int] = 1  # days
DEFAULT_CURATION_DAYS: Final[int] = 14  # days

DUCKDB_LIMITS: Final[DuckDbLimits] = DuckDbLimits(
    threads=2, memory_limit='700MB',
)
"""Room for the check.

Counting a day of audit rows needs far less than assembling one, and
the ceiling sits below the function's own so DuckDB spills to disk
before the runtime kills it.
"""


class Requested(TypedDict):
    """The window a response covers."""

    date: str
    collection_days: int
    curation_days: int


class CheckerResponse(TypedDict):
    """One health check's findings."""

    schema_version: int
    requested: Requested
    collection: dict[str, Any]
    curation: dict[str, Any]


def target_day(*, event: dict[str, Any], now: datetime) -> date:
    """Choose the last Sydney day a check covers.

    Parameters
    ----------
    event : dict[str, Any]
        Invocation event, optionally carrying a ``date`` override.
    now : datetime
        Current UTC time.

    Returns
    -------
    date
        The Sydney calendar date to summarise up to.
    """
    override = event.get('date')
    if override:
        return date.fromisoformat(override)
    return now.astimezone(SYDNEY).date() - timedelta(days=1)


def day_range(*, last: date, count: int) -> list[date]:
    """List the days a window covers, oldest first.

    Parameters
    ----------
    last : date
        Final day of the window.
    count : int
        Days the window spans, including `last`.

    Returns
    -------
    list[date]
        One date per day, in order.
    """
    return [
        last - timedelta(days=offset)
        for offset in reversed(range(max(count, 1)))
    ]


def jsonable(value: Any) -> Any:
    """Render a value as JSON-safe types.

    Parameters
    ----------
    value : Any
        A value from a summary dataclass.

    Returns
    -------
    Any
        The value with every date and datetime rendered as a string.
    """
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    return value


def collection_day(
    *,
    repository: CollectorRunRepository,
    day: date,
) -> dict[str, Any]:
    """Summarise one Sydney day of the collector's polling.

    No boundary rows are stitched in. The day is cut on each poll's
    fetch time across both source partitions, so a poll either falls
    in the day or does not; nothing is lost at the seam.

    Parameters
    ----------
    repository : CollectorRunRepository
        Source of the day's audit rows.
    day : date
        Sydney calendar date to summarise.

    Returns
    -------
    dict[str, Any]
        The day's summary, JSON-safe.
    """
    fetched = repository.fetch_day(day=day)
    summary = summarize_day(fetched.rows)
    return cast(dict[str, Any], jsonable({
        'date': day,
        'source': fetched.source.value,
        **asdict(summary),
    }))


def curation_day(
    *,
    repository: CurationRepository,
    day: date,
) -> dict[str, Any]:
    """Summarise one Sydney day of the curation Lambdas' runs.

    Parameters
    ----------
    repository : CurationRepository
        Source of the day's audit rows.
    day : date
        Sydney calendar date to summarise.

    Returns
    -------
    dict[str, Any]
        The day's summary, JSON-safe.
    """
    return cast(
        dict[str, Any], jsonable(asdict(repository.fetch_day(day=day))),
    )


def collection_window(
    *,
    repository: CollectorRunRepository,
    last: date,
    count: int,
) -> list[dict[str, Any]]:
    """Summarise each Sydney day of collection in a window.

    Parameters
    ----------
    repository : CollectorRunRepository
        Source of the audit rows.
    last : date
        Final day of the window.
    count : int
        Days the window spans.

    Returns
    -------
    list[dict[str, Any]]
        One summary per day, oldest first.
    """
    return [
        collection_day(repository=repository, day=day)
        for day in day_range(last=last, count=count)
    ]


def curation_window(
    *,
    repository: CurationRepository,
    last: date,
    count: int,
) -> list[dict[str, Any]]:
    """Summarise each Sydney day of curation in a window.

    Parameters
    ----------
    repository : CurationRepository
        Source of the audit rows.
    last : date
        Final day of the window.
    count : int
        Days the window spans.

    Returns
    -------
    list[dict[str, Any]]
        One summary per day, oldest first.
    """
    return [
        curation_day(repository=repository, day=day)
        for day in day_range(last=last, count=count)
    ]


def requested_window(*, event: dict[str, Any], day: date) -> Requested:
    """Read the window a caller asked for.

    Parameters
    ----------
    event : dict[str, Any]
        Invocation event, optionally widening either half.
    day : date
        Final Sydney day of the window.

    Returns
    -------
    Requested
        The window, with defaults filled in.
    """
    return Requested(
        date=day.isoformat(),
        collection_days=int(
            event.get('collection_days', DEFAULT_COLLECTION_DAYS),
        ),
        curation_days=int(
            event.get('curation_days', DEFAULT_CURATION_DAYS),
        ),
    )


def handler(  # pylint: disable=unused-argument
    event: dict[str, Any],
    context: LambdaContext,
    endpoint: str | None = None,
) -> CheckerResponse:
    """Summarise the pipeline's health over a window of Sydney days.

    Parameters
    ----------
    event : dict[str, Any]
        Invocation event, optionally carrying a ``date`` to end the
        window on and ``collection_days`` and ``curation_days`` to
        widen either half. Defaults summarise yesterday's collection
        and a fortnight of curation.
    context : LambdaContext
        Lambda context. Unused; the check writes nothing.
    endpoint : str | None
        Test-only S3 endpoint override, forwarded to ``configure``.
        Lambda invokes the handler with two positional arguments, so
        this is always None in production.

    Returns
    -------
    CheckerResponse
        Per-day figures for both halves of the pipeline.
    """
    bucket = os.environ['BUCKET_NAME']
    day = target_day(event=event, now=datetime.now(tz=UTC))
    requested = requested_window(event=event, day=day)
    logger.info('Checking pipeline health', extra=dict(requested))
    connection = duckdb.connect()
    configure(
        connection=connection, limits=DUCKDB_LIMITS, endpoint=endpoint,
    )
    session = boto3.Session()
    return CheckerResponse(
        schema_version=SCHEMA_VERSION,
        requested=requested,
        collection={'days': collection_window(
            repository=CollectorRunRepository(
                connection=connection, bucket=bucket, session=session,
            ),
            last=day,
            count=requested['collection_days'],
        )},
        curation={'days': curation_window(
            repository=CurationRepository(
                connection=connection, bucket=bucket, session=session,
            ),
            last=day,
            count=requested['curation_days'],
        )},
    )
