"""Fetching TfNSW GTFS-Realtime feeds over HTTP."""

from datetime import UTC, datetime
from http import HTTPStatus
from typing import Final

import requests
from aws_lambda_powertools import Logger

from src.common.clock import parse_server_date
from src.common.types_ import Feed, FetchResult

logger = Logger()

BASE_URL: Final[str] = 'https://api.transport.nsw.gov.au/v1/gtfs'
FEED_PATHS: Final[dict[Feed, str]] = {
    Feed.VEHICLE_POSITIONS: 'vehiclepos/buses',
    Feed.TRIP_UPDATES: 'realtime/buses',
}
TIMEOUT: Final[float] = 8.0


def feed_url(*, feed: Feed) -> str:
    """Build the fully qualified URL for a feed.

    Parameters
    ----------
    feed
        The feed to address.

    Returns
    -------
    str
        Absolute URL.
    """
    return f'{BASE_URL}/{FEED_PATHS[feed]}'


def read_server_date(*, response: requests.Response) -> (
    datetime | None
):
    """Extract the server's ``Date`` header, tolerating absence.

    Parameters
    ----------
    response
        A completed HTTP response.

    Returns
    -------
    datetime | None
        UTC server time, or None if absent or unparseable.
    """
    header_value = response.headers.get('Date')
    if header_value is None:
        return None
    try:
        return parse_server_date(header_value=header_value)
    except ValueError:
        logger.warning(
            'Unparseable Date header',
            extra={'header_value': header_value},
        )
        return None


def fetch_feed(
    *,
    feed: Feed,
    api_key: str,
    session: requests.Session | None = None,
) -> FetchResult:
    """Fetch one feed, converting every failure into a result.

    Parameters
    ----------
    feed
        The feed to fetch.
    api_key
        TfNSW Open Data Hub API key.
    session
        Optional session to reuse a connection pool.

    Returns
    -------
    FetchResult
        Always returned; never raises. A non-200 status or a transport
        failure is recorded in ``error`` with an empty ``body``.
    """
    session = session or requests.Session()
    fetched_at = datetime.now(tz=UTC)
    try:
        response = session.get(
            feed_url(feed=feed),
            headers={'Authorization': f'apikey {api_key}'},
            timeout=TIMEOUT,
        )
    except requests.RequestException as error:
        logger.exception(
            'Feed fetch failed',
            extra={'feed': feed.value},
        )
        return FetchResult(
            feed=feed,
            fetched_at_utc=fetched_at,
            received_at_utc=datetime.now(tz=UTC),
            server_date_utc=None,
            status_code=None,
            body=b'',
            error=str(error),
        )
    return _result_from(
        feed=feed,
        fetched_at=fetched_at,
        received_at=datetime.now(tz=UTC),
        response=response,
    )


def _result_from(
    *,
    feed: Feed,
    fetched_at: datetime,
    received_at: datetime,
    response: requests.Response,
) -> FetchResult:
    """Convert an HTTP response into a FetchResult.

    Parameters
    ----------
    feed
        The feed that was fetched.
    fetched_at
        When the request was issued.
    received_at
        When the response finished arriving.
    response
        The completed response.

    Returns
    -------
    FetchResult
        With an empty body when the status is not 200.
    """
    ok = response.status_code == HTTPStatus.OK
    if not ok:
        logger.warning(
            'Feed returned non-200',
            extra={
                'feed': feed.value,
                'status': response.status_code,
            },
        )
    return FetchResult(
        feed=feed,
        fetched_at_utc=fetched_at,
        received_at_utc=received_at,
        server_date_utc=read_server_date(response=response),
        status_code=response.status_code,
        body=response.content if ok else b'',
        error=None if ok else f'HTTP {response.status_code}',
    )
