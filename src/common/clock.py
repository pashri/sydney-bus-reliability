"""Reconciliation between the feed server's clock and ours.

Every delay in this project is a difference between two timestamps. If
one of them comes from a Lambda whose clock has drifted, the delay is
wrong and nothing downstream can detect it, so responses are always aged
against the server's own ``Date`` header.
"""

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime


def parse_server_date(*, header_value: str) -> datetime:
    """Parse an HTTP ``Date`` header into a UTC datetime.

    Parameters
    ----------
    header_value : str
        Raw header value in RFC 7231 format.

    Returns
    -------
    datetime
        Timezone-aware datetime in UTC.

    Raises
    ------
    ValueError
        If the header cannot be parsed as a date.
    """
    parsed = parsedate_to_datetime(header_value)
    return parsed.astimezone(UTC)


def round_trip_seconds(
    *,
    sent_at: datetime,
    received_at: datetime,
) -> float:
    """Measure how long a request took, start to finish.

    Parameters
    ----------
    sent_at : datetime
        Local time immediately before the request was issued.
    received_at : datetime
        Local time immediately after the response arrived.

    Returns
    -------
    float
        Round-trip time in seconds.
    """
    return (received_at - sent_at).total_seconds()


def skew_seconds(
    *,
    server_time: datetime,
    sent_at: datetime,
    received_at: datetime,
) -> float:
    """Estimate clock skew, corrected for network latency.

    The server stamps its ``Date`` somewhere between the request
    leaving and the response arriving. Comparing it to the midpoint
    of those two local timestamps removes roughly one leg of the
    round trip, so what is left is clock difference rather than
    distance to Sydney.

    Parameters
    ----------
    server_time : datetime
        Timestamp from the server's ``Date`` header.
    sent_at : datetime
        Local time immediately before the request was issued.
    received_at : datetime
        Local time immediately after the response arrived.

    Returns
    -------
    float
        Seconds of skew, positive when the local clock is ahead.
    """
    midpoint = sent_at + (received_at - sent_at) / 2
    return (midpoint - server_time).total_seconds()
