"""Reading the collector's per-invocation audit JSONL."""

from datetime import date, timedelta
from typing import Any, Final

from common.service_day import SYDNEY

COLLECTOR_RUN_PREFIX: Final[str] = 'curated/collector_run/'

COLLECTOR_RUN_COLUMNS: Final[str] = (
    "{feed: 'VARCHAR', fetched_at_utc: 'VARCHAR',"
    " received_at_utc: 'VARCHAR', rtt_s: 'DOUBLE',"
    " server_date_utc: 'VARCHAR', skew_s: 'DOUBLE',"
    " status_code: 'INTEGER', body_bytes: 'BIGINT', error: 'VARCHAR'}"
)
"""Every ``RunRecord`` field, typed for DuckDB's JSON reader.

Declared rather than inferred. Inference reads the same column as a
timestamp on one day and a string on the next, depending on whether any
value carries fractional seconds and whether the whole column is null,
which makes both the stored type and the day filter unstable.

Timestamps are read as text and cast once, so a filter never depends on
what the reader guessed.
"""


def source_day_globs(
    *,
    client: Any,
    bucket: str,
    service_date: date,
) -> list[str]:
    """List globs for the UTC days one Sydney service day can touch.

    Only days with at least one object are returned. DuckDB raises on a
    glob-list entry matching no files, which the earliest service date
    would otherwise hit, having no preceding UTC partition.

    Naming the two days beats globbing ``dt=*`` and filtering. Both read
    the same rows, but the wildcard makes S3 list every day ever
    collected first, so the read would slow down for the life of the
    project rather than staying flat.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The Sydney service date being assembled.

    Returns
    -------
    list[str]
        One ``s3://`` glob per UTC day that exists.
    """
    days = (service_date - timedelta(days=1), service_date)
    prefixes = (
        f'{COLLECTOR_RUN_PREFIX}dt={day:%Y-%m-%d}/' for day in days
    )
    return [
        f's3://{bucket}/{prefix}*.jsonl'
        for prefix in prefixes
        if client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, MaxKeys=1,
        ).get('KeyCount')
    ]


def collector_run_query(*, globs: list[str]) -> str:
    """Build the query cutting one Sydney day out of the audit JSONL.

    Parameters
    ----------
    globs : list[str]
        One ``s3://`` glob per UTC day to read.

    Returns
    -------
    str
        A query selecting one Sydney day's rows, with each timestamp
        parsed to an instant.
    """
    source = ', '.join(f"'{glob}'" for glob in globs)
    return f"""
    SELECT
        feed,
        CAST(fetched_at_utc AS TIMESTAMPTZ) AS fetched_at_utc,
        CAST(received_at_utc AS TIMESTAMPTZ) AS received_at_utc,
        rtt_s,
        CAST(server_date_utc AS TIMESTAMPTZ) AS server_date_utc,
        skew_s,
        status_code,
        body_bytes,
        error
    FROM read_json(
        [{source}],
        columns = {COLLECTOR_RUN_COLUMNS}
    )
    WHERE CAST(fetched_at_utc AS TIMESTAMPTZ) >= (
          CAST($day AS TIMESTAMP) AT TIME ZONE '{SYDNEY.key}'
      )
      AND CAST(fetched_at_utc AS TIMESTAMPTZ) < (
          CAST($day AS TIMESTAMP) + INTERVAL 1 DAY
      ) AT TIME ZONE '{SYDNEY.key}'
    ORDER BY fetched_at_utc, feed
    """
