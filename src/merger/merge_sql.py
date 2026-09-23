"""DuckDB SQL for the service-day merge.

``n_updates`` is summed across partials, never taken from the last
one. Each partial stores a count for its own hour, not a running
total, so the sum does not depend on the order files are read in.
"""

from typing import Final

RELIABLE_LEAD_SECONDS: Final[int] = 60  # seconds
"""How stale a final prediction may be and still count as an arrival.

A prediction that stopped updating more than a minute before the bus
was due is a forecast, not an observation.
"""

EARLIEST_PLAUSIBLE_ARRIVAL: Final[str] = '2000-01-01 00:00:00+00'
"""Arrivals before this instant are epoch artefacts, never observations."""

FIRST_TIMETABLED_HOUR: Final[str] = """
CAST(split_part(
    arg_min(COALESCE(departure_time, arrival_time), stop_sequence),
    ':', 1
) AS INTEGER)
"""
"""A trip's first timetabled hour, which exceeds 23 after midnight."""


def service_day_partials(*, dim_source: str | None, date_column: str) -> str:
    """Build the CTEs selecting one service day's partial rows.

    The feed does not always send the service date. A trip whose first
    timetabled time is 24:00 or later arrives with its start time
    wrapped below 24:00 and ``start_date`` set to the next calendar
    date. Nothing in the trip update itself marks these trips, so the
    timetable decides: such a trip belongs to the day before its
    ``start_date``. A trip that starts before midnight and runs past
    it already carries its service date. A trip missing from the
    timetable keeps its ``start_date``.

    Parameters
    ----------
    dim_source : str | None
        Path to the timetable snapshot, or None to take every
        ``start_date`` as the service date.
    date_column : str
        The partial column holding the feed's start date.

    Returns
    -------
    str
        CTE definitions, without the leading ``WITH``, ending in
        ``partials``. It holds every partial column except ``dt`` and
        ``hour``, with the feed's date as ``start_date`` and the
        derived ``service_date`` as ``YYYYMMDD``, and only rows for
        ``$service_date``.
    """
    service_date = f'partial.{date_column}'
    first_times = ''
    join = ''
    if dim_source is not None:
        first_times = f"""
        first_times AS (
            SELECT trip_id, {FIRST_TIMETABLED_HOUR} >= 24 AS after_midnight
            FROM read_parquet('{dim_source}')
            GROUP BY trip_id
        ),"""
        join = 'LEFT JOIN first_times USING (trip_id)'
        service_date = f"""CASE WHEN first_times.after_midnight THEN strftime(
            strptime(partial.{date_column}, '%Y%m%d') - INTERVAL 1 DAY,
            '%Y%m%d'
        ) ELSE partial.{date_column} END"""
    return f"""{first_times}
    partials AS (
        SELECT * FROM (
            SELECT
                partial.* EXCLUDE (dt, hour, {date_column}),
                partial.{date_column} AS start_date,
                {service_date} AS service_date
            FROM read_parquet(
                $partials, hive_partitioning = true, union_by_name = true
            ) AS partial
            {join}
            WHERE partial.dt >= $dt_from
              AND partial.dt <= $dt_to
              AND partial.{date_column} IN ($service_date, strftime(
                  strptime($service_date, '%Y%m%d') + INTERVAL 1 DAY,
                  '%Y%m%d'
              ))
        )
        WHERE service_date = $service_date
    )"""


def trip_stop_merge(*, dim_source: str | None) -> str:
    """Build the merge of one service day's trip-stop partials.

    Parameters
    ----------
    dim_source : str | None
        Timetable snapshot deciding each trip's service day, or None.

    Returns
    -------
    str
        A query yielding one row per service-day stop.
    """
    partials = service_day_partials(
        dim_source=dim_source, date_column='service_date',
    )
    return f"""
WITH {partials},
latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY service_date, trip_id, stop_id, stop_sequence
            ORDER BY
                last_update_at_utc DESC NULLS LAST,
                last_observed_at_utc DESC NULLS LAST,
                schedule_relationship DESC NULLS LAST
        ) AS recency
    FROM partials
),
totals AS (
    SELECT
        service_date,
        trip_id,
        stop_id,
        stop_sequence,
        CAST(SUM(n_updates) AS INTEGER) AS n_updates,
        BOOL_OR(had_vehicle) AS had_vehicle,
        MAX(last_observed_at_utc) AS final_last_observed_at_utc
    FROM partials
    GROUP BY service_date, trip_id, stop_id, stop_sequence
),
merged AS (
    SELECT
        latest.service_date,
        latest.trip_id,
        latest.stop_id,
        latest.stop_sequence,
        latest.route_id,
        latest.final_predicted_arrival_utc,
        latest.delay_s,
        latest.final_predicted_departure_utc,
        latest.departure_delay_s,
        latest.last_update_at_utc,
        totals.n_updates,
        latest.schedule_relationship,
        totals.had_vehicle,
        (
            latest.last_update_at_utc IS NOT NULL
            AND totals.final_last_observed_at_utc
                > latest.last_update_at_utc
        ) AS lost_tracking
    FROM latest
    JOIN totals
      ON  totals.service_date  = latest.service_date
      AND totals.trip_id       = latest.trip_id
      AND totals.stop_id       = latest.stop_id
      AND totals.stop_sequence = latest.stop_sequence
    WHERE latest.recency = 1
)
SELECT
    merged.*,
    COALESCE(
        merged.delay_s IS NOT NULL
        AND NOT merged.lost_tracking
        AND merged.final_predicted_arrival_utc
            >= TIMESTAMPTZ '{EARLIEST_PLAUSIBLE_ARRIVAL}'
        AND merged.last_update_at_utc >= (
            merged.final_predicted_arrival_utc
            - INTERVAL '{RELIABLE_LEAD_SECONDS}' SECOND
        ),
        false
    ) AS is_reliable
FROM merged
"""


TRIP_STOP_MERGE: Final[str] = trip_stop_merge(dim_source=None)

"""Merge hourly trip-stop partials into one row per service-day stop.

The grain is ``(service_date, trip_id, stop_id, stop_sequence)``, not
``(service_date, trip_id, stop_id)``. Loop and shuttle routes call the
same ``stop_id`` twice on one trip at two different sequence
positions, and both calls must survive the merge.

``n_updates`` and ``had_vehicle`` come from a separate aggregate
subquery joined back to the latest row by key, so the aggregation
cannot fan out the row count of ``latest``.

``lost_tracking`` describes the final state of the day, not whether
any hour ever dropped. It compares the day's latest observation of any
kind (``MAX(last_observed_at_utc)``) against its latest real
observation, which ``latest.last_update_at_utc`` already is, since the
window function orders by it descending. A key with no real
observation at all was never reported rather than lost, so it is not
flagged.

The ``dt`` bounds prune whole partial objects before their Parquet
footers are read. They do not select the service day, which
``service_date`` still does; they only stop the glob widening with
every day ever collected. ``dt`` and ``hour`` come from the partition
path rather than the data, so they are excluded to keep the merged
columns identical to the partial's own.

Partials are read by column name, so an hour written before a column
was added or retired still merges with a later one. Only the columns
listed in ``merged`` reach the fact.

``is_reliable`` is never NULL. A row can carry a delay with no
predicted arrival time, and comparing against a missing time yields NULL
rather than false, so the comparison is wrapped in ``COALESCE``. Nothing
reading the column has to tell "not reliable" apart from "unknown". An
arrival before ``EARLIEST_PLAUSIBLE_ARRIVAL`` is never reliable, since
every update time would pass the lead test against it.

``n_updates`` is cast back to ``INTEGER``. ``SUM`` widens it to a type
Parquet has no slot for, which lands in the file as a floating-point
number unless it is narrowed again.

The ``ROW_NUMBER()`` tiebreak is deterministic. Ties on
``last_update_at_utc``, usually both NULL for pre-departure echoes,
break on ``last_observed_at_utc``, which is set on every row
regardless of file read order, then on ``schedule_relationship``.
"""


def build_trip_stop_query(*, dim_source: str | None) -> str:
    """Wrap ``TRIP_STOP_MERGE`` with the scheduled-arrival join.

    Pure SQL end to end. Nothing here fetches a ``TIMESTAMPTZ`` value
    into Python, which the merger's Lambda package cannot support.

    Parameters
    ----------
    dim_source : str | None
        S3 path to the schedule snapshot in effect for the service
        date, or None when no snapshot exists at or before it.

    Returns
    -------
    str
        A query selecting every ``TRIP_STOP_MERGE`` column plus
        ``scheduled_arrival_utc``, NULL for a row with no matching
        schedule row. ``service_date`` is converted to a date, so it
        matches the ``service_date=`` partition it is written under
        rather than shadowing it with a different type.

    Notes
    -----
    Rows are sorted by route, then trip, then stop. Parquet keeps
    per-row-group minimum and maximum values, so a reader filtering on
    a route can skip most of a day's groups instead of opening all of
    them.

    Reproduces the wall-clock convention of
    ``common.service_day.scheduled_instant``. The GTFS clock
    offset is added to local midnight and the result localised to
    Sydney, not treated as elapsed seconds from a fixed anchor. The
    two conventions differ across a daylight-saving transition, so
    changing this one silently shifts scheduled times by an hour.
    """
    if dim_source is None:
        join = ''
        arrival = 'CAST(NULL AS TIMESTAMPTZ)'
    else:
        join = f"""
        LEFT JOIN read_parquet('{dim_source}') AS schedule
          ON  schedule.trip_id = day_merge.trip_id
          AND schedule.stop_sequence = day_merge.stop_sequence
        """
        arrival = """
        CASE WHEN schedule.arrival_time IS NULL THEN NULL ELSE (
            strptime(day_merge.service_date, '%Y%m%d')
            + INTERVAL (
                CAST(split_part(schedule.arrival_time, ':', 1) AS BIGINT)
            ) HOUR
            + INTERVAL (
                CAST(split_part(schedule.arrival_time, ':', 2) AS BIGINT)
            ) MINUTE
            + INTERVAL (
                CAST(split_part(schedule.arrival_time, ':', 3) AS BIGINT)
            ) SECOND
        ) AT TIME ZONE 'Australia/Sydney' END
        """
    return f"""
    WITH day_merge AS (
        {trip_stop_merge(dim_source=dim_source)}
    )
    SELECT day_merge.* REPLACE (
        strptime(day_merge.service_date, '%Y%m%d')::DATE AS service_date
    ), {arrival} AS scheduled_arrival_utc
    FROM day_merge
    {join}
    ORDER BY day_merge.route_id, day_merge.trip_id, day_merge.stop_sequence
    """


def trip_merge(*, dim_source: str | None) -> str:
    """Build the merge of one service day's trip-status partials.

    Each partial covers the polls fetched in its own hour, so no poll
    is counted twice: counts are summed, spans widened, and the final
    status taken from the hour holding the latest status poll.
    ``arg_max`` skips NULL arguments, so an hour whose trip carried no
    status never overrides one that did.

    Parameters
    ----------
    dim_source : str | None
        Timetable snapshot deciding each trip's service day, or None.

    Returns
    -------
    str
        A query yielding one row per service-day trip, keeping the
        feed's own ``start_date`` beside the service date.
    """
    partials = service_day_partials(
        dim_source=dim_source, date_column='start_date',
    )
    return f"""
WITH {partials}
SELECT
    service_date,
    trip_id,
    start_date,
    arg_max(start_time, last_seen_at_utc) AS start_time,
    arg_max(route_id, last_seen_at_utc) AS route_id,
    arg_max(final_status, final_status_at_utc) AS final_status,
    MAX(final_status_at_utc) AS final_status_at_utc,
    CAST(SUM(scheduled_polls) AS INTEGER) AS scheduled_polls,
    CAST(SUM(canceled_polls) AS INTEGER) AS canceled_polls,
    CAST(SUM(added_polls) AS INTEGER) AS added_polls,
    MIN(first_seen_at_utc) AS first_seen_at_utc,
    MAX(last_seen_at_utc) AS last_seen_at_utc,
    MIN(first_canceled_at_utc) AS first_canceled_at_utc,
    MAX(last_canceled_at_utc) AS last_canceled_at_utc,
    BOOL_OR(had_vehicle) AS had_vehicle
FROM partials
GROUP BY service_date, trip_id, start_date
"""


def build_trip_query(*, dim_source: str | None) -> str:
    """Build the query assembling ``fact_trip`` for one service day.

    Parameters
    ----------
    dim_source : str | None
        Timetable snapshot deciding each trip's service day, or None.

    Returns
    -------
    str
        ``TRIP_MERGE`` with ``service_date`` converted to a date, so
        it matches the ``service_date=`` partition it is written under,
        sorted by route and trip.
    """
    return f"""
    WITH day_merge AS (
        {trip_merge(dim_source=dim_source)}
    )
    SELECT day_merge.* REPLACE (
        strptime(day_merge.service_date, '%Y%m%d')::DATE AS service_date
    )
    FROM day_merge
    ORDER BY day_merge.route_id, day_merge.trip_id
    """


POSITION_MERGE: Final[str] = """
SELECT * FROM (
    SELECT DISTINCT ON (vehicle_id, observed_at_utc, lat, lon)
        * EXCLUDE (dt, hour)
    FROM read_parquet($partials, hive_partitioning = true)
    WHERE dt >= $dt_from
      AND dt <= $dt_to
      AND observed_at_utc >= $window_start
      AND observed_at_utc <  $window_end
    ORDER BY vehicle_id, observed_at_utc, lat, lon, fetched_at_utc
)
ORDER BY route_id, observed_at_utc
"""
"""Deduplicate positions again at the day level.

A stale position can be restated across an hour boundary, so the same
(vehicle, timestamp, position) tuple may appear in two partials. The
key includes position for the same reason it does in the compactor:
hundreds of samples per peak hour share a timestamp while reporting a
different location, and deduping on the pair alone would discard real
movement.

The ``dt`` bounds prune partial objects by partition path, before any
footer is read. ``observed_at_utc`` still decides which rows belong to
the day.

The inner ordering belongs to ``DISTINCT ON``, which uses it to pick
which duplicate survives, so the output ordering is applied outside it.
Rows are written sorted by route and time, which lets a reader skip
row groups rather than opening every one.
"""
