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

TRIP_STOP_MERGE: Final[str] = f"""
WITH partials AS (
    SELECT * EXCLUDE (dt, hour)
    FROM read_parquet($partials, hive_partitioning = true)
    WHERE dt >= $dt_from
      AND dt <= $dt_to
      AND service_date = $service_date
),
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
        latest.trip_schedule_relationship,
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
        {TRIP_STOP_MERGE}
    )
    SELECT day_merge.* REPLACE (
        strptime(day_merge.service_date, '%Y%m%d')::DATE AS service_date
    ), {arrival} AS scheduled_arrival_utc
    FROM day_merge
    {join}
    ORDER BY day_merge.route_id, day_merge.trip_id, day_merge.stop_sequence
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
