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

TRIP_STOP_MERGE: Final[str] = f"""
WITH partials AS (
    SELECT * FROM read_parquet($partials)
    WHERE service_date = $service_date
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
        SUM(n_updates) AS n_updates,
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
    (
        merged.delay_s IS NOT NULL
        AND NOT merged.lost_tracking
        AND merged.last_update_at_utc >= (
            merged.final_predicted_arrival_utc
            - INTERVAL '{RELIABLE_LEAD_SECONDS}' SECOND
        )
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
        schedule row.

    Notes
    -----
    Reproduces the wall-clock convention of
    ``src.common.service_day.scheduled_instant``. The GTFS clock
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
    SELECT day_merge.*, {arrival} AS scheduled_arrival_utc
    FROM day_merge
    {join}
    """


POSITION_MERGE: Final[str] = """
SELECT DISTINCT ON (vehicle_id, observed_at_utc, lat, lon) *
FROM read_parquet($partials)
WHERE observed_at_utc >= $window_start
  AND observed_at_utc <  $window_end
ORDER BY vehicle_id, observed_at_utc, lat, lon, fetched_at_utc
"""
"""Deduplicate positions again at the day level.

A stale position can be restated across an hour boundary, so the same
(vehicle, timestamp, position) tuple may appear in two partials. The
key includes position for the same reason it does in the compactor:
hundreds of samples per peak hour share a timestamp while reporting a
different location, and deduping on the pair alone would discard real
movement.
"""
