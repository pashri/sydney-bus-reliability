"""DuckDB SQL for the service-day merge.

Expressed as SQL rather than Python because the reduction is naturally
declarative, and because a fixture-Parquet test of this SQL is the
cheapest guard against join fan-out - the classic way a reliability
number silently doubles.

``n_updates`` is **summed** across partials, never taken from the last
one. Additivity has to be a property of the data rather than of the
order files happen to be read in, which is why each partial stores a
per-hour count instead of a running total.
"""

from typing import Final

RELIABLE_LEAD_SECONDS: Final[int] = 60
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
``(service_date, trip_id, stop_id)``: loop and shuttle routes call the
same ``stop_id`` twice on one trip, at two different sequence
positions, and both real calls must survive the merge.

``RELIABLE_LEAD_SECONDS`` is interpolated into the interval literal
rather than hardcoded, so the reliability threshold has one source of
truth. ``n_updates`` and ``had_vehicle`` come from a separate
aggregate subquery joined back to the latest row by key, so the
aggregation can never fan out the row count of ``latest``.

``lost_tracking`` describes the *final* state of the day, not whether
any hour ever dropped: it compares the day's latest observation of any
kind (``MAX(last_observed_at_utc)``) against the day's latest real
observation (which ``latest.last_update_at_utc`` already is, since the
window function orders by it descending). A key with no real
observation at all - ``last_update_at_utc IS NULL`` - is never-reported
rather than lost, so it is not marked as lost tracking either.

The ``ROW_NUMBER()`` tiebreak is fully deterministic: ties on
``last_update_at_utc`` (typically both NULL, pre-departure echoes) are
broken by ``last_observed_at_utc``, which is set on every row
regardless of file read order, and then by ``schedule_relationship``
as a final, purely cosmetic tiebreak.
"""


def build_trip_stop_query(*, dim_source: str | None) -> str:
    """Wrap ``TRIP_STOP_MERGE`` with the scheduled-arrival join.

    Pure SQL end to end: nothing here fetches a ``TIMESTAMPTZ`` value
    into Python, which the merger's Lambda package cannot support (see
    ``TRIP_STOP_MERGE``'s own module docstring for the packaging
    constraint this works around).

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
    Reproduces ``src.common.service_day.scheduled_instant``'s
    wall-clock convention: the GTFS clock offset is added to local
    midnight and the result localised to Sydney, not treated as
    elapsed seconds from a fixed anchor. This is deliberate and
    matches the printed timetable across the 4 October 2026 DST
    transition; do not switch conventions. Verified against
    ``scheduled_instant`` on both sides of the jump and past hour 24,
    up to the measured maximum of hour 30.
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
526 samples per peak hour share a timestamp while reporting a
different location, and deduping on the pair alone would discard real
movement.
"""
