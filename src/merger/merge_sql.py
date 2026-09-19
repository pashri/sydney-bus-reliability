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
            PARTITION BY service_date, trip_id, stop_id
            ORDER BY last_update_at_utc DESC NULLS LAST
        ) AS recency
    FROM partials
)
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
    totals.lost_tracking,
    (
        latest.delay_s IS NOT NULL
        AND NOT totals.lost_tracking
        AND latest.last_update_at_utc >= (
            latest.final_predicted_arrival_utc
            - INTERVAL '{RELIABLE_LEAD_SECONDS}' SECOND
        )
    ) AS is_reliable
FROM latest
JOIN (
    SELECT
        service_date,
        trip_id,
        stop_id,
        SUM(n_updates) AS n_updates,
        BOOL_OR(had_vehicle) AS had_vehicle,
        BOOL_OR(lost_tracking) AS lost_tracking
    FROM partials
    GROUP BY service_date, trip_id, stop_id
) AS totals
  ON  totals.service_date = latest.service_date
  AND totals.trip_id      = latest.trip_id
  AND totals.stop_id      = latest.stop_id
WHERE latest.recency = 1
"""

"""Merge hourly trip-stop partials into one row per service-day stop.

``RELIABLE_LEAD_SECONDS`` is interpolated into the interval literal
rather than hardcoded, so the reliability threshold has one source of
truth. ``n_updates``, ``had_vehicle`` and ``lost_tracking`` come from a
separate aggregate subquery joined back to the latest row by key, so
the aggregation can never fan out the row count of ``latest``.
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
