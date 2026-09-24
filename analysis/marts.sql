-- Analysis views over the curated layer.
--
-- Create these inputs first, then run this file against them.
-- analysis/marts.py does both over a local copy made by
-- scripts.pull_curated.
--
--   calendar_exclusion_seed        analysis/calendar_exclusions_<year>.csv
--   dim_route_all                  curated/dim_route/
--   dim_trip_all                   curated/dim_trip/
--   dim_stop_all                   curated/dim_stop/
--   dim_scheduled_stop_time_all    curated/dim_scheduled_stop_time/
--   dim_calendar_all               curated/dim_calendar/
--   dim_calendar_dates_all         curated/dim_calendar_dates/
--   fact_trip                      curated/fact_trip/
--   fact_trip_stop                 curated/fact_trip_stop/
--   stop_geography_all             reference/stop_geography/
--   stop_meshblock_all             reference/stop_meshblock/
--
-- Each dim_*_all input is every snapshot at once, read with hive
-- partitioning so it carries the snapshot's valid_from as text. The
-- ICU and spatial extensions must be loaded.
--
-- For example:
--
--   create view calendar_exclusion_seed as
--     select * from read_csv('analysis/calendar_exclusions_2026.csv');
--   create view dim_route_all as
--     select * from read_parquet(
--       'build/data/curated/dim_route/*/*.parquet',
--       hive_partitioning = true, union_by_name = true);

-- Seconds past the service day's midnight of a GTFS time, which can
-- exceed 24 hours: '25:10:00' is 90600.
create or replace macro gtfs_seconds(value) as
    split_part(value, ':', 1)::integer * 3600
    + split_part(value, ':', 2)::integer * 60
    + split_part(value, ':', 3)::integer;

-- The instant a GTFS time names on a service day.
--
-- The seconds are added to the Sydney wall clock at midnight, which is
-- how the merger computes scheduled_arrival_utc. Across a daylight
-- saving change the two readings of "midnight plus N hours" differ;
-- this one matches the facts.
create or replace macro sydney_instant(service_date, seconds) as
    timezone(
        'Australia/Sydney',
        service_date::timestamp + to_seconds(seconds)
    );

-- One row per excluded date, expanded from the seed's ranges.
--
-- A date can appear more than once: a public holiday inside the school
-- holidays carries a row of each kind. Join on date alone and that day
-- is counted twice.
create or replace view calendar_exclusion as
select
    unnest(generate_series(
        seed.start_date,
        seed.end_date,
        interval 1 day
    ))::date as day,
    seed.exclusion_type,
    seed.reason
from calendar_exclusion_seed as seed;

-- The weekdays peak comparisons are allowed to use.
--
-- Holiday traffic and term traffic are different things, so a peak
-- measured across both describes neither.
create or replace view term_weekday as
select series.day::date as day
from generate_series(
    (select min(day) from calendar_exclusion),
    (select max(day) from calendar_exclusion),
    interval 1 day
) as series(day)
where isodow(series.day::date) between 1 and 5
  and series.day::date not in (select day from calendar_exclusion);

-- The newest description of each route, trip and stop.
--
-- Identifiers are stable across snapshots, and a route or trip dropped
-- from a later bundle keeps its last description, so facts from before
-- the drop still join. Calendars are the exception: service_id is
-- renumbered in every bundle, so it is only ever joined within one
-- snapshot (see active_service), never through these views.
create or replace view dim_route as
select * exclude (valid_from)
from dim_route_all
qualify row_number() over (
    partition by route_id order by valid_from desc
) = 1;

create or replace view dim_trip as
select * exclude (valid_from)
from dim_trip_all
qualify row_number() over (
    partition by trip_id order by valid_from desc
) = 1;

create or replace view dim_stop as
select * exclude (valid_from)
from dim_stop_all
qualify row_number() over (
    partition by stop_id order by valid_from desc
) = 1;

-- Routes the operator declares as school services.
--
-- GTFS route type 712 is "school bus". It is the operator's own
-- statement and is more accurate than either route names or inferring
-- it from holiday cancellations: some school runs name the school
-- without the word "school", some all-year routes terminate at one,
-- and some school routes carry a few trips on all-year services.
--
-- School services run only on term weekdays and only at the shoulders
-- of the peaks, so leaving them in inflates peak route counts and
-- mixes a twice-a-day run in with an all-day service.
create or replace view school_route as
select
    route_id,
    route_short_name,
    route_long_name
from dim_route
where route_type = '712';

-- Trips on a school service, by way of their route.
create or replace view school_trip as
select trip.trip_id
from dim_trip as trip
where trip.route_id in (select route_id from school_route);

-- The routes every mart is built from: ordinary buses.
--
-- Route type 700 minus the rail-replacement operators that wear it.
-- Every other rail-replacement agency uses type 714; agencies 7083 and
-- 7084 (SW1, SW2, SW3, Bankstown and Campsie to Sydenham) are coded
-- 700 and run as often as a busy ordinary route. School runs coded 700
-- are kept; a trip-count floor at query time keeps them out of a
-- ranking if they distort it.
create or replace view ordinary_route as
select
    route_id,
    agency_id,
    route_short_name,
    route_long_name
from dim_route
where route_type = '700'
  and agency_id not in ('7083', '7084');

-- TfNSW's 2013 Service Planning Guidelines periods, by service hour.
--
-- Service hours run past 24: hour 25 is 01:00 the next morning on the
-- previous day's timetable. Take service_hour % 24 for a clock hour.
create or replace view period as
select
    hour::integer as service_hour,
    case
        when hour between 6 and 8 then 'am_peak'
        when hour between 9 and 14 then 'inter_peak'
        when hour between 15 and 17 then 'pm_peak'
        when hour between 18 and 21 then 'evening'
        else 'night'
    end as period
from generate_series(0, 47) as hours(hour);

-- Every timetable snapshot, and the Sydney date its check fell on.
--
-- valid_from is the UTC instant of the check, written without colons.
-- A check early on a Sydney morning is still the previous day in UTC,
-- so the date must be taken in Sydney. The 2026-09-23T020011Z snapshot
-- holds stop times alone; has_trips says whether the rest exist.
create or replace view snapshot as
with labels as (
    select distinct valid_from from dim_scheduled_stop_time_all
),
checks as (
    select
        valid_from,
        timezone(
            'UTC', strptime(valid_from, '%Y-%m-%dT%H%M%SZ')
        ) as checked_at_utc
    from labels
)
select
    valid_from,
    checked_at_utc,
    timezone('Australia/Sydney', checked_at_utc)::date as check_date,
    valid_from in (
        select distinct valid_from from dim_trip_all
    ) as has_trips
from checks;

-- The service days the marts cover, and the timetable each one used.
--
-- 16 September is left out: raw data begins at 02:06 on 17 September
-- Sydney time, so that day holds only its after-midnight tail.
--
-- stop_time_valid_from follows the merger exactly: the latest snapshot
-- checked on or before the day in Sydney time, or the earliest one for
-- a day before any was checked. trip_valid_from applies the same rule
-- to snapshots that hold trips and calendars, so a stop-times-only
-- snapshot borrows the previous full one. timetable_borrowed marks the
-- days that fell back to a later timetable; whether it changed in
-- between cannot be checked.
--
-- day_type is one label per day: public_holiday, saturday, sunday,
-- term_weekday, or holiday_weekday for a weekday in school holidays or
-- on a school development day.
create or replace view service_day as
with days as (
    select distinct service_date
    from fact_trip
    where service_date >= date '2026-09-17'
)
select
    days.service_date,
    coalesce(
        (select max(valid_from) from snapshot
         where check_date <= days.service_date),
        (select min(valid_from) from snapshot)
    ) as stop_time_valid_from,
    coalesce(
        (select max(valid_from) from snapshot
         where has_trips and check_date <= days.service_date),
        (select min(valid_from) from snapshot where has_trips)
    ) as trip_valid_from,
    not exists (
        select 1 from snapshot where check_date <= days.service_date
    ) as timetable_borrowed,
    case
        when days.service_date in (
            select day from calendar_exclusion
            where exclusion_type = 'public_holiday'
        ) then 'public_holiday'
        when isodow(days.service_date) = 6 then 'saturday'
        when isodow(days.service_date) = 7 then 'sunday'
        when days.service_date in (select day from term_weekday)
            then 'term_weekday'
        else 'holiday_weekday'
    end as day_type
from days;

-- Services running on each service day, by the day's own snapshot.
--
-- A calendar's window starts about when its bundle was published, so a
-- day that borrowed a later timetable is usually outside it and gets
-- few or no services here. scheduled_trip makes up for that with the
-- trips the feed reported.
create or replace view active_service as
with weekly as (
    select day.service_date, cal.valid_from, cal.service_id
    from service_day as day
    join dim_calendar_all as cal
      on cal.valid_from = day.trip_valid_from
     and strftime(day.service_date, '%Y%m%d')
         between cal.start_date and cal.end_date
     and '1' = case isodow(day.service_date)
            when 1 then cal.monday
            when 2 then cal.tuesday
            when 3 then cal.wednesday
            when 4 then cal.thursday
            when 5 then cal.friday
            when 6 then cal.saturday
            else cal.sunday
         end
),
exceptions as (
    select day.service_date, ex.valid_from, ex.service_id,
           ex.exception_type
    from service_day as day
    join dim_calendar_dates_all as ex
      on ex.valid_from = day.trip_valid_from
     and ex.date = strftime(day.service_date, '%Y%m%d')
)
(
    select * from weekly
    union
    select service_date, valid_from, service_id
    from exceptions where exception_type = '1'
)
except
select service_date, valid_from, service_id
from exceptions where exception_type = '2';

-- Every trip the marts expect on each service day: the spine.
--
-- A trip is on the spine when its calendar runs that day, or when the
-- feed reported it and the day's stop times know it. The second half
-- keeps trips the calendar misses, mostly on days outside the
-- calendar's window. ADDED trips absent from the timetable have no
-- schedule to measure against and stay off. is_timetabled says which
-- half a trip came from.
create or replace view scheduled_trip as
with timetabled as (
    select active.service_date, trip.trip_id
    from active_service as active
    join dim_trip_all as trip
      on trip.valid_from = active.valid_from
     and trip.service_id = active.service_id
),
reported as (
    select fact.service_date, fact.trip_id
    from fact_trip as fact
    join service_day as day using (service_date)
    where exists (
        select 1 from dim_scheduled_stop_time_all as stop_time
        where stop_time.valid_from = day.stop_time_valid_from
          and stop_time.trip_id = fact.trip_id
    )
),
spine as (
    select service_date, trip_id from timetabled
    union
    select service_date, trip_id from reported
)
select
    spine.service_date,
    spine.trip_id,
    trip.route_id,
    trip.direction_id,
    (spine.service_date, spine.trip_id) in (
        select (service_date, trip_id) from timetabled
    ) as is_timetabled
from spine
join dim_trip as trip using (trip_id);

-- Every scheduled call on the spine: one stop, on one trip, on one day.
--
-- is_boarding is false at the last stop and at set-down-only calls
-- (pickup_type 1), where nobody waits to board. Those calls are judged
-- on arrival with no early limit and get no headway. Every other call
-- is judged on departure. service_hour is the hour of the judged
-- event's scheduled time, past 24 after midnight. pickup_type is null
-- in snapshots written before it was kept, which reads as boarding.
create or replace view scheduled_call as
with calls as (
    select
        trip.service_date,
        trip.trip_id,
        trip.route_id,
        trip.direction_id,
        trip.is_timetabled,
        stop_time.stop_sequence,
        stop_time.stop_id,
        stop_time.timepoint,
        stop_time.pickup_type,
        gtfs_seconds(stop_time.arrival_time) as arrival_s,
        gtfs_seconds(stop_time.departure_time) as departure_s,
        stop_time.stop_sequence
            = min(stop_time.stop_sequence) over trip_calls
            as is_first_stop,
        stop_time.stop_sequence
            = max(stop_time.stop_sequence) over trip_calls
            as is_last_stop
    from scheduled_trip as trip
    join service_day as day using (service_date)
    join dim_scheduled_stop_time_all as stop_time
      on stop_time.valid_from = day.stop_time_valid_from
     and stop_time.trip_id = trip.trip_id
    window trip_calls as (partition by trip.service_date, trip.trip_id)
)
select
    calls.*,
    not calls.is_last_stop
        and calls.pickup_type is distinct from '1' as is_boarding,
    sydney_instant(calls.service_date, calls.arrival_s)
        as scheduled_arrival_utc,
    sydney_instant(calls.service_date, calls.departure_s)
        as scheduled_departure_utc,
    case when is_boarding then calls.departure_s else calls.arrival_s end
        // 3600 as service_hour
from calls;

-- Each scheduled call with what the feed said about it.
--
-- call_status is exactly one of:
--
--   skipped        the feed said the bus would not stop
--   observed       a predicted time for the judged event exists
--   cancelled      the trip's final status is CANCELED, and no time
--                  was predicted before the cancellation
--   no_prediction  the feed listed the call without a usable time
--   absent         the feed never mentioned the call
--
-- no_prediction and absent are unknown, not "did not run". An observed
-- call counts as a bus that ran whether or not it is judged: dropping
-- it from a headway sequence would merge two gaps into one.
--
-- A predicted time more than six hours from the schedule is not a
-- usable time: some first-stop departures arrive exactly a day late.
-- A cancelled trip's predictions from after the cancellation are
-- frozen, so they are not observations either.
--
-- is_judged says the time is fresh enough for on-time performance.
-- Departure shares arrival's update clock at every stop but the first,
-- so is_reliable, judged on arrival, holds for both there. At the first
-- stop the feed blanks the arrival once the bus leaves and later
-- updates can carry only a departure, so the departure itself must
-- have been updated within 60 seconds of its predicted time.
--
-- delay_s is observed minus scheduled, computed from the timestamps.
-- The feed's own delay fields are not used: its arrival delay is
-- measured against scheduled departure where the timetable has a
-- dwell.
create or replace view call_observation as
with joined as (
    select
        call.*,
        trip.final_status,
        trip.first_canceled_at_utc,
        stop.schedule_relationship,
        stop.is_reliable,
        stop.lost_tracking,
        stop.last_update_at_utc,
        stop.trip_id is not null as has_row,
        case when call.is_boarding
            then call.scheduled_departure_utc
            else call.scheduled_arrival_utc
        end as event_scheduled_utc,
        case when call.is_boarding
            then stop.final_predicted_departure_utc
            else stop.final_predicted_arrival_utc
        end as event_predicted_utc
    from scheduled_call as call
    left join fact_trip as trip using (service_date, trip_id)
    left join fact_trip_stop as stop using (
        service_date, trip_id, stop_sequence
    )
),
usable as (
    select
        joined.*,
        case
            when schedule_relationship = 'SCHEDULED'
             and abs(epoch(event_predicted_utc)
                     - epoch(event_scheduled_utc)) <= 6 * 3600
             and (final_status is distinct from 'CANCELED'
                  or event_predicted_utc < first_canceled_at_utc)
            then event_predicted_utc
        end as event_observed_utc
    from joined
)
select
    usable.* exclude (event_predicted_utc),
    case
        when schedule_relationship = 'SKIPPED' then 'skipped'
        when event_observed_utc is not null then 'observed'
        when final_status = 'CANCELED' then 'cancelled'
        when has_row then 'no_prediction'
        else 'absent'
    end as call_status,
    (epoch(event_observed_utc) - epoch(event_scheduled_utc))::integer
        as delay_s,
    event_observed_utc is not null and case
        when is_first_stop then
            not lost_tracking
            and last_update_at_utc
                >= event_observed_utc - interval 60 second
        else is_reliable
    end as is_judged
from usable;

-- Headways at every boarding call, scheduled and observed.
--
-- Sequences run per stop, route, direction and service day, so they
-- never span the overnight break. A headway belongs to the later
-- departure, and to that departure's scheduled hour. Observed headways
-- order the buses that ran by their predicted departure, so an
-- overtaking bus shortens one gap and lengthens the next.
create or replace view headway as
with boarding as (
    select call.*
    from call_observation as call
    join ordinary_route using (route_id)
    where call.is_boarding
),
scheduled as (
    select
        service_date, stop_id, route_id, direction_id, service_hour,
        'scheduled' as basis,
        epoch(scheduled_departure_utc)
            - epoch(lag(scheduled_departure_utc) over sequence)
            as headway_s
    from boarding
    window sequence as (
        partition by service_date, stop_id, route_id, direction_id
        order by scheduled_departure_utc, trip_id
    )
),
observed as (
    select
        service_date, stop_id, route_id, direction_id, service_hour,
        'observed' as basis,
        epoch(event_observed_utc)
            - epoch(lag(event_observed_utc) over sequence)
            as headway_s
    from boarding
    where call_status = 'observed'
    window sequence as (
        partition by service_date, stop_id, route_id, direction_id
        order by event_observed_utc, trip_id
    )
)
select * from scheduled where headway_s is not null
union all
select * from observed where headway_s is not null;

-- The stop mart: one row per stop, route, direction, service day and
-- service hour, on ordinary routes.
--
-- It stores ingredients, not finished figures, because hourly cells
-- are too thin to report alone and finished figures don't combine.
-- Excess wait time for any grouping is a ratio of sums:
--
--   sum(observed_headway_sq_s) / (2 * sum(observed_headway_sum_s))
--   - sum(scheduled_headway_sq_s) / (2 * sum(scheduled_headway_sum_s))
--
-- Averaging finished hourly figures instead would be wrong.
--
-- frequency_band comes from the cell's own mean scheduled headway:
-- <=10, 10-15, 15-20, 20-30 or >30 minutes. n_bunched counts observed
-- headways under a quarter of that mean. A predicted time is good to
-- about a minute, so bunching_below_precision marks cells where a
-- quarter of the mean is under 90 seconds.
--
-- On time is TfNSW's 2023 contract window applied at every stop:
-- departure no more than 59 s early or 5:59 late at boarding calls;
-- arrival no more than 5:59 late, with no early limit, at the last
-- stop and set-down-only calls. Only judged calls count. The same
-- counts at timepoints alone support a timepoint-only comparison.
-- Unreliable observed calls are n_observed minus n_judged.
create or replace view mart_stop_hour as
with calls as (
    select
        call.*,
        call.is_judged and call.is_boarding
            and call.delay_s < -59 as is_early,
        call.is_judged and call.delay_s > 359 as is_late
    from call_observation as call
    join ordinary_route using (route_id)
),
cell as (
    select
        service_date, stop_id, route_id, direction_id, service_hour,
        count(*) as n_scheduled_calls,
        count(*) filter (is_boarding) as n_boarding_calls,
        count(*) filter (call_status = 'observed') as n_observed,
        count(*) filter (call_status = 'skipped') as n_skipped,
        count(*) filter (call_status = 'cancelled') as n_cancelled,
        count(*) filter (call_status = 'no_prediction')
            as n_no_prediction,
        count(*) filter (call_status = 'absent') as n_absent,
        count(*) filter (is_judged) as n_judged,
        count(*) filter (is_early) as n_early,
        count(*) filter (is_late) as n_late,
        count(*) filter (is_judged and not is_early and not is_late)
            as n_on_time,
        count(*) filter (is_judged and timepoint = '1')
            as n_judged_timepoint,
        count(*) filter (
            is_judged and not is_early and not is_late
            and timepoint = '1'
        ) as n_on_time_timepoint
    from calls
    group by all
),
scheduled as (
    select
        service_date, stop_id, route_id, direction_id, service_hour,
        count(*) as scheduled_headway_n,
        sum(headway_s) as scheduled_headway_sum_s,
        sum(headway_s * headway_s) as scheduled_headway_sq_s
    from headway
    where basis = 'scheduled'
    group by all
),
observed as (
    select
        headway.service_date, headway.stop_id, headway.route_id,
        headway.direction_id, headway.service_hour,
        count(*) as observed_headway_n,
        sum(headway.headway_s) as observed_headway_sum_s,
        sum(headway.headway_s * headway.headway_s)
            as observed_headway_sq_s,
        count(*) filter (
            headway.headway_s < 0.25 * scheduled.scheduled_headway_sum_s
                / scheduled.scheduled_headway_n
        ) as n_bunched
    from headway
    left join scheduled using (
        service_date, stop_id, route_id, direction_id, service_hour
    )
    where headway.basis = 'observed'
    group by all
)
select
    cell.*,
    case
        when scheduled.scheduled_headway_n is null then null
        when scheduled.scheduled_headway_sum_s
             <= 600 * scheduled.scheduled_headway_n then '<=10'
        when scheduled.scheduled_headway_sum_s
             <= 900 * scheduled.scheduled_headway_n then '10-15'
        when scheduled.scheduled_headway_sum_s
             <= 1200 * scheduled.scheduled_headway_n then '15-20'
        when scheduled.scheduled_headway_sum_s
             <= 1800 * scheduled.scheduled_headway_n then '20-30'
        else '>30'
    end as frequency_band,
    coalesce(scheduled.scheduled_headway_n, 0) as scheduled_headway_n,
    coalesce(scheduled.scheduled_headway_sum_s, 0)
        as scheduled_headway_sum_s,
    coalesce(scheduled.scheduled_headway_sq_s, 0)
        as scheduled_headway_sq_s,
    coalesce(observed.observed_headway_n, 0) as observed_headway_n,
    coalesce(observed.observed_headway_sum_s, 0) as observed_headway_sum_s,
    coalesce(observed.observed_headway_sq_s, 0) as observed_headway_sq_s,
    coalesce(observed.n_bunched, 0) as n_bunched,
    0.25 * scheduled.scheduled_headway_sum_s
        / scheduled.scheduled_headway_n < 90 as bunching_below_precision
from cell
left join scheduled using (
    service_date, stop_id, route_id, direction_id, service_hour
)
left join observed using (
    service_date, stop_id, route_id, direction_id, service_hour
);

-- The trip mart: one row per trip on the spine per service day, on
-- ordinary routes.
--
-- fact_trip stores what the feed said, not a verdict. status applies
-- one rule, in this order:
--
--   cancelled   final status CANCELED, with no judged call
--   incomplete  final status CANCELED after judged calls, or judged at
--               the first stop and not judged for the last ten minutes
--               of scheduled running
--   ran         any judged call
--   unknown     none of the above
--
-- A trip cancelled and later reinstated is judged on what it did;
-- ever_canceled keeps the cancellation for a sensitivity run that
-- counts any cancellation.
--
-- unjudged_tail_s is the scheduled time from the last judged call to
-- the last stop. The last few stops often go unjudged on a trip that
-- finished: the terminus arrival is commonly last updated minutes
-- ahead, and tracking often drops as the bus arrives. So a short tail
-- is not incompletion, and a rule of "judged to the last stop" would
-- call a large share of ordinary trips incomplete. Group on
-- unjudged_tail_s for any other threshold. A trip that is running but
-- untracked cannot be told from a silent cancellation, and both read
-- unknown.
--
-- start_service_hour is the scheduled first departure's hour, and
-- first_stop_delay_s its judged departure delay, the SD1-comparable
-- figure.
create or replace view mart_trip as
with per_trip as (
    select
        call.service_date,
        call.trip_id,
        call.route_id,
        call.direction_id,
        call.is_timetabled,
        count(*) as n_calls,
        count(*) filter (call.call_status = 'observed') as n_observed,
        count(*) filter (call.is_judged) as n_judged,
        count(*) filter (call.call_status = 'skipped') as n_skipped,
        count(*) filter (
            call.call_status in ('no_prediction', 'absent')
        ) as n_unknown,
        min(call.stop_sequence) filter (call.is_judged)
            as first_judged_stop_sequence,
        max(call.stop_sequence) filter (call.is_judged)
            as last_judged_stop_sequence,
        max(call.stop_sequence) as last_stop_sequence,
        coalesce(bool_or(call.is_first_stop and call.is_judged), false)
            as judged_at_first_stop,
        max(call.arrival_s) filter (call.is_last_stop)
            - arg_max(call.arrival_s, call.stop_sequence)
                filter (call.is_judged) as unjudged_tail_s,
        min(call.delay_s) filter (call.is_first_stop and call.is_judged)
            as first_stop_delay_s,
        min(call.scheduled_departure_utc) filter (call.is_first_stop)
            as scheduled_start_utc,
        min(call.departure_s // 3600) filter (call.is_first_stop)
            as start_service_hour
    from call_observation as call
    join ordinary_route using (route_id)
    group by all
)
select
    per_trip.*,
    case
        when trip.final_status = 'CANCELED' and per_trip.n_judged = 0
            then 'cancelled'
        when trip.final_status = 'CANCELED' then 'incomplete'
        when per_trip.judged_at_first_stop
         and per_trip.unjudged_tail_s > 600 then 'incomplete'
        when per_trip.n_judged > 0 then 'ran'
        else 'unknown'
    end as status,
    trip.trip_id is not null as reported,
    trip.final_status,
    coalesce(trip.canceled_polls, 0) > 0 as ever_canceled,
    trip.canceled_polls,
    trip.first_canceled_at_utc,
    trip.had_vehicle
from per_trip
left join fact_trip as trip using (service_date, trip_id);

-- The current vintage of each reference table.
--
-- Reference data is not joined point in time, unlike the timetable. A
-- fact row belongs to the timetable that applied on its service day,
-- but the newest boundaries and census are the best description of a
-- place for every day, including days already collected. So the
-- latest vintage wins outright.
--
-- The filter reads the vintage column inside the files rather than the
-- partition value in the path. The two are written together and agree,
-- but only one of them survives being copied somewhere else.
create or replace view stop_geography as
select *
from stop_geography_all
where vintage = (select max(vintage) from stop_geography_all);

create or replace view stop_meshblock as
select *
from stop_meshblock_all
where vintage = (select max(vintage) from stop_meshblock_all);

-- Stops whose coordinates have moved since the geography was built.
--
-- A stop that moves keeps its old suburb, disadvantage score and
-- catchment until someone rebuilds. Nothing about the stale row looks
-- wrong, so the check has to be made deliberately.
create or replace view stop_geography_stale as
select
    current.stop_id,
    built.stop_lat as built_lat,
    built.stop_lon as built_lon,
    current.stop_lat,
    current.stop_lon,
    st_distance_sphere(
        st_point(built.stop_lat, built.stop_lon),
        st_point(current.stop_lat, current.stop_lon)
    ) as moved_m
from dim_stop as current
left join stop_geography as built on built.stop_id = current.stop_id
where built.stop_id is null
   or st_distance_sphere(
        st_point(built.stop_lat, built.stop_lon),
        st_point(current.stop_lat, current.stop_lon)
      ) > 25;

-- Residents within walking and straight-line distance of each stop.
--
-- Walking population counts only routed pairs. A pair with residents
-- inside 800 m that could not be routed is counted in
-- n_unrouted_meshblocks_800m instead of falling back to the straight
-- line, so a catchment missing people shows it.
create or replace view stop_catchment as
select
    stop_id,
    coalesce(sum(person_count) filter (
        routing_status = 'routed' and network_distance_m <= 400
    ), 0) as walk_population_400m,
    coalesce(sum(person_count) filter (
        routing_status = 'routed' and network_distance_m <= 800
    ), 0) as walk_population_800m,
    coalesce(sum(person_count) filter (
        straight_line_distance_m <= 800
    ), 0) as straight_population_800m,
    count(*) filter (
        straight_line_distance_m <= 800
        and person_count > 0
        and routing_status is distinct from 'routed'
    ) as n_unrouted_meshblocks_800m
from stop_meshblock
group by stop_id;

-- Everything known about where a stop is, one row per stop.
--
-- Marts carry keys only; join this at analysis time. A stop with no
-- mesh blocks nearby has null catchment, not zero.
create or replace view stop_context as
select
    geography.*,
    catchment.walk_population_400m,
    catchment.walk_population_800m,
    catchment.straight_population_800m,
    catchment.n_unrouted_meshblocks_800m
from stop_geography as geography
left join stop_catchment as catchment using (stop_id);

-- Combined scheduled departures per stop and service hour, across
-- every ordinary route: the corridor frequency a stop has, as a
-- covariate. Corridor EWT is not computed.
create or replace view stop_frequency as
select
    call.stop_id,
    call.service_date,
    call.service_hour,
    count(*) as n_departures,
    count(distinct call.route_id) as n_routes
from scheduled_call as call
join ordinary_route using (route_id)
where call.is_boarding
group by all;

-- The places a route serves, weighted by its scheduled calls.
--
-- A route spans many deciles, so it has no single geography; this is
-- a profile of its stops, one row per route and direction, over every
-- service day. Each stop counts once per scheduled call, the only
-- honest weight without boardings. A weighted quantile is the lowest
-- value whose cumulative weight reaches that share.
--
-- Stops with no SEIFA score are left out of the IRSD summaries and
-- counted in n_calls_null_seifa, never imputed. Quintile shares are of
-- calls with a score, from the state decile.
create or replace view route_context as
with calls as (
    select route_id, direction_id, stop_id, count(*) as n_calls
    from scheduled_call
    join ordinary_route using (route_id)
    group by all
),
stops as (
    select
        calls.*,
        geography.irsd_score,
        geography.irsd_state_decile,
        catchment.walk_population_800m,
        geography.distance_to_nearest_centre_m
    from calls
    left join stop_geography as geography using (stop_id)
    left join stop_catchment as catchment using (stop_id)
),
long as (
    unpivot (
        select
            route_id, direction_id, n_calls,
            irsd_score::double as irsd_score,
            walk_population_800m::double as walk_population_800m,
            distance_to_nearest_centre_m::double
                as distance_to_nearest_centre_m
        from stops
    )
    on irsd_score, walk_population_800m, distance_to_nearest_centre_m
    into name measure value value
),
cumulative as (
    select
        long.*,
        sum(n_calls) over (
            partition by route_id, direction_id, measure
            order by value
            rows between unbounded preceding and current row
        ) as weight_below,
        sum(n_calls) over (
            partition by route_id, direction_id, measure
        ) as weight_total
    from long
),
summary as (
    select
        route_id,
        direction_id,
        measure,
        sum(value * n_calls) / sum(n_calls) as mean,
        min(value) filter (weight_below >= 0.25 * weight_total) as q25,
        min(value) filter (weight_below >= 0.5 * weight_total) as median,
        min(value) filter (weight_below >= 0.75 * weight_total) as q75
    from cumulative
    group by all
),
profile as (
    select
        route_id,
        direction_id,
        count(*) as n_stops,
        sum(n_calls) as n_calls,
        coalesce(sum(n_calls) filter (irsd_score is null), 0)
            as n_calls_null_seifa,
        min(irsd_state_decile) as irsd_state_decile_min,
        max(irsd_state_decile) as irsd_state_decile_max,
        sum(n_calls) filter (irsd_state_decile in (1, 2))
            / sum(n_calls) filter (irsd_state_decile is not null)
            as irsd_quintile_1_call_share,
        sum(n_calls) filter (irsd_state_decile in (3, 4))
            / sum(n_calls) filter (irsd_state_decile is not null)
            as irsd_quintile_2_call_share,
        sum(n_calls) filter (irsd_state_decile in (5, 6))
            / sum(n_calls) filter (irsd_state_decile is not null)
            as irsd_quintile_3_call_share,
        sum(n_calls) filter (irsd_state_decile in (7, 8))
            / sum(n_calls) filter (irsd_state_decile is not null)
            as irsd_quintile_4_call_share,
        sum(n_calls) filter (irsd_state_decile in (9, 10))
            / sum(n_calls) filter (irsd_state_decile is not null)
            as irsd_quintile_5_call_share
    from stops
    group by all
)
select
    profile.*,
    max(summary.mean) filter (summary.measure = 'irsd_score')
        as irsd_score_call_weighted_mean,
    max(summary.median) filter (summary.measure = 'irsd_score')
        as irsd_score_call_weighted_median,
    max(summary.q25) filter (summary.measure = 'irsd_score')
        as irsd_score_call_weighted_q25,
    max(summary.q75) filter (summary.measure = 'irsd_score')
        as irsd_score_call_weighted_q75,
    max(summary.mean) filter (summary.measure = 'walk_population_800m')
        as walk_population_800m_call_weighted_mean,
    max(summary.median) filter (summary.measure = 'walk_population_800m')
        as walk_population_800m_call_weighted_median,
    max(summary.q25) filter (summary.measure = 'walk_population_800m')
        as walk_population_800m_call_weighted_q25,
    max(summary.q75) filter (summary.measure = 'walk_population_800m')
        as walk_population_800m_call_weighted_q75,
    max(summary.mean)
        filter (summary.measure = 'distance_to_nearest_centre_m')
        as distance_to_nearest_centre_m_call_weighted_mean,
    max(summary.median)
        filter (summary.measure = 'distance_to_nearest_centre_m')
        as distance_to_nearest_centre_m_call_weighted_median,
    max(summary.q25)
        filter (summary.measure = 'distance_to_nearest_centre_m')
        as distance_to_nearest_centre_m_call_weighted_q25,
    max(summary.q75)
        filter (summary.measure = 'distance_to_nearest_centre_m')
        as distance_to_nearest_centre_m_call_weighted_q75
from profile
left join summary using (route_id, direction_id)
group by all;

-- Whether a route's timetable is clock-face, per route, direction,
-- period and service day, at the first stop.
--
-- Clock-face means the median scheduled headway, in whole minutes, is
-- 15, 20, 30 or 60, and every departure's minutes past the hour repeat
-- on that cycle within a minute. It is a timetable shape, not a
-- frequency, and is meant for bands above 15 minutes.
create or replace view route_clockface as
with first_stop as (
    select
        call.service_date,
        call.route_id,
        call.direction_id,
        period.period,
        call.departure_s,
        call.departure_s // 60 as departure_minute,
        call.departure_s - lag(call.departure_s) over (
            partition by call.service_date, call.route_id,
                call.direction_id, period.period
            order by call.departure_s
        ) as headway_s
    from scheduled_call as call
    join ordinary_route using (route_id)
    join period on period.service_hour = call.departure_s // 3600
    where call.is_first_stop
),
cycle as (
    select
        service_date, route_id, direction_id, period,
        count(*) as n_departures,
        round(median(headway_s) / 60)::integer as headway_min
    from first_stop
    group by all
),
drift as (
    select
        first_stop.service_date, first_stop.route_id,
        first_stop.direction_id, first_stop.period,
        (
            (
                first_stop.departure_minute - first_value(
                    first_stop.departure_minute
                ) over (
                    partition by first_stop.service_date,
                        first_stop.route_id, first_stop.direction_id,
                        first_stop.period
                    order by first_stop.departure_s
                )
            ) % cycle.headway_min + cycle.headway_min
        ) % cycle.headway_min as offset_min,
        cycle.headway_min
    from first_stop
    join cycle using (service_date, route_id, direction_id, period)
    where cycle.headway_min in (15, 20, 30, 60)
),
worst as (
    select
        service_date, route_id, direction_id, period,
        max(least(offset_min, headway_min - offset_min)) as max_drift_min
    from drift
    group by all
)
select
    cycle.*,
    coalesce(
        cycle.headway_min in (15, 20, 30, 60)
        and cycle.n_departures >= 2
        and worst.max_drift_min <= 1,
        false
    ) as is_clockface
from cycle
left join worst using (service_date, route_id, direction_id, period);

-- Routes ranked by period, on term weekdays: a thin view over the two
-- marts.
--
-- Term weekdays only, because holiday and weekend service answers a
-- different question; group the marts directly for any other set of
-- days. A trip's period is that of its scheduled first departure.
-- excess_wait_s is the ratio of sums over every cell. unknown_share is
-- no_prediction plus absent calls over scheduled calls. sd7_rate is
-- cancelled plus incomplete trips over all trips, the contract's
-- completion measure; sd1_on_time_share is the share of judged first
-- stop departures within the on-time window.
create or replace view route_league as
with days as (
    select service_date
    from service_day
    where day_type = 'term_weekday'
),
cells as (
    select
        mart.route_id,
        mart.direction_id,
        period.period,
        count(distinct mart.service_date) as n_days,
        sum(mart.n_scheduled_calls) as n_scheduled_calls,
        sum(mart.n_judged) as n_judged,
        sum(mart.n_on_time) as n_on_time,
        sum(mart.n_no_prediction + mart.n_absent) as n_unknown_calls,
        sum(mart.scheduled_headway_n) as scheduled_headway_n,
        sum(mart.scheduled_headway_sum_s) as scheduled_headway_sum_s,
        sum(mart.scheduled_headway_sq_s) as scheduled_headway_sq_s,
        sum(mart.observed_headway_n) as observed_headway_n,
        sum(mart.observed_headway_sum_s) as observed_headway_sum_s,
        sum(mart.observed_headway_sq_s) as observed_headway_sq_s
    from mart_stop_hour as mart
    join days using (service_date)
    join period using (service_hour)
    group by all
),
trips as (
    select
        trip.route_id,
        trip.direction_id,
        period.period,
        count(*) as n_trips,
        count(*) filter (trip.status = 'ran') as n_ran,
        count(*) filter (trip.status = 'cancelled') as n_cancelled,
        count(*) filter (trip.status = 'incomplete') as n_incomplete,
        count(*) filter (trip.status = 'unknown') as n_unknown_trips,
        count(*) filter (trip.first_stop_delay_s is not null)
            as n_first_stop_judged,
        count(*) filter (
            trip.first_stop_delay_s between -59 and 359
        ) as n_first_stop_on_time
    from mart_trip as trip
    join days using (service_date)
    join period on period.service_hour = trip.start_service_hour
    group by all
)
select
    route_id,
    route.route_short_name,
    direction_id,
    period,
    cells.* exclude (route_id, direction_id, period),
    trips.* exclude (route_id, direction_id, period),
    cells.n_on_time / nullif(cells.n_judged, 0) as on_time_share,
    cells.n_unknown_calls / nullif(cells.n_scheduled_calls, 0)
        as unknown_share,
    cells.observed_headway_sq_s
        / nullif(2 * cells.observed_headway_sum_s, 0)
    - cells.scheduled_headway_sq_s
        / nullif(2 * cells.scheduled_headway_sum_s, 0) as excess_wait_s,
    (trips.n_cancelled + trips.n_incomplete)
        / nullif(trips.n_trips, 0) as sd7_rate,
    trips.n_first_stop_on_time / nullif(trips.n_first_stop_judged, 0)
        as sd1_on_time_share
from cells
full join trips using (route_id, direction_id, period)
join ordinary_route as route using (route_id);

-- Scheduled boarding calls per SA2, period and day type, including
-- none at all.
--
-- Every SA2 with a stop appears in every period and day type, so "no
-- service" is a row with zero calls rather than a missing one. An SA2
-- with no stop in the geography at all does not appear. calls_per_day
-- divides by the number of service days of that type.
create or replace view coverage as
with areas as (
    select distinct sa2_code, sa2_name, gccsa_name
    from stop_geography
    where sa2_code is not null
),
day_types as (
    select day_type, count(*) as n_days
    from service_day
    group by day_type
),
calls as (
    select
        geography.sa2_code,
        period.period,
        day.day_type,
        count(*) as n_calls
    from scheduled_call as call
    join ordinary_route using (route_id)
    join service_day as day using (service_date)
    join period using (service_hour)
    join stop_geography as geography using (stop_id)
    where call.is_boarding
    group by all
)
select
    areas.sa2_code,
    areas.sa2_name,
    areas.gccsa_name,
    periods.period,
    day_types.day_type,
    day_types.n_days,
    coalesce(calls.n_calls, 0) as n_calls,
    coalesce(calls.n_calls, 0) / day_types.n_days as calls_per_day
from areas
cross join (select distinct period from period) as periods
cross join day_types
left join calls using (sa2_code, period, day_type);
