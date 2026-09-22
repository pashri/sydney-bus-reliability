-- Analysis views over the curated layer.
--
-- Create these inputs first, then run this file against them:
--
--   calendar_exclusion_seed  analysis/calendar_exclusions_<year>.csv
--   dim_route                curated/dim_route/
--   dim_trip                 curated/dim_trip/
--
-- For example:
--
--   create view calendar_exclusion_seed as
--     select * from read_csv('analysis/calendar_exclusions_2026.csv');
--   create view dim_route as
--     select * from read_parquet('s3://<bucket>/curated/dim_route/*/*.parquet');

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
