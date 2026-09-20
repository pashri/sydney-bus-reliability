# Data dictionary

Every table this project stores, column by column.

This document assumes you have never seen transit data before. It explains
the vocabulary first, then describes each table: what one row of it
represents, what each column holds, and where that value came from.

Everything here is taken from the code that writes the data. Where the code
does not settle a question, the entry says so rather than guessing.

For what the data can and cannot be used to conclude, and the measurements
behind each limit, see [`methodology.md`](methodology.md). This document
describes shape; that one describes trust.

## Contents

- [What you need to know first](#what-you-need-to-know-first)
- [How to read the tables](#how-to-read-the-tables)
- [Where everything lives](#where-everything-lives)
- [Raw data](#raw-data)
  - [`raw/vehiclepos/`](#rawvehiclepos)
  - [`raw/tripupdates/`](#rawtripupdates)
- [Hourly intermediates](#hourly-intermediates)
  - [`curated/_partial/vehicle_position/`](#curated_partialvehicle_position)
  - [`curated/_partial/trip_stop/`](#curated_partialtrip_stop)
- [Service-day facts](#service-day-facts)
  - [`fact_trip_stop`](#fact_trip_stop)
  - [`fact_vehicle_position`](#fact_vehicle_position)
  - [`fact_collector_run`](#fact_collector_run)
- [Dimensions](#dimensions)
  - [`dim_route`](#dim_route)
  - [`dim_trip`](#dim_trip)
  - [`dim_stop`](#dim_stop)
  - [`dim_scheduled_stop_time`](#dim_scheduled_stop_time)
  - [`dim_shape`](#dim_shape)
  - [`dim_calendar`](#dim_calendar)
  - [`dim_calendar_dates`](#dim_calendar_dates)
- [Audit tables](#audit-tables)
  - [`collector_run`](#collector_run)
  - [`curation_run`](#curation_run)
  - [`schedule_check`](#schedule_check)
- [Learn more](#learn-more)

---

## What you need to know first

### GTFS

GTFS is the file format the world's transit agencies use to publish
timetables. A GTFS feed is a zip file of comma-separated text files:
`routes.txt`, `trips.txt`, `stops.txt`, and so on. Transport for NSW
republishes one whenever the timetable changes.

Reference: [GTFS Schedule](https://gtfs.org/documentation/schedule/reference/).

### GTFS-Realtime

GTFS-Realtime is the companion format for live data. It answers "where is
the bus now" and "when will it get here", which the timetable cannot.

It is not text. It is [protocol buffers](https://protobuf.dev/programming-guides/proto2/),
a compact binary encoding, so you cannot open a live feed in a text editor
and read it. You need the message definitions to decode it.

References: [GTFS-Realtime](https://gtfs.org/documentation/realtime/reference/),
and the [message definitions themselves](https://github.com/google/transit/blob/master/gtfs-realtime/proto/gtfs-realtime.proto).

### Trip updates and vehicle positions

Transport for NSW publishes two live feeds, and this project collects both.

A **vehicle position** says where one bus is at one moment: latitude,
longitude, and a timestamp. Each tracked bus reports about every 10 seconds.

A **trip update** says when one bus is expected to reach the stops still
ahead of it. Each is a bundle of predictions, one per upcoming stop, and the
whole feed refreshes about every 60 seconds.

These predictions are the closest thing this project has to arrival times.
Transport for NSW never publishes "the bus arrived at 08:14" -
see [methodology 1](methodology.md#1-arrival-times-are-predictions-not-observations).

References: [trip updates](https://gtfs.org/documentation/realtime/feed-entities/trip-updates/),
[vehicle positions](https://gtfs.org/documentation/realtime/feed-entities/vehicle-positions/).

### Trip, route, stop

A **route** is a numbered service, like the 333.

A **trip** is one run of that route at one time, like the 07:14 from
Parramatta. A route has hundreds of trips a day.

A **stop** is one kerbside place a bus calls at. A trip calls at a sequence
of them.

None of these is a bus. The word for a physical bus is **vehicle**, and
identifying one is harder than it looks -
see [methodology 2](methodology.md#2-vehicle_id-identifies-a-trip-not-a-bus).

### Service day

A **service day** is a transport day, not a calendar day. A trip leaving at
00:30 on Saturday morning belongs to Friday's service day, because Friday's
timetable is the one it runs on.

Timetables therefore count hours past 24. A departure written `25:10:00`
leaves at 01:10 the following morning. Hour 30 appears in the real Transport
for NSW bundle, meaning 06:00 the next day.

### Facts and dimensions

Tables named `fact_` hold things that happened: observations, one row per
event. They are large and they grow every day.

Tables named `dim_` describe the things those events refer to: the
timetable's own account of routes, trips and stops. They are small, and they
change only when the timetable changes.

A `fact_` table stores identifiers like `route_id` and nothing else. To turn
one into a route number people recognise, join to `dim_route`.

### Parquet

The derived tables are [Parquet](https://parquet.apache.org/docs/) files, a
columnar binary format. Every column has a declared type, so the types in
this document are what is actually stored, not a convention.

The fact tables are compressed with zstd and written in sorted order: trip
stops by route, trip and stop; vehicle positions by route and time. The
hourly intermediates and the dimensions use snappy instead, because the
pyarrow that writes them cannot produce zstd. Parquet records the smallest and largest value of every column in each
block of rows, so a query filtered to one route can skip most of a day's
blocks without reading them.

---

## How to read the tables

**Type** is the stored type, read from the schema definitions in the code,
or measured from a file the pipeline produced.

- `string` - text, any length.
- `int32` - whole number.
- `double` - decimal number. Arrow and pyarrow call this `float64`; it is
  the same 64-bit type.
- `bool` - true or false.
- `timestamp[s, tz=UTC]` - an instant, stored to the second, in UTC.
- `timestamp[us, tz=UTC]` - the same, stored to the microsecond. Nothing in
  this project measures sub-second time; the extra precision is an artefact
  of the daily merge rewriting the files, not a claim of accuracy.

**Timezones.** Every stored instant is UTC. Sydney is UTC+10, or UTC+11 when
daylight saving is in effect, so a Sydney reader must convert. Two kinds of
value are Sydney-local instead, and they are dates or clock readings rather
than instants:

- `service_date` and the `valid_from=` / `service_date=` partition values
  are Sydney calendar dates.
- `arrival_time` and `departure_time` in `dim_scheduled_stop_time` are
  Sydney wall-clock readings, and can exceed 24 hours.

**Units.** Durations are seconds, named with an `_s` suffix. Coordinates are
decimal degrees, WGS 84, the same convention as a phone map.

**Nulls.** Where a column can be empty, the entry says when and why. One
distinction matters throughout: a **null** means the value was never
recorded, while an **empty string** means the feed carried the field but
left it blank. The code reads identifiers without checking whether they were
sent, so a missing identifier arrives as `''`, not as null. Filter on
`= ''` as well as `IS NULL` when you check for missing identifiers.

**Claims about what TfNSW sends.** Some entries say a field is populated
or empty. Those come from two places, and where they disagree this document
follows the measurement, because the point of the column is to tell you what
is in it. TfNSW's own field-level guide for the bus feeds is the
[Buses Fileset Consumer Guide](https://opendata.transport.nsw.gov.au/sites/default/files/2024-10/TfNSW_Realtime_Bus_Technical_Doc_v4.4.pdf);
where this project has measured the feed and found something different, the
entry says so and names both. Where there is no measurement, the guide
stands unopposed.

**Unrecognised codes.** Most coded fields - `occupancy_status`,
`current_status`, `congestion_level`, `trip_schedule_relationship`, and
`vehicle_position`'s `schedule_relationship` - read as null if Transport
for NSW ever sends a value outside the published list. The original value
is not preserved anywhere.

`trip_stop`'s own `schedule_relationship` is the exception. It is read
without first asking whether the field was sent, so a value outside the
list, or no value at all, reads as `SCHEDULED` - the format's default -
and cannot be told apart from a genuine `SCHEDULED`. See
[methodology 12](methodology.md#12-codes-we-dont-recognise-arrive-as-missing).

---

## Where everything lives

Everything sits in one S3 bucket, Amazon's flat file store.

    raw/
      vehiclepos/dt=YYYY-MM-DD/hour=HH/HHMMSS.pb.gz
      tripupdates/dt=YYYY-MM-DD/hour=HH/HHMMSS.pb.gz
    curated/
      _partial/vehicle_position/dt=YYYY-MM-DD/hour=HH/data.parquet
      _partial/trip_stop/dt=YYYY-MM-DD/hour=HH/data.parquet
      fact_vehicle_position/service_date=YYYY-MM-DD/data.parquet
      fact_trip_stop/service_date=YYYY-MM-DD/data.parquet
      fact_collector_run/collection_date=YYYY-MM-DD/data.parquet
      dim_route/valid_from=YYYY-MM-DD/data.parquet
      dim_trip/...                  (and five more dimensions)
      collector_run/dt=YYYY-MM-DD/<invocation-id>.jsonl
      curation_run/dt=YYYY-MM-DD/<invocation-id>.jsonl
      schedule_check/dt=YYYY-MM-DD/HHMMSS-<invocation-id>.jsonl

The `dt=` and `hour=` path segments are partitions, in the layout most
query engines call Hive partitioning: the engine reads them as extra columns
and uses them to skip files it does not need.

A partition whose name matches a real column **shadows it**. `fact_trip_stop`
has both a `service_date=` folder and a `service_date` column. They hold the
same date, deliberately, so it does not matter which one a reader gets. Where
they ever diverge the folder wins, and anything that reads a file by its full
path and writes it back would persist the folder's value over the column's.
`dt` and `hour` under `raw/` and `_partial/` are **UTC**. `service_date=`
and `collection_date=` are **Sydney dates**. `valid_from=` is a **UTC
date**, taken from the instant of the check: the scheduled noon-Sydney run
lands on the same date either way, but a run before 10:00 Sydney - 11:00
while daylight saving is in effect - files under the previous UTC date.

Three prefixes expire on a schedule. `raw/` is deleted after 30 days.
`curated/_partial/` is deleted after 3 days, which is the window in which a
failed merge can still be re-run. `curated/collector_run/` is deleted after
30 days, by which time the merger has folded each day into
`fact_collector_run`, which is kept. Everything else is kept.

The leading underscore in `_partial` marks it as working state rather than a
published table. Query the `fact_` tables instead unless you specifically
need one hour.

Four jobs write all of it, and the rest of this document names them:

- the **collector** polls both live feeds and writes `raw/`, every minute
- the **compactor** turns one hour of `raw/` into two files under `_partial/`
- the **merger** folds a service day of `_partial/` into the `fact_` tables
- the **schedule loader** downloads the timetable and writes the dimensions

---

## Raw data

This is what arrives from Transport for NSW, stored byte for byte before
anything touches it. Each object is one poll of one feed: gzipped protocol
buffers, named for the second it was fetched.

Because it is binary, you cannot query it with SQL. The compactor decodes
it into the Parquet tables below, and that is what analysis reads. This section
exists so you can tell what the pipeline had to work with, and check its
work.

The two feeds share an outer envelope. A `FeedMessage` carries a list of
`entity` values, and each entity holds either a `vehicle` (a vehicle
position) or a `trip_update`, never both. Entities of the other kinds the
specification allows - `alert`, `shape`, `stop`, `trip_modifications` - are
not read.

Field paths below are written from the entity down, matching how the code
navigates them. A dot means a step into a nested block of fields, which
protocol buffers calls a message; `message` in the Type column means the
field is one of those blocks rather than a single value.

Portal: [TfNSW Open Data Hub](https://opendata.transport.nsw.gov.au/).
Field-by-field documentation for the bus feeds, including which fields TfNSW
populates and a full sample message:
[GTFS Feed for NSW Buses Fileset Consumer Guide](https://opendata.transport.nsw.gov.au/sites/default/files/2024-10/TfNSW_Realtime_Bus_Technical_Doc_v4.4.pdf)
(PDF). It is not linked from any of the dataset pages; it lives under
[developers/documentation](https://opendata.transport.nsw.gov.au/developers/documentation).

### `raw/vehiclepos/`

**One object is one poll of the vehicle position feed. One entity inside it
is one bus at one moment.** Polled roughly every 10 seconds.

Dataset page:
[Public Transport - Realtime Vehicle Positions](https://opendata.transport.nsw.gov.au/data/dataset/public-transport-realtime-vehicle-positions).

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `vehicle.timestamp` | `uint64` | When the bus reported this position. Seconds since 1 January 1970 UTC, the standard Unix epoch. | Read directly, without checking whether it was sent. An unsent value reads as 0, which becomes 1970-01-01. |
| `vehicle.vehicle.id` | `string` | An identifier issued per trip, not per bus. Do not group buses by it. See [methodology 2](methodology.md#2-vehicle_id-identifies-a-trip-not-a-bus). | Populated. |
| `vehicle.vehicle.label` | `string` | The fleet number painted on the bus, e.g. `8183`. This is the real identity of a physical vehicle. | Populated. |
| `vehicle.trip.trip_id` | `string` | Which scheduled run the bus is on. Joins to `dim_trip`. | Populated. |
| `vehicle.trip.route_id` | `string` | Which route the bus is running. Joins to `dim_route`. A small share do not match the timetable; they are kept, not guessed at. See [methodology 9](methodology.md#9-identifiers-that-dont-match-are-recorded-not-repaired). | Populated. |
| `vehicle.trip.schedule_relationship` | `enum` | Whether the trip is running as timetabled. One of `SCHEDULED`, `ADDED`, `UNSCHEDULED`, `CANCELED`, `REPLACEMENT`, `DUPLICATED`, `DELETED`, `NEW`. | Read only when sent, so it is absent rather than defaulted. |
| `vehicle.position.latitude` | `float` | Latitude, decimal degrees, WGS 84. Sydney is about -33.9. | Present whenever the `position` sub-message is present. |
| `vehicle.position.longitude` | `float` | Longitude, decimal degrees, WGS 84. Sydney is about 151.2. | Present whenever the `position` sub-message is present. |
| `vehicle.position.bearing` | `float` | Which way the bus is pointing, in degrees clockwise from true north: 0 is north, 90 is east. | Populated. TfNSW's bus guide documents it as the bearing reported by the bus itself, and warns against deducing direction from a sequence of positions instead. |
| `vehicle.position.speed` | `float` | How fast the bus is going, in **metres per second**. Multiply by 3.6 for km/h. | Populated. TfNSW's bus guide calls it "momentary speed measured by the vehicle, in meters per second", taken from the bus's own reported velocity. |
| `vehicle.current_status` | `enum` | Whether the bus is approaching, stopped at, or travelling to its next stop. | **Never sent by TfNSW**, measured across every entity in a sampled hour. TfNSW's bus guide lists the field as one it provides, and its own sample message omits it; the measurement is what this pipeline sees. Its unset default is `IN_TRANSIT_TO`, which would look like a real answer, so the code checks for presence and stores null. |
| `vehicle.occupancy_status` | `enum` | How full the bus is, counted by the on-board passenger counter and compared against the seating capacity of that vehicle. | Populated, but only three of the seven values TfNSW lists are ever emitted: `MANY_SEATS_AVAILABLE` at half the seats or fewer, `FEW_SEATS_AVAILABLE` up to the seated capacity, `STANDING_ROOM_ONLY` above it. `EMPTY`, `FULL`, `CRUSHED_STANDING_ROOM_ONLY` and `NOT_ACCEPTING_PASSENGERS` have no rule defined against them. The scale therefore saturates: a bus with five standing passengers and one with fifty read the same. |
| `vehicle.congestion_level` | `enum` | Not a traffic reading, despite the name. TfNSW compute it statistically: how far the recent journey time sits above that segment's own long-run average, measured in standard deviations. `RUNNING_SMOOTHLY` is within one, `STOP_AND_GO` one to two, `CONGESTION` two to three, `SEVERE_CONGESTION` beyond three. `UNKNOWN_CONGESTION_LEVEL` means the short-term average was not available. | Populated. Because it is normalised against each segment's own history, it is closer to a delay signal than to a measure of traffic, which makes it worth more to this project than the name suggests. |

Not read from this feed: `vehicle.stop_id`, `vehicle.current_stop_sequence`,
`vehicle.position.odometer`, `vehicle.occupancy_percentage`,
`vehicle.vehicle.license_plate`, and every field on the feed header.

### `raw/tripupdates/`

**One object is one poll of the trip update feed. One entity inside it is
one trip; one `stop_time_update` within that entity is one prediction, for
one stop, on that trip.** Polled roughly every 60 seconds.

Dataset page:
[Public Transport - Realtime Trip Update](https://opendata.transport.nsw.gov.au/data/dataset/public-transport-realtime-trip-update).

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `trip_update.trip.start_date` | `string` | The service day this trip belongs to, as `YYYYMMDD`, Sydney local. Authoritative: a trip running past midnight keeps the date it started under. | Read directly, so an unsent value reads as `''`. |
| `trip_update.trip.trip_id` | `string` | Which scheduled run this is. Joins to `dim_trip`. | Populated. |
| `trip_update.trip.route_id` | `string` | Which route. Joins to `dim_route`. | Populated. |
| `trip_update.trip.schedule_relationship` | `enum` | Whether the whole trip is running as timetabled. `CANCELED` here means the trip is cancelled, and it is a different thing from `NO_DATA` on a stop. See [methodology 4](methodology.md#4-no_data-does-not-mean-cancelled). | Read only when sent. |
| `trip_update.timestamp` | `uint64` | When Transport for NSW last refreshed this trip. Unix epoch seconds, UTC. | **Often absent.** When it is, the pipeline falls back to the time the poll was issued. |
| `trip_update.vehicle` | message | Presence alone is read, not the contents: it says whether a bus was assigned to this trip at all. | Roughly 3% of trips that should be underway carry no vehicle. See [methodology 5](methodology.md#5-about-3-of-running-trips-report-no-bus-at-all). |
| `trip_update.stop_time_update.stop_id` | `string` | Which stop this prediction is for. Joins to `dim_stop`. | Read directly, so an unsent value reads as `''`. |
| `trip_update.stop_time_update.stop_sequence` | `uint32` | Where this stop falls in the trip's order, counting from the start. Needed as well as `stop_id`, because loop and shuttle routes call the same stop twice on one trip. | Populated on every row in the feed. |
| `trip_update.stop_time_update.schedule_relationship` | `enum` | What kind of statement this is: `SCHEDULED` (a real prediction), `SKIPPED` (the bus will not stop), `NO_DATA` (no live information - the timetable echoed back), or `UNSCHEDULED`. | The `NO_DATA` case is roughly a quarter of rows and is not an observation. See [methodology 3](methodology.md#3-a-quarter-of-the-rows-are-the-timetable-echoed-back). |
| `trip_update.stop_time_update.arrival.time` | `int64` | Predicted arrival at this stop. Unix epoch seconds, UTC. | Optional within `arrival`. |
| `trip_update.stop_time_update.arrival.delay` | `int32` | How late the arrival is against the timetable, in seconds. Negative means early. | Optional within `arrival`. |
| `trip_update.stop_time_update.departure.time` | `int64` | Predicted departure from this stop. Unix epoch seconds, UTC. | Optional within `departure`. |
| `trip_update.stop_time_update.departure.delay` | `int32` | How late the departure is, in seconds. Negative means early. | Optional within `departure`. |

Not read from this feed: `trip_update.delay` (the trip-level summary),
`stop_time_update.departure_occupancy_status`, `StopTimeEvent.uncertainty`,
`StopTimeEvent.scheduled_time`, and every field on the feed header.

---

## Hourly intermediates

Each hour, the compactor reads that hour of raw objects and writes two
Parquet files. This is where the binary feeds first become queryable.

These files are working state. They are deleted after 3 days, and each holds
only one UTC hour, so a service day is spread across about 32 of them. Use
the `fact_` tables for analysis; use these when you need to see what one
hour actually contained.

### `curated/_partial/vehicle_position/`

**One row is one bus at one moment, within one UTC hour.**

Near-identical samples are collapsed first. A bus is polled about as often
as it refreshes, so roughly a quarter of consecutive samples restate the
previous timestamp. A row survives if its vehicle, timestamp, latitude and
longitude have not all been seen together in this hour. The position is in
that test on purpose. Hundreds of samples an hour repeat a timestamp while
reporting a genuinely different place, and testing on the vehicle and
timestamp alone would throw that movement away.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `observed_at_utc` | `timestamp[s, tz=UTC]` | When the bus reported this position. | `vehicle.timestamp`, converted from Unix epoch seconds. Reads as 1970-01-01 when the feed omitted it, because the field is read without a presence check. |
| `fetched_at_utc` | `timestamp[s, tz=UTC]` | When the pipeline issued the poll that returned this row. | Recovered from the raw object's own key, which is named for its fetch time. |
| `position_age_s` | `double` | Seconds. How stale the position already was when we fetched it. Positive means the bus reported before we asked. | Computed: `fetched_at_utc` minus `observed_at_utc`. |
| `vehicle_id` | `string` | Per-trip identifier. Not a bus. See [methodology 2](methodology.md#2-vehicle_id-identifies-a-trip-not-a-bus). | Copied from `vehicle.vehicle.id`. `''` if the feed omitted it. |
| `vehicle_label` | `string` | The fleet number, e.g. `8183`. Use this to follow a physical bus. | Copied from `vehicle.vehicle.label`. `''` if omitted. |
| `trip_id` | `string` | Which scheduled run. Joins to `dim_trip.trip_id`. | Copied from `vehicle.trip.trip_id`. `''` if omitted. |
| `route_id` | `string` | Which route. Joins to `dim_route.route_id`. | Copied from `vehicle.trip.route_id`. `''` if omitted. |
| `lat` | `double` | Latitude, decimal degrees, WGS 84. | Copied from `vehicle.position.latitude`. Null when the feed sent no `position` at all. |
| `lon` | `double` | Longitude, decimal degrees, WGS 84. | Copied from `vehicle.position.longitude`. Null when the feed sent no `position` at all. |
| `bearing` | `double` | Which way the bus is pointing, degrees clockwise from true north. | Copied from `vehicle.position.bearing`. Null when the feed sent no `position` at all, or when `bearing` itself was not sent. |
| `speed` | `double` | How fast the bus is going, in **metres per second**. Multiply by 3.6 for km/h. | Copied from `vehicle.position.speed`. Null when the feed sent no `position` at all, or when `speed` itself was not sent. |
| `occupancy_status` | `string` | How full the bus is, stored as the code's own name rather than its number so an unfamiliar value shows up as itself. In practice only `MANY_SEATS_AVAILABLE`, `FEW_SEATS_AVAILABLE` and `STANDING_ROOM_ONLY` occur, and the scale saturates above seated capacity - see the `raw/vehiclepos/` entry. | Copied from `vehicle.occupancy_status`. Null when not sent or not recognised. |
| `congestion_level` | `string` | How far the recent journey time sits above this segment's own long-run average, in standard deviations, by name - `RUNNING_SMOOTHLY` through `SEVERE_CONGESTION`. Not a traffic measurement; see the `raw/vehiclepos/` entry. | Copied from `vehicle.congestion_level`. Null when not sent or not recognised. |
| `schedule_relationship` | `string` | The trip's status, by name, e.g. `SCHEDULED`, `CANCELED`. | Copied from `vehicle.trip.schedule_relationship`. Null when not sent or not recognised. |
| `current_status` | `string` | Whether the bus is approaching, stopped at, or heading for its next stop. | Copied from `vehicle.current_status`. **Always null**, because TfNSW never sends the field, though their guide lists it as one they provide. |
| `null_island` | `bool` | True when the bus reported latitude 0 and longitude 0 - a point in the Atlantic off West Africa, and the usual signature of a GPS unit with no fix. | Computed: true when `lat` and `lon` are both exactly 0. The row is flagged, not dropped. Exclude these before mapping anything. |

### `curated/_partial/trip_stop/`

**One row is one stop, on one trip, on one service day - reduced to the best
information that hour produced.**

Consecutive polls overlap almost entirely, so the hour's updates are folded
down by roughly 40 to 1. The reduction follows three rules:

1. `NO_DATA` rows echo the timetable rather than reporting anything, so
   their times and delays are nulled. The row is kept, because "nothing was
   reported here" is worth knowing.
2. A `NO_DATA` row never overwrites a real observation. It usually means
   tracking dropped part-way through a run.
3. `n_updates` counts real observations only. A trip is listed and echoed
   every 60 seconds for hours before it departs.

Among real observations, the latest wins.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `service_date` | `string` | The service day, as `YYYYMMDD`, Sydney local. Part of the row's identity. | Copied from `trip_update.trip.start_date`. `''` if the feed omitted it. |
| `trip_id` | `string` | Which scheduled run. Joins to `dim_trip.trip_id`. | Copied from `trip_update.trip.trip_id`. `''` if omitted. |
| `stop_id` | `string` | Which stop. Joins to `dim_stop.stop_id`. | Copied from `stop_time_update.stop_id`. `''` if omitted. |
| `stop_sequence` | `int32` | Position of this stop in the trip's order. Part of the row's identity, because a loop route calls the same stop twice. | Copied from `stop_time_update.stop_sequence`. Null if the feed omitted it, which is not observed in practice. |
| `route_id` | `string` | Which route. Joins to `dim_route.route_id`. | Copied from `trip_update.trip.route_id`. `''` if omitted. |
| `final_predicted_arrival_utc` | `timestamp[s, tz=UTC]` | The last real prediction of when the bus reaches this stop. This project's stand-in for the arrival time. | From `arrival.time` on the latest `SCHEDULED` or `SKIPPED` update. Null when no real update landed this hour, or when the update carried a delay but no time. |
| `delay_s` | `int32` | Seconds late against the timetable, from that same last real prediction. Negative means early. | From `arrival.delay`. Null when no real update landed, or when the update carried a time but no delay. |
| `final_predicted_departure_utc` | `timestamp[s, tz=UTC]` | The last real prediction of when the bus leaves this stop. | From `departure.time` on the same update. Null when absent. |
| `departure_delay_s` | `int32` | Seconds late leaving. Negative means early. | From `departure.delay`. Null when absent. |
| `last_update_at_utc` | `timestamp[s, tz=UTC]` | When the last **real** observation for this stop was made. | `trip_update.timestamp` when the feed sent one, otherwise the poll time. Null when this stop was only ever echoed. |
| `n_updates` | `int32` | How many real observations landed for this stop during this hour. Echoes are not counted, so `0` means the stop was listed but never genuinely reported on. | Counted during the reduction. Never null. |
| `schedule_relationship` | `string` | What the feed last said about this stop: `SCHEDULED`, `SKIPPED`, `NO_DATA` or `UNSCHEDULED`. | The latest real observation's value once one exists; before that, whatever was last seen, including `NO_DATA`. Read without first asking whether the field was sent, so a value outside the list, or no value at all, reads as `SCHEDULED`. Set on the stop's first sighting, so never null. |
| `trip_schedule_relationship` | `string` | What the feed said about the whole trip, e.g. `SCHEDULED`, `CANCELED`. Separate from the stop's own status. | Copied from `trip_update.trip.schedule_relationship`, taken when the row was first created. Null when not sent or not recognised. |
| `had_vehicle` | `bool` | True if a bus was ever attached to this trip during the hour. | True once any poll carried `trip_update.vehicle`, and it never goes back to false. Never null. |
| `lost_tracking` | `bool` | True when the bus went quiet part-way: a real observation landed, and then only echoes followed. The prediction is frozen at whatever it last said. | Computed at write time: true when `last_observed_at_utc` is later than `last_update_at_utc`. False when neither exists, because a stop that was never reported on was not lost. |
| `last_observed_at_utc` | `timestamp[s, tz=UTC]` | When this stop was last mentioned by the feed at all, echo or not. Bookkeeping for `lost_tracking`. | The latest observation time of any kind. Null only if the row was somehow never observed. **Not carried into `fact_trip_stop`** - it is folded into `lost_tracking` there. |

The two timestamps come from different clocks:
`last_update_at_utc` prefers the feed's own stamp, `last_observed_at_utc`
often falls back to poll time. That does not change which value is last -
see [methodology 13](methodology.md#13-lost_tracking-compares-two-different-clocks).

---

## Service-day facts

Once a day, the merger folds about 32 hourly partials into one file per
service day. These are the tables to query.

The window it reads runs from an hour before Sydney midnight to seven hours
after the following midnight, which covers trips timetabled as late as hour
30 plus a margin for late-written files.

### `fact_trip_stop`

**One row is one stop, on one trip, on one service day.** The main table for
punctuality work.

Written to `curated/fact_trip_stop/service_date=YYYY-MM-DD/data.parquet`,
where `service_date` is the Sydney service day.

Hourly rows for the same stop are combined in two different ways, on
purpose. The descriptive columns - times, delays, statuses - are taken whole
from the single most recent hour that carried a real observation, so they
stay mutually consistent. The counters are summed across every hour, so they
describe the day rather than its last hour.

Timestamps below are wider than the partials': the merge rewrites the files
and promotes second precision to microsecond. No extra precision was
measured.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `service_date` | `date32[day]` | The service day, Sydney local. Stored as a date so it matches the `service_date=` folder it is written under: most query engines expose that folder as a column of the same name, and if the two disagreed a reader would silently get one or the other. | Converted from the partial's `YYYYMMDD` text, which is how the feed sends it. |
| `trip_id` | `string` | Which scheduled run. Joins to `dim_trip.trip_id`. | Carried through. |
| `stop_id` | `string` | Which stop. Joins to `dim_stop.stop_id`. | Carried through. |
| `stop_sequence` | `int32` | Position of this stop in the trip's order. | Carried through. |
| `route_id` | `string` | Which route. Joins to `dim_route.route_id`. | Carried through from the most recent hour's row. |
| `final_predicted_arrival_utc` | `timestamp[us, tz=UTC]` | The day's last real prediction of arrival at this stop. Used as the arrival time. | From the most recent hour that carried a real observation. Null when no hour did, or when the last observation gave a delay but no time. |
| `delay_s` | `int32` | Seconds late on arrival. Negative means early. | Same source row as `final_predicted_arrival_utc`. Null when no real observation landed all day. |
| `final_predicted_departure_utc` | `timestamp[us, tz=UTC]` | The day's last real prediction of departure. | Same source row. Null when absent. |
| `departure_delay_s` | `int32` | Seconds late departing. Negative means early. | Same source row. Null when absent. |
| `last_update_at_utc` | `timestamp[us, tz=UTC]` | When the day's last real observation was made. How fresh `delay_s` is. | Same source row. Null when the stop was only ever echoed. |
| `n_updates` | `int32` | Real observations across the whole day. `0` means the stop was listed but never genuinely reported on. | Summed over every hour, not taken from the last one, so it does not depend on file read order. Never null. |
| `schedule_relationship` | `string` | This stop's status: `SCHEDULED`, `SKIPPED`, `NO_DATA`, `UNSCHEDULED`. | From the most recent hour's row. |
| `trip_schedule_relationship` | `string` | The whole trip's status, e.g. `CANCELED`. Not the same thing as the column above - see [methodology 4](methodology.md#4-no_data-does-not-mean-cancelled). | From the most recent hour's row. |
| `had_vehicle` | `bool` | True if a bus was attached to this trip at any point in the day. | True if any hour said so. Never null. |
| `lost_tracking` | `bool` | True when the bus stopped reporting before reaching this stop and only echoes followed. The prediction is stale. | Recomputed for the whole day: true when the day's latest observation of any kind is later than its latest real one. False when there was never a real observation. Never null. |
| `is_reliable` | `bool` | True when the arrival time is worth trusting: a real delay exists, tracking did not drop, and the last update landed no more than 60 seconds before the predicted arrival. **Headline figures use only rows where this is true.** | Computed. False when any condition fails, including when a delay exists but no predicted arrival time does, leaving nothing to compare against. Never null. The 60-second rule is explained in [methodology 1](methodology.md#1-arrival-times-are-predictions-not-observations). |
| `scheduled_arrival_utc` | `timestamp[us, tz=UTC]` | When the timetable said the bus should arrive. Subtract from `final_predicted_arrival_utc` to get lateness directly. | Joined from `dim_scheduled_stop_time` on `trip_id` and `stop_sequence`, using the most recent snapshot in effect on or before this service date. Its `HH:MM:SS` reading, which may exceed 24 hours, is added to Sydney midnight and converted to UTC. Null when no snapshot exists for the date, or when that snapshot has no matching row. Which midnight it counts from is settled one way here and is not confirmed against TfNSW - see [methodology 10](methodology.md#10-which-midnight-a-timetable-time-counts-from). |

Days before the first timetable was captured have live data and no schedule
to compare it to, so `scheduled_arrival_utc` is null throughout them. See
[methodology 8](methodology.md#8-the-timetable-only-describes-the-future).

### `fact_vehicle_position`

**One row is one bus at one moment, across a whole service day.**

Written to
`curated/fact_vehicle_position/service_date=YYYY-MM-DD/data.parquet`.

The columns are exactly those of
[`curated/_partial/vehicle_position/`](#curated_partialvehicle_position),
with the same meanings, so they are not repeated here. Two things differ.

Every timestamp is `timestamp[us, tz=UTC]` rather than
`timestamp[s, tz=UTC]`, because the merge rewrote the file.

Rows are deduplicated a second time, because a stale position can be
restated across an hour boundary and appear in two partials. Where a
vehicle, timestamp, latitude and longitude all repeat, the copy fetched
earliest is kept. Whether a row belongs to the day is decided by
`observed_at_utc` falling inside the day's window, not by which file it came
from.

This table has no `service_date` column. The service day is the partition
name only.

### `fact_collector_run`

**One row is one attempt to fetch one feed.** The health record of the
collection process, folded from JSON into Parquet so it can be queried
alongside everything else.

Written to
`curated/fact_collector_run/collection_date=YYYY-MM-DD/data.parquet`.

The columns are those of [`collector_run`](#collector_run) below, which is
the source, with one difference: the three timestamp columns are stored as
`timestamp[us, tz=UTC]` here rather than as text.

**The partition is `collection_date`, a Sydney calendar day**, midnight to
midnight, cut from the two UTC source partitions it spans. It is named
`collection_date` rather than `service_date` precisely because it is not
one. The cut uses Sydney's offset on the
day rather than a fixed one, because a Sydney day is 23 or 25 hours long
across a daylight-saving change.

That is deliberately not the same thing as `fact_trip_stop.service_date`,
even though both are Sydney dates. A service day follows the
timetable and runs past midnight, so a trip that departs late is still being
observed in the small hours of the next calendar day. The collector has no
such shape: it polls on the clock, every minute, whether or not a bus is
running, and a calendar day is the only grain that gives it a fixed expected
row count. Joining the two on their partition value is close but not exact.
To ask how collection was going while a given service day was being
observed, filter `fetched_at_utc` over that day's window instead, which the
column's type supports.

The column types are declared, not inferred. That matters more than it
sounds: left to infer, the reader types a column from the values it happens
to see, so a day where one column is entirely null, or one mixing timestamps
written with and without fractional seconds, comes back as text instead. See
[DuckDB's JSON reader](https://duckdb.org/docs/stable/data/json/overview) for
what it would otherwise guess.

The source JSON under `collector_run/` is left in place by this step, so
any day can be rebuilt from it - but only until the bucket deletes it after
30 days. Past that, `fact_collector_run` is the only copy.

---

## Dimensions

The seven dimensions are the timetable, unpacked out of the GTFS zip into
one Parquet file each.

They are snapshots, not a history. The loader downloads the bundle once a
day and writes a new snapshot only when the bundle's contents have changed,
under `valid_from=YYYY-MM-DD`, the UTC date of the check that noticed the
change. To use the timetable that applied on a given day, take the latest
`valid_from` at or before it, which is what the merge does.

Every value in a GTFS zip arrives as quoted text, so types are set
deliberately here rather than guessed. Identifiers stay `string`: `stop_id`
runs 5 to 7 digits with no fixed width, and `200013` sits beside `2000100`,
so reading it as a number breaks the join. Sequence numbers become `int32`,
because as text they sort `1, 10, 11, 2`, silently reordering any route with
ten or more stops.

Reference: [GTFS Schedule](https://gtfs.org/documentation/schedule/reference/).
Source bundle: [Timetables Complete GTFS](https://opendata.transport.nsw.gov.au/data/dataset/timetables-complete-gtfs).

The bundle covers all of New South Wales and many operators, not just
Sydney. See [methodology 7](methodology.md#7-the-feeds-cover-the-whole-state-not-just-sydney)
for the count and what it means for analysis.

### `dim_route`

**One row is one route**, such as the 333. From `routes.txt`.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `route_id` | `string` | The identifier the live feeds use for this route. The join key. | Copied from `routes.txt`. |
| `agency_id` | `string` | Which operator runs it. The bundle carries many. | Copied. |
| `route_short_name` | `string` | The number on the front of the bus, e.g. `333`. | Copied. |
| `route_long_name` | `string` | The longer descriptive name, where the bundle gives one. | Copied. |
| `route_type` | `string` | The GTFS code for the mode of transport, e.g. `3` for bus. Kept as text, not converted to a number. Codes are listed in the [GTFS reference](https://gtfs.org/documentation/schedule/reference/#routestxt). | Copied verbatim. |

Two operators can run the same route number under different `route_id`s,
which is why one unmatched id in the live feed is genuinely unresolvable.
See [methodology 9](methodology.md#9-identifiers-that-dont-match-are-recorded-not-repaired).

### `dim_trip`

**One row is one scheduled run of a route.** From `trips.txt`.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `trip_id` | `string` | The identifier the live feeds use for this run. The join key. | Copied from `trips.txt`. |
| `route_id` | `string` | Which route this run belongs to. Joins to `dim_route`. | Copied. |
| `service_id` | `string` | Which pattern of days this run operates on. Joins to `dim_calendar` and `dim_calendar_dates`. | Copied. |
| `direction_id` | `string` | Which way round the route this run goes, `0` or `1`. The labels are the operator's own; GTFS does not define which is inbound. Kept as text. | Copied. **This is the only source of direction in the whole project** - neither live feed sends it. |
| `trip_headsign` | `string` | The destination shown on the front of the bus. | Copied. |
| `shape_id` | `string` | Which drawn path this run follows. Joins to `dim_shape`. | Copied. |

### `dim_stop`

**One row is one stop.** From `stops.txt`.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `stop_id` | `string` | The identifier the live feeds use for this stop. The join key. Text, not a number: widths vary. | Copied from `stops.txt`. |
| `stop_name` | `string` | The name printed on the timetable, including the street and stand where the bundle gives them. | Copied. |
| `stop_lat` | `double` | Latitude, decimal degrees, WGS 84. | Parsed from text. Null when the bundle left the field blank. |
| `stop_lon` | `double` | Longitude, decimal degrees, WGS 84. | Parsed from text. Null when the bundle left the field blank. |

These coordinates are how "Sydney" is defined at analysis time. The feeds do
not hand you a Sydney filter; you apply one. See
[methodology 7](methodology.md#7-the-feeds-cover-the-whole-state-not-just-sydney).

### `dim_scheduled_stop_time`

**One row is one scheduled call: one stop, on one trip.** From
`stop_times.txt`. This is the dimension `fact_trip_stop` joins to for
`scheduled_arrival_utc`.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `trip_id` | `string` | Which run. Joins to `dim_trip`. | Copied from `stop_times.txt`. |
| `stop_id` | `string` | Which stop. Joins to `dim_stop`. | Copied. |
| `stop_sequence` | `int32` | Where this call falls in the run's order. Converted to a number so it sorts correctly. | Parsed from text. |
| `arrival_time` | `string` | Scheduled arrival, as `HH:MM:SS`, **Sydney wall clock, not UTC**. The hour can exceed 24: `25:10:00` means 01:10 the next morning. Kept as written, because resolving it to an instant needs a date. | Copied verbatim. |
| `departure_time` | `string` | Scheduled departure, same convention. | Copied verbatim. |
| `shape_dist_traveled` | `double` | How far along the route's drawn path this stop sits, in **metres**, measured from the first point of the shape. | Parsed from text. Null when blank. The unit is TfNSW's answer alone: GTFS lets the publisher choose and asks only that the two files agree, while TfNSW's guide states metres for both. The values are not checked against the shape geometry anywhere in this project. |

### `dim_shape`

**One row is one point on a route's drawn path.** From `shapes.txt`. Join
the points of one `shape_id` in `shape_pt_sequence` order to draw the line a
bus follows on a map. This file holds millions of rows.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `shape_id` | `string` | Which path this point belongs to. Joins to `dim_trip.shape_id`. | Copied from `shapes.txt`. |
| `shape_pt_sequence` | `int32` | Where this point falls along the path. Converted to a number so it sorts correctly. | Parsed from text. |
| `shape_pt_lat` | `double` | Latitude of the point, decimal degrees, WGS 84. | Parsed from text. Null when blank. |
| `shape_pt_lon` | `double` | Longitude of the point, decimal degrees, WGS 84. | Parsed from text. Null when blank. |
| `shape_dist_traveled` | `double` | Distance along the path at this point, in **metres**, measured from the first point of the shape. Increases with `shape_pt_sequence`, so it cannot express doubling back. | Parsed from text. Null when blank, **and also null when the bundle omits the column entirely** - this is the one place the loader tolerates the column's absence. |

### `dim_calendar`

**One row is one named service pattern and the window it runs in.** From
`calendar.txt`. Use it with `dim_calendar_dates` to work out whether a trip
was meant to run on a given day.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `service_id` | `string` | The pattern's identifier. Joins to `dim_trip.service_id`. | Copied from `calendar.txt`. |
| `monday` | `string` | `1` if this pattern runs on Mondays, `0` if not. Kept as text, not a boolean. | Copied verbatim. |
| `tuesday` | `string` | As above, for Tuesdays. | Copied verbatim. |
| `wednesday` | `string` | As above, for Wednesdays. | Copied verbatim. |
| `thursday` | `string` | As above, for Thursdays. | Copied verbatim. |
| `friday` | `string` | As above, for Fridays. | Copied verbatim. |
| `saturday` | `string` | As above, for Saturdays. | Copied verbatim. |
| `sunday` | `string` | As above, for Sundays. | Copied verbatim. |
| `start_date` | `string` | First day the pattern applies, `YYYYMMDD`, Sydney local. Inclusive. | Copied verbatim. |
| `end_date` | `string` | Last day the pattern applies, `YYYYMMDD`, Sydney local. Inclusive. | Copied verbatim. |

The window looks forward from the day the bundle was published and barely
covers that day itself, so this table cannot reconstruct a past timetable.
See [methodology 8](methodology.md#8-the-timetable-only-describes-the-future).

### `dim_calendar_dates`

**One row is one single-day exception to a service pattern** - a public
date on which service is added or removed, such as a public holiday. From
`calendar_dates.txt`.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `service_id` | `string` | Which pattern this exception applies to. Joins to `dim_calendar` and `dim_trip`. | Copied from `calendar_dates.txt`. |
| `date` | `string` | The day the exception applies to, `YYYYMMDD`, Sydney local. | Copied verbatim. |
| `exception_type` | `string` | `1` means service runs on this date even though the weekly pattern says otherwise. `2` means it does not run. Kept as text. | Copied verbatim. |

---

## Audit tables

These record what each job read and wrote. They exist so a quiet day is
distinguishable from a broken pipeline: with no audit trail, a day with few
buses and a day where collection died both look like a day with few rows.

All three are JSON Lines - one JSON object per line, plain text. There is no
declared schema, so a query engine infers types from the values it sees.
Every timestamp in them is an ISO 8601 UTC string.

### `collector_run`

**One row is one attempt to fetch one feed.** Written by the collector every
time it runs, whether the fetch worked or not.

At `curated/collector_run/dt=YYYY-MM-DD/<invocation-id>.jsonl`, where `dt`
is the UTC date of the fetch. One file per invocation, with one line per
feed attempted.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `feed` | `string` | Which feed was fetched: `vehiclepos` or `tripupdates`. | The feed the poll was scheduled for. |
| `fetched_at_utc` | `string` | When the request was issued, by the local clock. ISO 8601, UTC. | Recorded immediately before the request. |
| `received_at_utc` | `string` | When the response arrived, by the local clock. ISO 8601, UTC. | Recorded immediately after. |
| `rtt_s` | `double` | Seconds the round trip took. **Not** the clock difference below; the two are separate numbers. | Computed: `received_at_utc` minus `fetched_at_utc`. |
| `server_date_utc` | `string` or null | The time Transport for NSW's own server put on the response. ISO 8601, UTC. | Parsed from the HTTP `Date` header. Null when the header was missing or unparseable. |
| `skew_s` | `double` or null | Seconds our clock is ahead of the server's. Negative means behind. Corrected for network latency by comparing the server's stamp against the midpoint of send and receive, which removes roughly one leg of the round trip. | Computed. Null exactly when `server_date_utc` is null. |
| `status_code` | `int` or null | The HTTP status, e.g. `200`. | From the response. Null when the request never completed. |
| `body_bytes` | `int` | Size of the response body in bytes, before compression. `0` on any failure. | Measured. |
| `error` | `string` or null | What went wrong, in plain words. Null on success. | The transport error's message, or the literal `poll worker crashed unexpectedly` when the poll itself died. **A crash and a network failure are otherwise indistinguishable** - both give a null status, zero bytes and a null server date - and the message is human-readable text, not a code to branch on. |

A failed fetch is stored as a row, not thrown away. Inside the 30-day
retention window a missing row means the poll never ran at all; past it,
the JSONL has been deleted and `fact_collector_run` is where to look.

The Parquet-backed copy of this table is
[`fact_collector_run`](#fact_collector_run), which re-cuts these rows into
Sydney service days and gives the timestamp columns a declared type.

### `curation_run`

**One row is one run of one processing job.** Written by the compactor and
the merger, once per invocation.

At `curated/curation_run/dt=YYYY-MM-DD/<invocation-id>.jsonl`, where `dt` is
the UTC date the run **started**, so a run crossing midnight files under the
day it began.

Several counters are hard-coded to `0` rather than measured, and which ones
depends on the job. The table names each, because a hard `0` and a genuine
zero look identical and nothing else in the row tells them apart.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `job` | `string` | Which job ran: `compactor` or `merger`. The value `schedule_loader` is defined in the code but never written here; the schedule loader writes to [`schedule_check`](#schedule_check) instead. | Set by the job. |
| `invocation_id` | `string` | The run's unique identifier, matching the filename. | The AWS request id. |
| `started_at_utc` | `string` | When the run began. ISO 8601, UTC. | Recorded at entry. |
| `finished_at_utc` | `string` | When the run finished. ISO 8601, UTC. Subtract from the above for elapsed time. | Recorded as the record is built. |
| `partition` | `string` | What the run covered. `YYYY-MM-DDTHH` for the compactor, being one UTC hour. `YYYY-MM-DD` for the merger, being one Sydney service date. | Formatted from the target. |
| `objects_expected` | `int` | How many input files there should have been. For the compactor, 420 per hour: 360 position polls plus 60 trip-update polls, at the deployed cadence. For the merger, the number of hours the service day's window covers, times two tables. | A constant for the compactor; counted from the window for the merger. |
| `objects_read` | `int` | How many actually existed. **Lower than expected means a short run.** A failed poll leaves a permanent gap; a missing hour means the compactor failed or its partial expired before the merge. | Counted. The merger checks S3 directly rather than trusting the query. |
| `rows_in` | `int` | Compactor: real trip-stop observations seen, before reduction. Merger: always `0`; it folds already-curated files, so there is nothing to count. | Counted during reduction. |
| `rows_out` | `int` | Rows written. Compactor: both partials added together. Merger: all three fact tables added together. | Counted from the writers. |
| `dupes_collapsed` | `int` | Compactor: repeated vehicle-position samples discarded. A large number is normal - the feed is polled about as often as it refreshes. Merger: always `0`. | Counted by the deduplicator. |
| `dupes_differing_position` | `int` | Compactor: samples sharing a vehicle and timestamp while reporting a different place. These are kept, not collapsed, which is why the position is part of the deduplication key. Watching this number is how you would notice that changing. Merger: always `0`. | Counted by the deduplicator. |
| `unjoined_route_ids` | `int` | Intended to count live route ids with no timetable match. Always `0` in both jobs - neither currently measures it. Treat as not recorded, not as zero. | Not measured. |
| `unjoined_trip_ids` | `int` | As above, for trip ids. Always `0`. | Not measured. |
| `unjoined_stop_ids` | `int` | As above, for stop ids. Always `0`. | Not measured. |
| `peak_rss_mb` | `int` | The most memory the run held at once, in megabytes. This is how you see a job growing towards its memory ceiling before it hits it. | Read from the operating system at the end of the run. |
| `error` | `string` or null | Recorded as null. A run that fails badly enough throws before it writes anything, so the failure shows up as a missing record, not as an error string here. | Always null in practice. |

A run that failed before writing leaves no row at all. Absence is the signal.

### `schedule_check`

**One row is one daily check of the timetable bundle, whether or not it had
changed.** Written every day on purpose, so an unchanged day is a recorded
fact rather than a gap.

At `curated/schedule_check/dt=YYYY-MM-DD/HHMMSS-<invocation-id>.jsonl`,
where `dt` is the UTC date of the check.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `checked_at_utc` | `string` | When the bundle was downloaded. ISO 8601, UTC. | Recorded at entry. |
| `zip_sha256` | `string` | A fingerprint of the downloaded zip's bytes, as 64 hex characters. Two downloads with the same fingerprint are the same timetable. | Computed over the whole file. |
| `zip_filename` | `string` | The filename the server offered, e.g. `buses_GTFS_PROD_20260918103100.zip`. **Provenance only.** The timestamp in it changes on every rebuild whether or not the contents did, so it must never be used to detect change. | Read from the `Content-Disposition` header. `''` when the header is absent, or uses a form the parser does not handle. |
| `changed` | `bool` | True when this fingerprint differs from the previous check's. Only a true here causes dimension snapshots to be written. | Compared against the most recent stored check. True on the very first check ever, since there is nothing to compare to. |
| `valid_from` | `string` or null | The snapshot partition this check created, as `YYYY-MM-DD`, UTC. Matches the `valid_from=` folder under `curated/dim_*`. | The check date when `changed` is true. **Null when `changed` is false**, because no snapshot was written. |

To find which timetable was in force on a given day, take the largest
`valid_from` at or before it. That is what the merge does when it resolves
`scheduled_arrival_utc`.

If this job does not run on a day the timetable changed, that timetable is
gone. The bundle describes the future and is not archived anywhere else. See
[methodology 8](methodology.md#8-the-timetable-only-describes-the-future).

---

## Learn more

**The formats**

- [GTFS Schedule reference](https://gtfs.org/documentation/schedule/reference/) -
  every field of every file in the timetable zip.
- [GTFS-Realtime reference](https://gtfs.org/documentation/realtime/reference/) -
  the live message definitions, including the full list of values for each
  coded field.
- [Trip updates explained](https://gtfs.org/documentation/realtime/feed-entities/trip-updates/)
  and [vehicle positions explained](https://gtfs.org/documentation/realtime/feed-entities/vehicle-positions/) -
  prose versions of the same, worth reading first.
- [gtfs-realtime.proto](https://github.com/google/transit/blob/master/gtfs-realtime/proto/gtfs-realtime.proto) -
  the actual message definitions the decoder is built from.
- [Protocol buffers, version 2](https://protobuf.dev/programming-guides/proto2/) -
  the encoding. The section on field presence explains why an unset field is
  indistinguishable from a sent default unless you check.

**The source**

- [TfNSW Open Data Hub](https://opendata.transport.nsw.gov.au/) - the portal.
  Both live feeds and the timetable bundle need an API key from here.
- [TfNSW developer documentation](https://opendata.transport.nsw.gov.au/developers/documentation) -
  the per-mode technical guides. Not linked from the dataset pages, and the
  most useful thing on the site.
- [GTFS Feed for NSW Buses Fileset Consumer Guide](https://opendata.transport.nsw.gov.au/sites/default/files/2024-10/TfNSW_Realtime_Bus_Technical_Doc_v4.4.pdf) -
  the one that matters here. Field-by-field tables for both the static bus
  fileset and the realtime feeds, the rules behind each `occupancy_status`
  and `congestion_level` value, and a full sample vehicle-position message.
- [Public Transport - Realtime Vehicle Positions](https://opendata.transport.nsw.gov.au/data/dataset/public-transport-realtime-vehicle-positions)
- [Public Transport - Realtime Trip Update](https://opendata.transport.nsw.gov.au/data/dataset/public-transport-realtime-trip-update)
- [Timetables Complete GTFS](https://opendata.transport.nsw.gov.au/data/dataset/timetables-complete-gtfs) -
  the bundle the dimensions come from.

**The storage**

- [Apache Parquet](https://parquet.apache.org/docs/) - the columnar format
  the derived tables use.
- [Parquet logical types](https://parquet.apache.org/docs/file-format/types/) -
  how timestamps and their precision are recorded in a file.
- [DuckDB's JSON reader](https://duckdb.org/docs/stable/data/json/overview) -
  how the audit tables' types get inferred, and how to override it.

**This project**

- [`methodology.md`](methodology.md) - what the data can and cannot tell you,
  with the measurement behind each limit.
