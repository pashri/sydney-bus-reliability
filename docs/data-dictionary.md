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
  - [`curated/_partial/trip/`](#curated_partialtrip)
- [Service-day facts](#service-day-facts)
  - [`fact_trip_stop`](#fact_trip_stop)
  - [`fact_trip`](#fact_trip)
  - [`fact_vehicle_position`](#fact_vehicle_position)
  - [`fact_collector_run`](#fact_collector_run)
- [Dimensions](#dimensions)
  - [`dim_agency`](#dim_agency)
  - [`dim_route`](#dim_route)
  - [`dim_trip`](#dim_trip)
  - [`dim_stop`](#dim_stop)
  - [`dim_scheduled_stop_time`](#dim_scheduled_stop_time)
  - [`dim_shape`](#dim_shape)
  - [`dim_calendar`](#dim_calendar)
  - [`dim_calendar_dates`](#dim_calendar_dates)
  - [`dim_calendar_exclusion`](#dim_calendar_exclusion)
- [Reference tables](#reference-tables)
  - [`stop_geography`](#stop_geography)
  - [`stop_meshblock`](#stop_meshblock)
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

The live feed does not always say which service day a trip belongs to. A
trip whose first timetabled time is 24:00 or later is sent with its start
time wrapped below 24:00 and its start date set to the next calendar date.
The merger corrects this from the timetable when it builds the `fact_`
tables, so a `service_date` there is the true service day. The correction
needs the trip in the timetable snapshot. A trip missing from it keeps the
feed's date: rail-replacement runs that are not in the bus bundle, and, for
days before the first snapshot, trips withdrawn before it was captured.
Their rows are filed a day late with no `scheduled_arrival_utc`.

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

- `service_date`, and the `service_date=` and `collection_date=` partition
  values, are Sydney calendar dates. (`valid_from=` is not: it is a UTC
  instant, below.)
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
`current_status`, `congestion_level`, `fact_trip`'s `final_status`, and
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
      _partial/trip/dt=YYYY-MM-DD/hour=HH/data.parquet
      fact_vehicle_position/service_date=YYYY-MM-DD/data.parquet
      fact_trip_stop/service_date=YYYY-MM-DD/data.parquet
      fact_trip/service_date=YYYY-MM-DD/data.parquet
      fact_collector_run/collection_date=YYYY-MM-DD/data.parquet
      dim_route/valid_from=YYYY-MM-DDTHHMMSSZ/data.parquet
      dim_trip/...                  (and six more dimensions)
      schedule_bundle/valid_from=YYYY-MM-DDTHHMMSSZ/<bundle filename>.zip
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
and `collection_date=` are **Sydney dates**. `valid_from=` is the **UTC
instant of the check**, to the second, written without colons, e.g.
`2026-09-23T230911Z`. Query engines read it as text; sorting the text sorts
the snapshots in time order.

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
- the **compactor** turns one hour of `raw/` into three files under `_partial/`
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
| `trip_update.trip.start_date` | `string` | The day this trip starts, as `YYYYMMDD`, Sydney local. Usually the service day: a trip that starts before midnight and runs past it keeps the date it started under. **Not the service day** for a trip timetabled to start at 24:00 or later: the feed sends the next calendar date, and writes `start_time` minus 24 hours. | Read directly, so an unsent value reads as `''`. |
| `trip_update.trip.trip_id` | `string` | Which scheduled run this is. Joins to `dim_trip`. | Populated. |
| `trip_update.trip.route_id` | `string` | Which route. Joins to `dim_route`. | Populated. |
| `trip_update.trip.schedule_relationship` | `enum` | Whether the whole trip is running as timetabled. `CANCELED` here means the trip is cancelled, and it is a different thing from `NO_DATA` on a stop. See [methodology 4](methodology.md#4-no_data-does-not-mean-cancelled). | Read only when sent. A `CANCELED` update carries only the trip descriptor: no stop-time updates, no vehicle and no timestamp. A trip can be cancelled and later reinstated. |
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

Each hour, the compactor reads that hour of raw objects and writes three
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
| `service_date` | `string` | The feed's start date, as `YYYYMMDD`, Sydney local. Part of the row's identity. Despite the name, not always the service day - see [Service day](#service-day). The merger derives the true one. | Copied from `trip_update.trip.start_date`. `''` if the feed omitted it. |
| `trip_id` | `string` | Which scheduled run. Joins to `dim_trip.trip_id`. | Copied from `trip_update.trip.trip_id`. `''` if omitted. |
| `stop_id` | `string` | Which stop. Joins to `dim_stop.stop_id`. | Copied from `stop_time_update.stop_id`. `''` if omitted. |
| `stop_sequence` | `int32` | Position of this stop in the trip's order. Part of the row's identity, because a loop route calls the same stop twice. | Copied from `stop_time_update.stop_sequence`. Null if the feed omitted it, which is not observed in practice. |
| `route_id` | `string` | Which route. Joins to `dim_route.route_id`. | Copied from `trip_update.trip.route_id`. `''` if omitted. |
| `final_predicted_arrival_utc` | `timestamp[s, tz=UTC]` | The last real prediction of when the bus reaches this stop. This project's stand-in for the arrival time. | From `arrival.time` on the latest `SCHEDULED` or `SKIPPED` update that sent an arrival. An update with no arrival, or with `arrival.time = 0`, keeps the earlier arrival: the feed blanks the first stop's arrival to 0 once the bus leaves, and 0 read literally is 1970. Null when no real update landed this hour, or when the update carried a delay but no time. |
| `delay_s` | `int32` | Seconds late against the timetable, from that same last real prediction. Negative means early. | From `arrival.delay`, on the same update as the arrival time. A zero arrival time discards its delay too. Null when no real update landed, or when the update carried a time but no delay. |
| `final_predicted_departure_utc` | `timestamp[s, tz=UTC]` | The last real prediction of when the bus leaves this stop. | From `departure.time` on the same update. Null when absent. |
| `departure_delay_s` | `int32` | Seconds late leaving. Negative means early. | From `departure.delay`. Null when absent. |
| `last_update_at_utc` | `timestamp[s, tz=UTC]` | When the last **real** observation for this stop was made. | `trip_update.timestamp` when the feed sent one, otherwise the poll time. Null when this stop was only ever echoed. |
| `arrival_updated_at_utc` | `timestamp[s, tz=UTC]` | When the arrival in this row was last sent. Earlier than `last_update_at_utc` when later updates carried only a departure. | Same clock as `last_update_at_utc`. Null when no real update sent an arrival this hour. Absent from partials written before it was added. |
| `n_updates` | `int32` | How many real observations landed for this stop during this hour. Echoes are not counted, so `0` means the stop was listed but never genuinely reported on. | Counted during the reduction. Never null. |
| `schedule_relationship` | `string` | What the feed last said about this stop: `SCHEDULED`, `SKIPPED`, `NO_DATA` or `UNSCHEDULED`. | The latest real observation's value once one exists; before that, whatever was last seen, including `NO_DATA`. Read without first asking whether the field was sent, so a value outside the list, or no value at all, reads as `SCHEDULED`. Set on the stop's first sighting, so never null. |
| `had_vehicle` | `bool` | True if a bus was ever attached to this trip during the hour. | True once any poll carried `trip_update.vehicle`, and it never goes back to false. Never null. |
| `lost_tracking` | `bool` | True when the bus went quiet part-way: a real observation landed, and then only echoes followed. The prediction is frozen at whatever it last said. | Computed at write time: true when `last_observed_at_utc` is later than `last_update_at_utc`. False when neither exists, because a stop that was never reported on was not lost. |
| `last_observed_at_utc` | `timestamp[s, tz=UTC]` | When this stop was last mentioned by the feed at all, echo or not. Bookkeeping for `lost_tracking`. | The latest observation time of any kind. Null only if the row was somehow never observed. **Not carried into `fact_trip_stop`** - it is folded into `lost_tracking` there. |

The two timestamps come from different clocks:
`last_update_at_utc` prefers the feed's own stamp, `last_observed_at_utc`
often falls back to poll time. That does not change which value is last -
see [methodology 13](methodology.md#13-lost_tracking-compares-two-different-clocks).

Trip-level status is not on these rows. It is in
[`curated/_partial/trip/`](#curated_partialtrip).

Partials written before trip status moved out still carry a
`trip_schedule_relationship` column. The merger reads partials by column
name and never carries it into the fact.

### `curated/_partial/trip/`

**One row is one trip on one feed start date, reduced to what that hour's
polls said about the whole trip.**

Written from every trip update, including the ones with no stop-time
updates. That is the only place a `CANCELED` trip appears, because a
cancelled trip update carries no stops. Updates with an empty `trip_id`
(the feed's `UNSCHEDULED` runs) cannot be keyed and are skipped.

Every time on this row is the **poll's fetch time**, since cancelled
updates carry no timestamp of their own.

The key is not unique within one poll. An `ADDED` update can share its
`trip_id` and start date with a `SCHEDULED` one, so a status is counted at
most once per poll, and when one poll reports a trip under more than one
status the final status is chosen by rank - `CANCELED`, then `SCHEDULED`,
then `ADDED`, then anything else - never by message order.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `start_date` | `string` | The feed's start date, as `YYYYMMDD`. | Copied from `trip_update.trip.start_date`. |
| `trip_id` | `string` | Which scheduled run. Joins to `dim_trip.trip_id`. | Copied from `trip_update.trip.trip_id`. Never `''`: such updates are skipped. |
| `route_id` | `string` | Which route. | From the latest poll in the hour. |
| `start_time` | `string` | The feed's start time, `HH:MM:SS`. Never above 23:59:59. | From the latest poll in the hour. |
| `final_status` | `string` | The trip-level status in the hour's latest poll: `SCHEDULED`, `CANCELED`, `ADDED`, ... | `trip_update.trip.schedule_relationship`, by name. Null when never sent. |
| `final_status_at_utc` | `timestamp[s, tz=UTC]` | Fetch time of the poll `final_status` came from. | Null when `final_status` is. |
| `scheduled_polls` | `int32` | Polls this hour that reported the trip `SCHEDULED`. | Counted once per poll. Never null. |
| `canceled_polls` | `int32` | Polls this hour that reported it `CANCELED`. | Counted once per poll. Never null. |
| `added_polls` | `int32` | Polls this hour that reported it `ADDED`. | Counted once per poll. Never null. |
| `first_seen_at_utc` | `timestamp[s, tz=UTC]` | First poll this hour that mentioned the trip at all. | Never null. |
| `last_seen_at_utc` | `timestamp[s, tz=UTC]` | Last poll this hour that mentioned it. | Never null. |
| `first_canceled_at_utc` | `timestamp[s, tz=UTC]` | First poll this hour that reported it `CANCELED`. | Null when none did. |
| `last_canceled_at_utc` | `timestamp[s, tz=UTC]` | Last poll this hour that reported it `CANCELED`. | Null when none did. |
| `had_vehicle` | `bool` | True if any update this hour carried a vehicle. | Never null. |

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
| `service_date` | `date32[day]` | The service day, Sydney local. Stored as a date so it matches the `service_date=` folder it is written under: most query engines expose that folder as a column of the same name, and if the two disagreed a reader would silently get one or the other. | The partial's start date, moved back one day for a trip whose first timetabled time is 24:00 or later, then converted to a date. The timetable used is the one `scheduled_arrival_utc` uses; a trip missing from it keeps its start date. |
| `trip_id` | `string` | Which scheduled run. Joins to `dim_trip.trip_id`. | Carried through. |
| `stop_id` | `string` | Which stop. Joins to `dim_stop.stop_id`. | Carried through. |
| `stop_sequence` | `int32` | Position of this stop in the trip's order. | Carried through. |
| `route_id` | `string` | Which route. Joins to `dim_route.route_id`. | Carried through from the most recent hour's row. |
| `final_predicted_arrival_utc` | `timestamp[us, tz=UTC]` | The day's last real prediction of arrival at this stop. Used as the arrival time. | From the most recent hour that sent an arrival, which can be earlier than the hour the other columns come from. Null when no hour did, or when the last arrival gave a delay but no time. |
| `delay_s` | `int32` | Seconds late on arrival. Negative means early. | Same source row as `final_predicted_arrival_utc`. Null when no arrival was sent all day. |
| `final_predicted_departure_utc` | `timestamp[us, tz=UTC]` | The day's last real prediction of departure. | Same source row. Null when absent. |
| `departure_delay_s` | `int32` | Seconds late departing. Negative means early. | Same source row. Null when absent. |
| `last_update_at_utc` | `timestamp[us, tz=UTC]` | When the day's last real observation was made, arrival or not. | From the most recent hour with a real observation. Null when the stop was only ever echoed. |
| `arrival_updated_at_utc` | `timestamp[us, tz=UTC]` | When the arrival was last sent. How fresh `delay_s` is. | Latest over every hour. Null when no arrival was sent all day. |
| `n_updates` | `int32` | Real observations across the whole day. `0` means the stop was listed but never genuinely reported on. | Summed over every hour, not taken from the last one, so it does not depend on file read order. Never null. |
| `schedule_relationship` | `string` | This stop's status: `SCHEDULED`, `SKIPPED`, `NO_DATA`, `UNSCHEDULED`. | From the most recent hour's row. |
| `had_vehicle` | `bool` | True if a bus was attached to this trip at any point in the day. | True if any hour said so. Never null. |
| `lost_tracking` | `bool` | True when the bus stopped reporting before reaching this stop and only echoes followed. The prediction is stale. | Recomputed for the whole day: true when the day's latest observation of any kind is later than its latest real one. False when there was never a real observation. Never null. |
| `is_reliable` | `bool` | True when the arrival time is worth trusting: a real delay exists, tracking did not drop, the arrival is after 2000 (never an epoch artefact), and the arrival was last sent no more than 60 seconds before the predicted arrival (`arrival_updated_at_utc`, not `last_update_at_utc`, so a later departure-only update does not make a stale arrival fresh). **Headline figures use only rows where this is true.** | Computed. False when any condition fails, including when a delay exists but no predicted arrival time does, leaving nothing to compare against. Never null. The 60-second rule is explained in [methodology 1](methodology.md#1-arrival-times-are-predictions-not-observations). |
| `scheduled_arrival_utc` | `timestamp[us, tz=UTC]` | When the timetable said the bus should arrive. Subtract from `final_predicted_arrival_utc` to get lateness directly. | Joined from `dim_scheduled_stop_time` on `trip_id` and `stop_sequence`, using the latest snapshot whose check's UTC date is on or before this service date, or the earliest snapshot for a day before any was captured. Its `HH:MM:SS` reading, which may exceed 24 hours, is added to Sydney midnight and converted to UTC. Null when no snapshot exists at all, or when the snapshot has no matching row. Which midnight it counts from is settled one way here and is not confirmed against TfNSW - see [methodology 10](methodology.md#10-which-midnight-a-timetable-time-counts-from). |

Days before the first timetable was captured borrow the earliest snapshot,
on the assumption that the timetable did not change in between. That
cannot be checked, because the bundle itself was not archived then. See
[methodology 8](methodology.md#8-the-timetable-only-describes-the-future).

The whole trip's status is not on this table; join
[`fact_trip`](#fact_trip) on `service_date` and `trip_id`. Stop rows of a
trip cancelled part-way through are kept: the stops it reached before the
cancellation are real, and later stops' frozen predictions already fail
`is_reliable`.

A known limit: an `ADDED` trip update sharing a `trip_id` and start date
with a `SCHEDULED` one in the same poll is folded into the same stop rows.

### `fact_trip`

**One row is one trip on one service day, as the feed reported it.** Use
it to tell a cancelled trip from one that ran, or one the feed never
mentioned (absent from this table, present in the timetable).

Written to `curated/fact_trip/service_date=YYYY-MM-DD/data.parquet`.

Status is stored as evidence, not as a verdict. A trip can be cancelled and
later reinstated, and a cancellation can last one poll or several hours, so
the table keeps the final status, the poll counts per status and the span
of the cancellation. Which of these counts as "cancelled" is an analysis
rule, not a pipeline one.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `service_date` | `date32[day]` | The service day, Sydney local. | Derived as for `fact_trip_stop`: the feed's start date, moved back a day for a trip first timetabled at 24:00 or later. |
| `trip_id` | `string` | Which scheduled run. Joins to `dim_trip.trip_id`. | Carried through. |
| `start_date` | `string` | The feed's own start date, `YYYYMMDD`. | Carried through. |
| `start_time` | `string` | The feed's start time. | From the hour with the latest poll. |
| `route_id` | `string` | Which route. | From the hour with the latest poll. |
| `final_status` | `string` | The status in the day's latest poll that sent one. | From the hour with the latest `final_status_at_utc`. Null when never sent. |
| `final_status_at_utc` | `timestamp[us, tz=UTC]` | Fetch time of that poll. | Latest over every hour. |
| `scheduled_polls` | `int32` | Polls in the day reporting `SCHEDULED`. | Summed over every hour. Never null. |
| `canceled_polls` | `int32` | Polls reporting `CANCELED`. Above 0 means the trip was cancelled at some point, whatever its final status. | Summed. Never null. |
| `added_polls` | `int32` | Polls reporting `ADDED`. | Summed. Never null. |
| `first_seen_at_utc` | `timestamp[us, tz=UTC]` | First poll mentioning the trip. | Earliest over every hour. |
| `last_seen_at_utc` | `timestamp[us, tz=UTC]` | Last poll mentioning it. A cancelled trip stays listed until about its scheduled end. | Latest over every hour. |
| `first_canceled_at_utc` | `timestamp[us, tz=UTC]` | First poll reporting `CANCELED`. | Earliest over every hour. Null when never cancelled. |
| `last_canceled_at_utc` | `timestamp[us, tz=UTC]` | Last poll reporting `CANCELED`. | Latest over every hour. Null when never cancelled. |
| `had_vehicle` | `bool` | True if a bus was attached at any point in the day. A reinstated trip with a vehicle ran. | True if any hour said so. Never null. |

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

The eight dimensions are the timetable, unpacked out of the GTFS zip into
one Parquet file each. They keep only some of the zip's columns, so the zip
itself is archived beside them, at
`curated/schedule_bundle/valid_from=YYYY-MM-DDTHHMMSSZ/`, named as the server named
it (`bundle.zip` when it gave no name). A column the dimensions drop can be
recovered from any snapshot archived since this began, but not from the
snapshots before it.

They are snapshots, not a history. The loader downloads the bundle once a
day and writes a new snapshot only when the bundle's contents have changed,
under `valid_from=YYYY-MM-DDTHHMMSSZ`, the UTC time of the check that
noticed the change. Naming by time, not date, keeps both snapshots when the
bundle changes twice in one day. To use the timetable that applied on a
given service day, take the latest `valid_from` whose UTC date is on or
before it, which is what the merge does.

Snapshots written before 24 September 2026 were named by UTC date alone and
were renamed to their check time. On 23 September two changed bundles
shared one date and the later overwrote the earlier, so that day holds only
the 23:09 UTC check's snapshot.

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

### `dim_agency`

**One row is one operator.** From `agency.txt`. Join `dim_route.agency_id`
to it for operator names.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `agency_id` | `string` | The operator's identifier. The join key. | Copied from `agency.txt`. |
| `agency_name` | `string` | The operator's name. | Copied. |
| `agency_url` | `string` | The operator's web address. | Copied. |
| `agency_timezone` | `string` | The operator's timezone. | Copied. |

### `dim_route`

**One row is one route**, such as the 333. From `routes.txt`.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `route_id` | `string` | The identifier the live feeds use for this route. The join key. | Copied from `routes.txt`. |
| `agency_id` | `string` | Which operator runs it. The bundle carries many. Joins to `dim_agency`. | Copied. |
| `route_short_name` | `string` | The number on the front of the bus, e.g. `333`. | Copied. |
| `route_long_name` | `string` | The longer descriptive name, where the bundle gives one. | Copied. |
| `route_type` | `string` | The GTFS code for the kind of service. Kept as text, not converted to a number. Transport for NSW uses the **extended** codes, not the basic ones, so the bus code here is `700` and not `3`. See the table below. | Copied verbatim. |

The extended codes are defined in [Google's extended route types](https://developers.google.com/transit/gtfs/reference/extended-route-types); the basic codes in the [GTFS reference](https://gtfs.org/documentation/schedule/reference/#routestxt) apply only to `4`. Four values appear in the bundle:

| Code | Means | Notes for analysis |
| --- | --- | --- |
| `712` | School bus | The large majority of routes. They run only on term weekdays, at the edges of the peaks, often a couple of trips a day. Counting them alongside all-day routes inflates route counts and mixes two different kinds of service. |
| `700` | Bus service | Ordinary all-day routes. This is the population most reliability questions are about. |
| `714` | Rail replacement bus | A few dozen routes. They run only during trackwork, on no regular pattern, so their headways and punctuality do not describe normal operations. |
| `4` | Ferry | A single row. The bundle is almost, but not quite, all buses. |

`route_type` is the operator's own statement about a route, which makes it
a better filter than route names. Some school runs name the school without
using the word - `Balgowlah Boys High` - and some all-year public routes
terminate at a school and are not school services at all.

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
| `wheelchair_boarding` | `string` | Whether the stop is accessible: `1` means at least some vehicles can be boarded by a wheelchair user, `2` means not. Kept as text. | Copied. Null when the field is blank or the column is absent. |

`wheelchair_boarding` has no third code for "unknown". GTFS uses `0` or a
blank for that, and both arrive here as null, so a null means the bundle
said nothing rather than that the stop is inaccessible. Counting nulls as
inaccessible would overstate the problem considerably.

The column was added after collection began, so snapshots written before
it exists do not carry it. Reading across those snapshots needs
`union_by_name=true`; without it, selecting the column fails with a schema
mismatch rather than returning nulls for the older files. Counting rows
works either way, which is what makes this easy to miss.

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
| `timepoint` | `string` | `1` when the time is an exact timetabled time, `0` when it is approximate or interpolated. | Copied verbatim. Null when blank or absent. Not in snapshots written before this column was kept. |
| `pickup_type` | `string` | Whether passengers can board here: `0` regular, `1` no pickup, `2` phone ahead, `3` arrange with the driver. | Copied verbatim. Null when blank or absent, and in older snapshots. |
| `drop_off_type` | `string` | Whether passengers can alight here, same codes. | Copied verbatim. Null when blank or absent, and in older snapshots. |

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

### `dim_calendar_exclusion`

**One row is one calendar date carrying one reason it is not an ordinary
school-term day.** Unlike every other table here, this one is not derived
from a feed: it is hand-built once a year from published NSW calendars,
committed as `analysis/calendar_exclusions_<year>.csv`, and expanded from
date ranges into individual dates by `analysis/calendar_exclusion.py`.

It exists so that peak-hour comparisons can be restricted to term time.
School holidays change traffic and patronage enough that including them
alongside term weekdays compares two different things.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `date` | `date` | The excluded day, Sydney local. | Expanded from the seed row's `start_date`/`end_date`, which are inclusive of both endpoints. |
| `exclusion_type` | `string` | `public_holiday`, `school_holiday`, or `school_development_day`. | Copied from the seed. |
| `reason` | `string` | The event's published name, such as `Spring holidays`. | Copied from the seed. |
| `source` | `string` | URL the date was transcribed from. | Copied from the seed. |

A date can appear more than once, so the grain is `(date, exclusion_type)`
and never `date` alone. Labour Day falls inside the spring holidays and
carries a row of each kind; joining on `date` without deduplicating will
double-count that day's traffic.

The public holiday rows are transcribed by hand. The data.gov.au holidays
dataset is marked inactive and stops before this project's data begins, and
the current NSW source publishes PDFs and an `.ics` rather than a CSV. The
school dates come from the Department of Education's machine-readable
outlook calendar, which omits Easter Saturday even though NSW observes it.

To check a year's public holidays, look for clusters of
`dim_calendar_dates.exception_type = 2` on dates this table calls ordinary.
A mass service removal on an unexcluded date means a missed holiday.

---

## Reference tables

These describe the places stops are in, rather than anything the buses did.
They are built once from published ABS and OpenStreetMap files, on a laptop
rather than in the pipeline, and written under `reference/` so what a
scheduled job produces stays separable from what a person produced.

They are versioned differently from everything else. A fact row joins to the
timetable that applied on its own service day, but the newest boundaries
describe a place better for every day, including days already collected. So
**the latest vintage wins outright** and there is no point-in-time join. The
partition is the date the build ran; which ABS editions went into it are
columns, because a build can pair 2021 boundaries with a later SEIFA and the
path should not imply the two move together.

Field definitions for the census and SEIFA columns are the ABS's own:
the [SEIFA 2021 release](https://www.abs.gov.au/statistics/people/people-and-communities/socio-economic-indexes-areas-seifa-australia/latest-release),
the [ASGS Edition 3](https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026)
and the [Census DataPacks](https://www.abs.gov.au/census/find-census-data/datapacks).

### `stop_geography`

**One row is one stop, and where it sits.** At
`reference/stop_geography/vintage=YYYY-MM-DD/`.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `stop_id` | `string` | Joins to `dim_stop`. | Copied. |
| `stop_lat`, `stop_lon` | `double` | The coordinates the geography was computed from, not a convenience copy. | From `dim_stop` at build time. |
| `mesh_block_code` | `string` | ABS mesh block, 11 digits. Text, because some begin with a zero. | Point-in-polygon against ASGS. |
| `mesh_block_category` | `string` | `Residential`, `Commercial`, `Parkland`, `Industrial` and so on. | Census mesh block counts. |
| `sa1_code` to `gccsa_name` | `string` | The statistical hierarchy above the mesh block. | Attributes of the mesh block. |
| `lga_code`, `lga_name` | `string` | Local government area. | A separate point-in-polygon; LGA is not a mesh block attribute. |
| `geography_match` | `string` | `inside` when the stop fell within a mesh block, `nearest` when it did not. | How the assignment was made. |
| `geography_match_distance_m` | `double` | Zero when `inside`; the distance to the polygon when `nearest`. | Spherical distance. |
| `boundary_tie` | `boolean` | True when the stop sits exactly on a shared edge and matched more than one block. | Count of matches. |
| `irsd_score`, `irsd_national_decile`, `irsd_state_decile` | `double`, `smallint` | Relative socio-economic disadvantage. Deciles both ways: against Australia, and against NSW alone. | SEIFA 2021 at SA1. |
| `irsad_*`, `ier_*`, `ieo_*` | | The other three SEIFA indexes, same shape. | SEIFA 2021 at SA1. |
| `sa1_usual_resident_population` | `integer` | Residents of the stop's SA1. | SEIFA. Null when the SA1 matched no SEIFA row. |
| `seifa_irsd_excluded` | `boolean` | True when the ABS explicitly excluded this area from the index. **False does not mean the stop was scored**: a stop whose SA1 matched neither the index nor the excluded-areas table is also false. | SEIFA excluded areas table. |
| `distance_to_cbd_m` | `double` | Straight-line distance to the Sydney CBD. | Spherical distance to a committed point. |
| `nearest_centre_id`, `nearest_centre_name`, `distance_to_nearest_centre_m` | | The closest major centre, named. | A committed list of centres. |
| `vintage`, `asgs_edition`, `seifa_release`, `census_year` | | Which build, and from which editions. | Recorded at build time. |

A small share of stops carry **no SEIFA score at all**, for two different
reasons. Among Greater Sydney stops on ordinary bus routes (route type
`700`), measured on the September 2026 build, 1,500 stops (5.7%) have no
score:

- **175 are in areas the ABS excluded** from the index, usually for having
  almost no residents. `seifa_irsd_excluded` is true for these.
- **The other 1,325 have an SA1 that matched no SEIFA row at all.** Their
  `sa1_usual_resident_population` is null too, and `seifa_irsd_excluded` is
  false. By `mesh_block_category` they are mostly industrial (614), parkland
  (423) and commercial (222), but also education (102) and residential (52),
  so "no residents" does not describe all of them.

These stops sit on 489 of 663 routes. Borrowing a surrounding area's score
would attribute residents' characteristics to places the index does not
describe, so the score is left null. Summaries by SEIFA leave these stops
out and report how many were left out; they are never imputed.

Distance to the Sydney CBD describes a monocentric city, which Sydney is
not. `distance_to_nearest_centre_m` exists because a Parramatta stop is not
peripheral merely because Martin Place is far away, and for a Newcastle stop
the distance to Sydney says nothing worth knowing.

### `stop_meshblock`

**One row is one stop and one mesh block near it.** At
`reference/stop_meshblock/vintage=YYYY-MM-DD/`. Every mesh block whose
interior point lies within 1600 m of the stop.

| Column | Type | What it means | Where it comes from |
| --- | --- | --- | --- |
| `stop_id`, `mesh_block_code` | `string` | The pair. Together they are the grain. | |
| `straight_line_distance_m` | `double` | Spherical distance from the stop to a point inside the mesh block. | Computed. |
| `network_distance_m`, `network_duration_s` | `double` | Real walking distance and time along the street network. Null unless `routing_status` is `routed`. | A local OSRM server on an OpenStreetMap extract, foot profile. |
| `routing_status` | `string` | `routed`, `snap_failed`, `unroutable`, or `not_attempted` for pairs deliberately skipped. | Says why a network distance is missing. |
| `snap_distance_m` | `double` | How far the further of the two ends moved to reach the walking network. | Reported by the router. |
| `detour_ratio` | `double` | Walking distance over straight-line distance. | Computed. |
| `person_count`, `dwelling_count` | `integer` | Residents and dwellings of that mesh block. | Census mesh block counts. |
| `mesh_block_category`, `sa1_code`, `area_sqkm` | | What kind of place it is, and how big. | ASGS and the counts. |

This table exists so that **catchment population is a query, not a column**.
"How many people live within 800 m of this stop" is a sum over the rows
inside that distance, and so is any other radius, or a weighting that decays
with distance. Precomputing a few fixed radii instead would answer only the
questions someone thought of first.

`straight_line_distance_m` is not a walk, and the gap is not uniform. Across
a harbour or a motorway the two diverge enormously, and they diverge more in
cul-de-sac outer suburbs than in the gridded inner ones. Measured here, the
median detour is about 1.38 inner and about 1.45 outer, and restricting an
800 m catchment to real walking distance retains around 54% of the
straight-line population inner but only around 48% outer. Using straight-line
distance therefore flatters outer suburbs, in the direction that would
understate the project's headline comparison.

Three cautions on the routed values. Only pairs with residents inside 800 m
were routed, because a mesh block with nobody in it adds to no population
figure and beyond 800 m nothing was asked; everything else reads
`not_attempted`, which is not a failure. A pair that could not be routed
stays null rather than falling back to the straight line, so a catchment
missing people is countable instead of quietly mixing two different
measurements. And mesh block counts are perturbed by the ABS, so a block
reporting nobody is not proof that nobody lives there.

A small share of pairs have a `detour_ratio` below 1.0, which is
geometrically impossible. Both ends of a pair snap independently onto the
walking network, so where the true distance is comparable to that
displacement the measured walk can come out shorter than the straight line.
The share falls away sharply with distance, from roughly two fifths of pairs
under 50 m to about one in ninety at 300-400 m, and the shortfall is bounded
by the snap distance, so it is a property of snapping rather than a defect
and it changes no catchment.

The mesh block is represented by a point guaranteed to lie inside it, not
its centroid: a centroid of a block bent around a bay or a park can fall
outside the block entirely, and routing from the wrong block returns a
confident wrong answer.

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
| `valid_from` | `string` or null | The snapshot partition this check created, as `YYYY-MM-DDTHHMMSSZ`, UTC. Matches the `valid_from=` folder under `curated/dim_*`. | The check time when `changed` is true. **Null when `changed` is false**, because no snapshot was written. Records written before 24 September 2026 hold the UTC date alone (`YYYY-MM-DD`); their folders were since renamed to the check time. |

To find which timetable was in force on a given day, take the largest
`valid_from` whose UTC date is on or before it. That is what the merge does when it resolves
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
