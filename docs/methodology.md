# Methodology and known limitations

This document states what the curated data can and cannot support, with the
measured number behind each limit. Every figure below traces to
`sydney-bus-reliability-phase-2-SPIKE.md` or to the Phase 2 design, both
outside this repo. Nothing here is asserted from assumption.

## 1. Trip Updates are predictions, not measurements

GTFS-Realtime Trip Updates carry a predicted arrival time, refreshed roughly
every 60 seconds. There is no direct record of when a bus actually reached a
stop — the last prediction issued before the bus passes is used as a proxy
for actual arrival. That proxy is close to the truth when a vehicle keeps
reporting up to the moment of arrival, and wrong when it does not.

Rows whose final update landed more than 60 seconds before the predicted
arrival time get `is_reliable = false` and are excluded from headline
metrics. Every arrival-time figure in this project inherits this proxy and
its exclusion rule.

## 2. `vehicle_id` is a trip-instance token, not a bus

Measured across one hour: 6,920 distinct `vehicle.id` values against 6,919
distinct `trip_id` values, and zero vehicles reported under more than one
`trip_id`. The id is issued per trip, not per physical vehicle, and does not
survive a trip change.

The real identity of a bus is `vehicle_label`, a four-digit fleet number
(e.g. `8183`), corroborated by `license_plate` (`MO8183`). Grouping by
`vehicle_id` to study individual buses — bunching by vehicle, fleet-level
effects — produces a fictional fleet of roughly 6,920 buses per hour instead
of the real fleet. All per-vehicle analysis in this project keys on
`vehicle_label`.

## 3. `NO_DATA` rows echo the static timetable

24.4% of stop-time-updates in the sampled hour carry
`schedule_relationship = NO_DATA`. These rows still populate `arrival.delay`
and `arrival.time`, and the values are not predictions: `arrival.delay` is
identically 0 on 99.906% of them, and `arrival.time` matches the static
scheduled instant to the exact second on 99.86%, with no ±1-second rounding
band. A real prediction has no reason to land on the scheduled second that
precisely — the absence of any rounding noise is the signature of a copy,
not a computation. TfNSW is echoing the timetable back when no vehicle is
assigned to a trip.

Treating these rows as observations injects a fake, perfect, zero-second
delay onto roughly a quarter of the data, which biases any on-time or delay
metric toward looking better than it is. This project nulls `delay_s` and
the predicted-time columns on `NO_DATA` rows rather than dropping them —
"no vehicle was reported for this trip at this time" is itself a fact worth
keeping, and dropping the row would erase it.

## 4. `NO_DATA` is not cancellation

`NO_DATA` and `CANCELED` are different fields describing different
populations. `NO_DATA` is a stop-level value on `StopTimeUpdate` meaning no
realtime timing information is available for that stop. `CANCELED` is a
trip-level value on `TripDescriptor`, measured at 1.56% of trip updates, and
a canceled trip still carries a full array of stop-time updates. Neither
implies the other, and code that reads one to infer the other will be wrong.

## 5. Realtime coverage has a floor of about 3%

Of trips that should be in progress at the moment of a poll, 4.52% report no
vehicle. Excluding trips within 20 minutes of either scheduled endpoint,
where "in progress" is a fuzzier classification, that figure falls to 2.68%.
This is a genuine floor on realtime coverage, not boundary noise from the
in-progress definition, and every coverage-dependent figure in this project
inherits it. Some of the residual is very-late or cancelled-but-unflagged
trips rather than a telematics fault; the spike could not separate the two
causes.

## 6. Coverage is not geographically biased

This is load-bearing for the project's Western Sydney comparison: if
realtime coverage dropped out more in one part of the metro area than
another, a reliability gap could be an artefact of missing data rather than
a real difference in service.

Measured on in-progress trips within the Sydney metro area, bucketed by stop
centroid longitude: west (lon < 151.0) shows 5.20% NO_DATA across 2,904
trips; east (lon ≥ 151.15) shows 4.69% across 3,325 trips. The gap is 0.51
percentage points against a standard error on that difference of about 0.55
points — smaller than its own uncertainty, so it is not distinguishable from
zero.

State the limits of this check plainly. It covers one day at steady state.
It would not catch a bad-day incident where a single operator's AVL drops
out wholesale, because that is exactly the kind of event a single-day
snapshot cannot see.

## 7. The feeds are statewide NSW, not Sydney

Vehicles from Newcastle and Wollongong were observed in the realtime feed,
and the static bundle carries 33 agencies, not one Sydney operator. Sydney
is not a property of the feed; it is defined at analysis time by filtering
on `dim_stop` geography. Anything that skips that filter is analysing the
whole state.

## 8. The static bundle is forward-looking

The static bundle's calendar spans 20260917–20270101, but only 3
`service_id`s are active on the bundle's own generation day, against 98 the
next day. The bundle describes the future from its generation date onward
and cannot reconstruct a day that has already passed. As a direct
consequence, 16–18 September 2026 have realtime data but no schedule to join
it to, because no bundle was captured on or before those dates.

## 9. Unjoinable identifiers are recorded, not repaired

About 0.2% of realtime route ids fail to join to the static bundle. One
case, `_144` (empty agency prefix), is genuinely ambiguous: `144` exists
under two agencies, `2508_144` and `2514_144`, and nothing in the data says
which one a bare `_144` refers to. These rows are kept with a null
dimension reference and a counter in `curation_run`, never guessed at by
fuzzy matching.

## 10. The GTFS time convention is wall-clock, and unverified for TfNSW

`scheduled_instant` is computed by adding the GTFS time offset to local
midnight. The GTFS spec's literal definition is noon minus 12 hours, which
agrees with the wall-clock interpretation except across a daylight-saving
transition. This project has not confirmed which convention TfNSW's feed
actually follows.

The `NO_DATA` echo finding (§3) gives a way to settle it directly: compare
echoed `arrival.time` values against `scheduled_instant` for trips crossing
02:00 on 4 October 2026, the date Sydney moves to daylight saving. If the
two conventions disagree, the echoes will show it.

## 11. Two CloudWatch alarms will produce false positives, by design

`TreatMissingData: breaching` is set deliberately: Lambda publishes no
datapoint at all when a function does not run, so a dead function produces
silence rather than an error metric, and treating silence as breaching is
the only way to catch it. Two consequences follow from that choice.

First, every such alarm fires once at creation, before its function has run
even a single time, and clears itself on the first invocation. This already
happened: `schedule-loader-not-running` alarmed at 15:19 on 19 September and
cleared at 15:22.

Second, a 24-hour alarm window on a 24-hour job is stable except on 4
October, when Sydney's daylight-saving shift moves noon-local by an hour in
UTC terms and can put two runs inside one window and none inside the next.
Expect one more self-clearing email around that date. Neither of these is an
outage.

## 12. Unknown enum values are recorded as null, not coerced

GTFS-Realtime is proto2, where enum fields are closed: a wire value the
compiled bindings do not recognise never reaches the field at all, so it
reads as absent rather than as some known-but-wrong value. The unrecognised
value itself is not preserved anywhere — proto2 does not expose it. Code
that reads these fields uses `HasField` rather than trusting a present
default, because the corresponding default (e.g. `IN_TRANSIT_TO` for
`current_status`) is indistinguishable from a genuine reading unless checked
that way.

## 13. `lost_tracking` compares two different clocks

The `lost_tracking` flag compares an echo's poll time against a real
observation's entity timestamp — two clocks with different behaviour.
Measured entity-timestamp staleness: median 6-9 seconds, p90 15-65 seconds,
max 3,103 seconds. Despite that spread, the flag cannot produce a wrong
final answer in realistic update sequences, because what decides it is
whether the echoes that follow the last real observation are ordered after
it, and they are — the mixing of clocks affects how stale a value looks, not
which value is last.
