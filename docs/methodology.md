# Methodology and known limitations

This document explains what the data in this project can and cannot tell
you, and gives the measurement behind each limit.

Every figure here comes from decoding a day of the collected feeds, which
happened before the pipeline was designed. That order matters: the design
answers what Transport for NSW actually publishes, rather than everything
the GTFS specification would permit a publisher to do. Some questions a
single day cannot settle. Where that is the case, the limit says so.

## Where the data comes from

Transport for NSW publishes two kinds of public transport data, and this
project uses both.

The first is the timetable: a zip file, republished whenever services
change, saying which trips are meant to run, along which route, calling at
which stops and when.

The second is live, published continuously, and comes in two feeds that
answer different questions. Vehicle Positions say where a bus is now; each
tracked bus reports its coordinates roughly every 10 seconds. Trip Updates
say when a bus is expected to arrive; roughly every 60 seconds, each trip
publishes a predicted arrival time for the stops still ahead of it.

This project collects both feeds continuously and joins them to the
timetable, which is how it can ask how late buses actually are.

## Terms used here

A **trip** is one scheduled run of a route at a particular time, like the
07:14 from Parramatta. It is not a bus, and not a route.

A **stop-time update** is one prediction, for one stop, on one trip. When a
stop-time update is marked **`NO_DATA`**, Transport for NSW is saying it has
no live information for that stop. That marker turns out to matter a great
deal, and section 3 is about why.

A **service day** is a transport day rather than a calendar day. A trip
leaving at 00:30 on Saturday belongs to Friday's service day, because
Friday's timetable is the one it runs on.

## The short version

| The limit | What it means when you use the data |
| --- | --- |
| 1. Arrival times are predictions | There is no record of when a bus truly arrived. We use its last prediction as a stand-in. |
| 2. `vehicle_id` is not a bus | Use `vehicle_label` to follow a physical bus. |
| 3. A quarter of rows echo the timetable | These are not observations. We blank their times so they can't flatter the results. |
| 4. `NO_DATA` does not mean cancelled | They are separate fields. Reading one as the other gives wrong answers. |
| 5. About 3% of running trips report nothing | Every coverage figure carries this floor. |
| 6. That gap is not worse in Western Sydney | Checked, because the project's headline comparison depends on it. |
| 7. The feeds cover all of NSW | Sydney is a filter you apply, not something the feed gives you. |
| 8. The timetable only looks forward | Days before collection began cannot be reconstructed. |
| 9. Unmatched ids are kept, not guessed | A small number of rows have no route attached, on purpose. |
| 10. One timezone convention is unverified | It only matters on the day daylight saving starts. |
| 11. Two alarms cry wolf by design | Expect a self-clearing alert on 4 October. |
| 12. Unknown codes read as missing | Absent means "not sent or not recognised". |
| 13. `lost_tracking` mixes two clocks | Checked, and it cannot change the answer. |
| 14. The holiday list is hand-built | Checked against the timetable's own cancellations; nothing missing in range. |

---

## What the numbers actually measure

### 1. Arrival times are predictions, not observations

Transport for NSW never publishes "the bus arrived at 08:14". What it
publishes is a stream of predictions, refreshed roughly every 60 seconds,
each saying when it currently expects the bus to get there. The arrival
itself is never reported.

So this project takes the last prediction issued before the bus passed the
stop and treats it as the arrival time. How good that stand-in is depends
entirely on when the bus stopped talking: one reporting right up to the kerb
gives a prediction worth trusting, while one that goes quiet five minutes
out leaves a guess frozen at whatever it last said.

To keep that from quietly corrupting the results, any row whose final update
arrived more than 60 seconds before the predicted arrival is marked
`is_reliable = false` and left out of headline figures. Every arrival-time
number in this project depends on this stand-in and this exclusion rule.

### 2. `vehicle_id` identifies a trip, not a bus

This one is a trap, because the field name suggests otherwise.

Across one measured hour there were 6,920 distinct `vehicle.id` values and
6,919 distinct trips, and no vehicle id ever appeared on more than one trip.
The id is issued fresh for each trip and does not survive when a bus starts
its next one.

The real identity of a bus is `vehicle_label`, a four-digit fleet number such
as `8183`, backed up by its number plate (`MO8183`). Anyone grouping by
`vehicle_id` to study individual buses - looking at bunching, or how one
vehicle performs across a day - would be studying an imaginary fleet of
about 6,920 buses an hour instead of the few thousand that exist. All
per-vehicle analysis here uses `vehicle_label`.

### 3. A quarter of the rows are the timetable echoed back

When no bus is assigned to a trip, Transport for NSW does not go quiet. It
sends back the scheduled time, marked `NO_DATA`, in the same shape as a real
prediction.

In the sampled hour, 24.4% of stop-time updates were `NO_DATA`. They still
carried a delay and an arrival time, and those values give the copying away:

- the delay was exactly 0 on 99.906% of them
- the arrival time matched the timetable to the exact second on 99.86%, with
  no one-second scatter either side

That second figure is the giveaway. A real calculation produces rounding
noise, and a genuine prediction has no reason to land precisely on the
scheduled second. There is no noise at all.

Treating these rows as observations would inject a perfect, zero-second
delay into roughly a quarter of the data, and punctuality would look far
better than it is. So this project blanks the delay and predicted-time
columns on `NO_DATA` rows. It keeps the rows themselves, because "no bus was
reported here at this time" is worth knowing.

### 10. Which midnight a timetable time counts from

Timetables express times as an offset into the service day, and services
running past midnight use hours past 24 - a trip at `25:10:00` leaves at
1:10 am the next morning.

This project converts those times by adding the offset to local midnight.
The GTFS specification defines it slightly differently, as noon minus twelve
hours. On any ordinary day the two agree exactly; they part company only
across a daylight-saving transition, where they land an hour apart. Which
one Transport for NSW follows has not been confirmed.

There is a clean way to settle it, using the echo finding above: compare the
echoed arrival times against our own calculation for trips crossing 02:00 on
4 October 2026, when Sydney moves to daylight saving. If the conventions
disagree, the echoes will show it.

### 13. `lost_tracking` compares two different clocks

The `lost_tracking` flag works out whether a bus stopped reporting partway
through its trip, and working that out means comparing an echo's poll time
against a real observation's own timestamp. Those two clocks behave
differently. Measured staleness of the entity timestamp: median 6-9 seconds,
90th percentile 15-65 seconds, maximum 3,103 seconds.

That spread cannot change the answer, though. What decides the flag is
whether the echoes that follow the last real observation are ordered after
it, and they always are. Mixing the clocks changes how stale a value looks,
never which value is last.

### 14. The holiday list is transcribed by hand, and was checked

Peak-hour comparisons are restricted to school-term weekdays, because
holiday traffic and term traffic are different things. Which days those are
comes from `dim_calendar_exclusion`, a list typed out once a year from
published NSW calendars rather than pulled from a feed. The obvious failure
is a forgotten public holiday: it would pass silently into the results as an
ordinary weekday with strangely light traffic.

The timetable can referee this. It records days on which a service does not
run, so a public holiday shows up as a cluster of removals. Checked against
the bundle published on 22 September 2026:

- No date the list calls ordinary has an unusual number of removals. Nothing
  is missing.
- School-holiday weekdays carry roughly three times the removals of an
  ordinary weekday.
- Labour Day has more removals than any other day in the bundle - a public
  holiday falling inside the school holidays.
- The staff development day that opens term 4 barely registers, a little
  above an ordinary weekday. It is excluded anyway, because student travel
  is what the comparison measures, but it is the one entry the timetable
  does not strongly support.
- 29 December, a holiday for the NSW public service but not a general public
  holiday, shows ordinary service. It is deliberately not on the list.

Two limits on that check. The timetable only looks forward (section 8), so
this bundle can only referee dates from late September onwards - the summer
and autumn holidays and every public holiday before spring are unverified,
and would need a bundle published earlier in the year. And the check finds
missing exclusions, not spurious ones: a day wrongly marked as a holiday
removes real data without leaving a trace of having done so.

One entry deserves naming because a plausible source gets it wrong. NSW
observes Easter Saturday as a full public holiday, and the Department of
Education's machine-readable calendar omits it. Public holidays are
therefore transcribed from the NSW government's own list, and only the
school term dates come from the department's file.

---

## What the data cannot tell you

### 4. `NO_DATA` does not mean cancelled

These are two different fields describing two different things, and
conflating them is an easy mistake to make.

`NO_DATA` sits on an individual stop. It means there is no live timing for
that stop. `CANCELED` sits on the whole trip and was measured on 1.56% of trip
updates. Neither implies the other. Any code that reads one to infer the
other will be wrong.

A `CANCELED` update carries only the trip descriptor. Across every poll for
service day 22 September 2026, none of 149,301 `CANCELED` updates had a
stop-time update, a vehicle or a timestamp. So cancellations never appear in
`fact_trip_stop`; they are recorded in `fact_trip`.

Cancellation is not final. That day 511 trips were `CANCELED` at some point
and 21 flipped back at least once; 19 ended `SCHEDULED`, and 18 of those then
carried a vehicle, so they ran. Some cancellations lasted a poll or two,
others several hours. Just over half of the trips that went from `SCHEDULED`
to `CANCELED` did so after their scheduled start, many with a bus already
attached. `fact_trip` keeps the evidence - final status, polls per status,
first and last cancelled poll - and which of those counts as a cancellation
is an analysis rule.

### 5. About 3% of running trips report no bus at all

Of trips that should have been underway at the moment of a poll, 4.52%
reported no vehicle. Restricting that to trips well clear of their start and
end - more than 20 minutes from either, where "underway" is unambiguous -
gives 2.68%.

That 2.68% is a real floor on live coverage, not an artefact of where the
boundary was drawn, and every coverage-dependent figure in this project
inherits it. Some of it will be very late or quietly cancelled trips rather
than a tracking fault. The measurement cannot separate the two.

### 6. The coverage gap is not worse in one part of Sydney

This one is load-bearing. The project's headline question compares Western
Sydney with the east, so if buses in the west went untracked more often, a
reliability gap could simply be missing data wearing a disguise.

Measured on trips underway inside the Sydney metro area, split by stop
longitude: the west (below 151.0) showed 5.20% `NO_DATA` across 2,904 trips,
the east (151.15 and above) 4.69% across 3,325 trips. The difference is 0.51
percentage points, against an uncertainty on that difference of about 0.55
points. The gap is smaller than its own margin of error, so it cannot be
told apart from zero.

The limits of that check are worth stating. It covers one ordinary day. It
would not catch a bad-day incident where one operator's tracking fails
wholesale, because a single-day snapshot cannot see events like that.

### 7. The feeds cover the whole state, not just Sydney

Buses from Newcastle and Wollongong turn up in the live feed, and the
timetable covers 33 separate operators rather than one Sydney agency.

"Sydney" is not something the feed hands you. It is a filter applied at
analysis time using stop locations. Any analysis that skips that filter is
quietly reporting on all of New South Wales.

### 8. The timetable only describes the future

The timetable file looks forward from the moment it is published. Its
calendar ran from 17 September 2026 to 1 January 2027, but only 3 services
were active on the day it was generated, against 98 the following day.

It therefore cannot reconstruct a day that has already gone. That has a
direct consequence for this project: no timetable file was captured on or
before 16 to 18 September 2026. Those days borrow the earliest snapshot,
from 19 September, on the assumption that the timetable did not change in
between. The assumption cannot be checked, because the bundle itself was not
archived until later.

### 9. Identifiers that don't match are recorded, not repaired

About 0.2% of route ids in the live feed do not match anything in the
timetable. One of them, `_144`, is genuinely unresolvable: the route number
`144` exists under two different operators, `2508_144` and `2514_144`, and
nothing in the data says which one a bare `_144` means.

Rows like this are kept with an empty route reference and counted in the
audit record. They are never resolved by guessing at a close match, because
a plausible wrong answer is worse than a visible gap.

---

## Notes for running the pipeline

### 11. Two alarms raise false alerts on purpose

The alarms that watch for a function failing to run are configured to treat
silence as a problem. This is deliberate: when a scheduled function does not
run, it reports nothing at all rather than reporting an error, so silence is
the only symptom there is.

Two harmless consequences follow.

A new alarm of this kind fires the moment it is created, before its function
has had a chance to run, and clears itself on the first run. It counts
invocations rather than successes, so a run that fails clears it just as
readily as one that works - a function that runs and fails is what the
separate error alarms are for. This already happened:
`schedule-loader-not-running` alerted at 15:19 on 19 September and cleared
at 15:22.

A 24-hour alarm watching a once-a-day job is also stable except on 4
October, when the switch to daylight saving shifts the run by an hour and
can place two runs in one window and none in the next. Expect one more
self-clearing email around that date. Neither case is an outage.

### 12. Codes we don't recognise arrive as missing

The live feeds use a format (protocol buffers, version 2) where each coded
field has a fixed list of permitted values. If Transport for NSW ever sends
a value outside that list, it does not arrive as a wrong-but-valid code - it
does not arrive at all, and the field reads as empty. The unrecognised value
itself is not preserved anywhere.

This is why most coded fields are read by first asking whether the field was
sent, rather than reading it directly. Several of them have a default that
looks like a real answer - `current_status` defaults to "in transit to" -
and a default is indistinguishable from a genuine reading unless you check.

Two things that check does not cover. Identifiers are read directly, so a
missing one arrives as an empty string rather than as absent. And one coded
field is read directly too: a stop's `schedule_relationship`, which reads as
`SCHEDULED` when the value is unrecognised or was never sent. That is
exactly the confusion the check exists to avoid, and it is worth knowing
before treating a `SCHEDULED` stop as something the feed said.
