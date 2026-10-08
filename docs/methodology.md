# Methodology and known limitations

This document explains what the data in this project can and cannot tell
you, and gives the measurement behind each limit.

Most figures here come from decoding a day of the collected feeds, which
happened before the pipeline was designed. That order matters: the design
answers what Transport for NSW actually publishes, rather than everything
the GTFS specification would permit a publisher to do. Some questions a
single day cannot settle. Where that is the case, the limit says so. Later
sections name the days they were measured on.

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
| 9. Unmatched ids are kept, not guessed | A small number of rows carry a route id that matches no route, on purpose. |
| 10. A clock change moves a timetable's midnight | TfNSW starts each trip on the wall clock. On 3-4 October 2026 many night buses ran an hour behind it. |
| 11. The not-running alarms can cry wolf | A new alarm fires once before its function first runs. |
| 12. Unknown codes read as missing | Absent means "not sent or not recognised". |
| 13. `lost_tracking` mixes two clocks | Checked, and it cannot change the answer. |
| 14. The holiday list is hand-built | Checked against the timetable's own cancellations; nothing missing in range. |
| 15. A trip's status is a rule, not a record | `mart_trip` applies one; its evidence columns support others. |
| 16. A bad day must be caught within a week | The merger flags it; after 7 days nothing can rebuild it. |

---

## What the numbers actually measure

### 1. Arrival times are predictions, not observations

Transport for NSW never publishes "the bus arrived at 08:14". What it
publishes is a stream of predictions, refreshed roughly every 60 seconds,
each saying when it currently expects the bus to get there. The arrival
itself is never reported.

So this project takes the last prediction issued before the bus passed the
stop and treats it as what happened. At a stop where people board, that is
the predicted departure; at the last stop and at set-down-only stops, the
predicted arrival. How good that stand-in is depends entirely on when the
bus stopped talking: one reporting right up to the kerb gives a prediction
worth trusting, while one that goes quiet five minutes out leaves a guess
frozen at whatever it last said.

To keep that from quietly corrupting the results, a call is judged only if
its time was last updated no more than 60 seconds before it. Away from the
first stop that is `is_reliable`, which tests the arrival's update time; at
the first stop the feed blanks the arrival once the bus leaves, so the
departure's own update time is tested instead. Calls that fail are left out
of on-time figures. Every number in this project depends on this stand-in
and this exclusion rule.

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

The GTFS specification counts that offset from noon minus twelve hours on
the service day. Read literally, that is elapsed time, and across a
daylight-saving change it lands an hour away from the wall-clock reading,
midnight plus the offset. On any other day the two agree exactly.

The 4 October 2026 changeover showed what Transport for NSW does, using the
timetable times the feed echoes back for trips with no bus assigned
(section 3). Its realtime system reads each trip's first departure on the
wall clock, then keeps the trip's own spacing in elapsed time. Trips that
started after the change matched the wall-clock reading to the second.
Trips that started before it and ran across it matched elapsed time, and a
trip timetabled in the skipped hour started an hour later on the clock.
Neither reading alone fits every trip. The marts follow TfNSW's rule.
`fact_trip_stop.scheduled_arrival_utc`, as the merger stores it, reads every
call on the wall clock, so it differs from TfNSW only on the later calls of
a trip that ran across the change.

That night, many buses ran about an hour behind that timetable. For most
calls after 03:00, TfNSW's own feed reported a delay of close to an hour,
concentrated in a few agencies, and on the morning of 4 October part of the
feed at some agencies was still an hour late until about midday. The data
does not say why; operators working to the old clock would explain both.
They are kept as measured: by TfNSW's timetable those buses were an hour
late. The next change falls in April 2027, after collection ends.

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
comes from the `calendar_exclusion` and `term_weekday` views in
`analysis/marts.sql`, expanded from a list typed out once a year from
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
direct consequence for this project: no timetable was captured before 03:34
on 20 September 2026, Sydney time. Service days 16 to 19 September borrow
that earliest snapshot, on the assumption that the timetable did not change
in between. The assumption cannot be checked, because the bundle itself was not
archived until later.

### 9. Identifiers that don't match are recorded, not repaired

About 0.2% of route ids in the live feed do not match anything in the
timetable. One of them, `_144`, is genuinely unresolvable: the route number
`144` exists under two different operators, `2508_144` and `2514_144`, and
nothing in the data says which one a bare `_144` means.

Rows like this keep the feed's id, which simply fails to join `dim_route`.
They are not yet counted anywhere: the audit record's unjoined-id counters
are always zero. They are never resolved by guessing at a close match,
because a plausible wrong answer is worse than a visible gap. The marts take
a call's route from the timetable's trip, not from the feed, so an
unmatched feed route id does not affect them.

---

### 15. A trip's status is a rule, not a record

`fact_trip` stores what the feed said about a trip. `mart_trip` in
`analysis/marts.sql` turns that into one status, in this order:

- **cancelled**: final status `CANCELED`, and no call on the trip was
  judged (a fresh predicted time; see `call_observation`).
- **incomplete**: final status `CANCELED` after judged calls, or judged at
  the first stop and then not judged for more than the last ten minutes of
  scheduled running.
- **ran**: any judged call.
- **unknown**: none of the above, including trips the feed never
  mentioned.

The rule was set from service days 17-23 September 2026, on ordinary
routes (type `700` less rail replacement):

- **Final status, not "ever cancelled".** 179 of 2,821 trips cancelled at
  some point ended `SCHEDULED`, and 160 of those carried a vehicle and
  produced reliable stop times: they ran. Counting any cancellation would
  add them. `ever_canceled` keeps the evidence for that sensitivity run.
- **Partial runs.** 210 trips that ended `CANCELED` had reliable stop times,
  every one of them before the cancellation. They ran part of the way, so
  they count as incomplete, as TfNSW's SD7 measure would count them.
- **Why ten minutes.** Only about 71% of last-stop arrivals pass the
  60-second freshness test, because the terminus arrival is often last
  updated minutes ahead and tracking often drops as the bus arrives. "Judged
  to the last stop" would call over a quarter of trips incomplete. On
  22 September, of trips judged at the first stop, 30,410 were judged to
  within five minutes of their scheduled end and a further 1,051 to within
  ten, mostly one to three stops short. The 504 beyond ten minutes were
  on average 7 to 28 stops short, which is tracking genuinely lost.
  `unjudged_tail_s` is kept so any other threshold is a query.

Resulting daily shares on weekdays: cancelled 1.0-1.2%, incomplete about
1.4%, unknown 2-3%. An untracked trip that ran cannot be told from a silent
cancellation; both read unknown, which is the floor described in
[section 5](#5-about-3-of-running-trips-report-no-bus-at-all).

Two feed artefacts are handled elsewhere. As a bus leaves its first stop
between midnight and about 05:10, TfNSW sometimes sends one poll with every
stop time exactly a day late and the first stop's departure copied from the
timetable. Over 17-23 September that left 205 day-late times in
`fact_trip_stop`, 182 of them first-stop departures. Since 24 September the
compactor discards such an update whole, and 17-25 September were replayed
from raw under that rule; the marts still treat a predicted time more than
six hours from the schedule as no prediction, as a guard. And the feed's
`delay_s` is measured against scheduled departure where the timetable has a
dwell, so the marts compute every delay from the timestamps instead.

## Notes for running the pipeline

### 11. The not-running alarms can raise false alerts

The four alarms that watch for a function failing to run (the collector,
compactor, schedule loader and merger) are configured to treat silence as a
problem. This is deliberate: when a scheduled function does not
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

A 24-hour alarm watching a once-a-day job could in principle misfire on a
daylight-saving change, which shifts the run by an hour and can place two
runs in one window and none in the next. Across the 4 October 2026 change
neither 24-hour alarm fired. Neither case would be an outage.

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

### 16. A bad day must be caught within a week

After merging a service day, the merger checks it for anomalies: times
12 hours or more from the timetable, times before 2000, repeated keys, trip
rows that contradict themselves, too many stop rows with no scheduled time,
too few reliable ones, and too few trips. A failed check is logged by name
and fires the `merger-anomalies` alarm. The check runs only when one merge
writes both `fact_trip_stop` and `fact_trip`.

The window to act is short. A day can be re-merged from its hourly partials
for 3 days. After that it can only be rebuilt from raw, which is kept for
7 days. Past a week, a day stays as it was merged: a later fix to the
compactor or merger reaches only the days still in raw. Before the
retention was cut from 30 days to 7 in October 2026, 17-25 September were
replayed from raw under the current compactor and merger, later days were
merged by them directly, and 17 September to 5 October were checked
through the marts.
