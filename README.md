# Sydney bus reliability

[![CI](https://github.com/pashri/sydney-bus-reliability/actions/workflows/ci.yml/badge.svg)](https://github.com/pashri/sydney-bus-reliability/actions/workflows/ci.yml)

Collects Transport for NSW GTFS-Realtime bus feeds and folds them into a
queryable history, working towards measuring how reliable Sydney's buses
are. Collection, compaction, the daily service-day merge and the static
timetable snapshots are all deployed, and the marts are queryable locally
(see [Querying the data](#querying-the-data)).

Contains public sector information licensed under the Creative Commons
Attribution 4.0 licence. Data source: Transport for NSW.

Arrival times come from GTFS-Realtime Trip Updates, which carry
predictions, not observations. The last prediction before a bus passes a
stop is used as a proxy for actual arrival - a good proxy when a vehicle
keeps reporting up to that moment, unreliable when it drops out early. See
[`docs/methodology.md`](docs/methodology.md) for this and the other measured
limits.

[`docs/data-dictionary.md`](docs/data-dictionary.md) describes every stored
table, column by column, and assumes no prior knowledge of GTFS.

## Stack

AWS SAM: five Lambdas, two layers and one S3 bucket.

- `collector`, every minute. Polls vehicle positions every 10 seconds and
  trip updates every 60, and stores the raw protobuf gzipped under `raw/`,
  keyed by actual fetch time
- `compactor`, hourly at 10 past. Decodes one UTC hour of `raw/` into three
  Parquet partials under `curated/_partial/`
- `merger`, 08:00 Australia/Sydney. Folds a whole service day of partials
  into `fact_trip_stop`, `fact_trip`, `fact_vehicle_position` and
  `fact_collector_run`.
  Not midnight, and not 04:00 either: a trip can belong to one service day
  while running as late as 06:00 the next morning, so the window it reads
  closes at 07:00 and it has to run after that.
  Once `fact_trip_stop` and `fact_trip` are written it checks the day for
  things a correct day never has: times half a day off the schedule or
  before 2000, duplicate keys, self-contradicting trip rows, too few trips,
  or a collapse in scheduled or reliable rows. Any breach raises the
  `merger-anomalies` alarm
- `schedule_loader`, 12:00 Australia/Sydney. Fetches the static GTFS bundle
  and writes a new snapshot of the eight dimensions, named for the check time, and archives the zip itself, only when
  the bundle's content hash changes, since the bundle is forward-looking
  and a missed day's timetable cannot be recovered later
- `checker`, on demand only. Reads the curated layer and reports the
  collector's polling and the curation functions' runs for a window of
  Sydney days. It writes nothing, decides nothing, and has no schedule,
  so it carries no silent alarm: no invocations is its normal state
- Two layers: DuckDB for the merger and the checker, built by
  `sam build` through
  `layers/duckdb/Makefile`, and the AWS-managed SDK-for-pandas layer
  supplying pyarrow, pinned by version
- Lifecycle: `raw/` expires after 7 days, `curated/collector_run/` after
  30 once the merger has folded it into `fact_collector_run`, and
  `curated/_partial/` after 3. Everything else is kept
- Python 3.14, managed with uv

## Deploy

    uv sync
    aws ssm put-parameter \
      --name /sydney-bus-reliability/tfnsw-api-key \
      --value '<key>' --type SecureString --region ap-southeast-2
    AWS_PROFILE=<profile> ALERT_EMAIL=<address> ./deploy.sh

`ALERT_EMAIL` is only needed the first time, when the stack has no stored
value for it. After that, `AWS_PROFILE=<profile> ./deploy.sh`. Pass
`--no-deploy` to validate and build without deploying.

`sam build` builds the DuckDB layer itself, through `layers/duckdb/Makefile`,
because the layer declares `BuildMethod: makefile`. There is no separate
layer step, and nothing to build by hand on a fresh clone.

## Test

    uv run pytest

## Querying the data

The data is queried locally, in two steps:

1. **Download.** `scripts.pull_curated` copies the curated tables from S3
   into `build/data/` as Parquet files.
2. **Build the marts.** `analysis.marts` writes a DuckDB file,
   `build/marts.duckdb`, holding the marts: views defined in
   [`analysis/marts.sql`](analysis/marts.sql).

The marts are views, not tables, so they hold no data of their own. Each
query reads the downloaded Parquet files again, so it sees whatever the
last download fetched. Nothing is read from S3 at query time.

### Downloading and building

From the repo root, with a profile that can read the bucket:

    uv run python -m scripts.pull_curated build/data --profile <profile>
    uv run python -m analysis.marts build/data build/marts.duckdb

The download mirrors the bucket. It fetches new and changed files,
including days the merger has rewritten, and deletes local files whose
key has gone from S3. After the first run it fetches only what changed.
Run it again to bring in new days. The views see them without a rebuild.

Rebuild the DuckDB file after any change to `analysis/marts.sql`, because
the file stores the SQL as it was when built. The build deletes the file
and writes it fresh, so keep nothing else in it. The views name the
downloaded files by absolute path, so moving `build/data/` breaks them
until the next build.

### The views

- `mart_trip`: one row per scheduled trip per service day, with its
  status (`ran`, `incomplete`, `cancelled` or `unknown`)
- `mart_stop_hour`: one row per stop, route, direction, service day and
  hour, with call counts, on-time counts and headway sums
- `route_league`: routes ranked by period, on term weekdays only
- `coverage`: scheduled calls per SA2, period and day type, including
  zero

The other views are their inputs. `describe mart_trip` lists a view's
columns, and the comment above each view in `analysis/marts.sql` says
what its columns mean.

### Command line

Install the DuckDB CLI with `brew install duckdb`. It must be the same
version as the `duckdb` Python package that built the file, or newer:

    uv run python -c 'import duckdb; print(duckdb.__version__)'
    duckdb --version

Open the file read-only, so other sessions can open it at the same time:

    duckdb -readonly build/marts.duckdb

At the `D` prompt, end each statement with `;`:

    .tables
    .timer on
    select service_date, count(*) as trips,
           round(100 * avg((status = 'ran')::int), 1) as pct_ran
    from mart_trip
    group by service_date
    order by service_date;
    .exit

For a single query, pass it with `-c`:

    duckdb -readonly build/marts.duckdb -c "describe route_league"

`COPY` writes a result to a file, even in a read-only session:

    copy (select * from route_league) to 'route_league.csv';

`stop_geography_stale` uses the spatial extension, which the CLI does not
load by itself. Install it once per DuckDB version, then load it in each
session, or add `LOAD spatial;` to `~/.duckdbrc` to load it every time:

    INSTALL spatial;
    LOAD spatial;

### From an application

Any DuckDB client can open the file: Python, R, Node.js, Java through
JDBC (which also covers GUI tools such as DBeaver), Go and Rust among
them. Open it read-only. DuckDB lets many processes read a file at once,
or one process write to it, but not both. The client must run on the
machine that holds `build/data/`, because the views read it by path.

From Python in this repo:

    import duckdb

    con = duckdb.connect('build/marts.duckdb', read_only=True)
    rows = con.sql('select * from route_league limit 10').fetchall()

`.arrow()` returns a pyarrow table instead, and `.df()` a pandas data
frame where pandas is installed. `analysis.marts.connect` skips the build
step: it creates every view in memory from the current
`analysis/marts.sql`, with the spatial extension loaded:

    from pathlib import Path
    from analysis.marts import connect

    con = connect(root=Path('build/data'))

### Speed and frozen copies

Every query recomputes from the Parquet files. A day-by-day count over
`mart_trip` takes a few seconds, and `route_league` about half a minute.
To keep a result fixed, or to query it many times, copy it into a table
in a separate DuckDB file. Open that file, attach the marts read-only,
and copy:

    duckdb build/frozen.duckdb

    attach 'build/marts.duckdb' as marts (read_only);
    create table route_league_2026_10_08 as
        select * from marts.route_league;

The copy does not change when the data is downloaded again, and reading it
takes milliseconds.

## Replaying from raw

A fix to the compactor or merger only reaches past days by rebuilding them
from `raw/`, which is kept for 7 days:

    PYTHONPATH=src uv run python -m scripts.replay \
      --from 2026-09-17 --to 2026-09-22 \
      --profile pashri-admin --log replay.jsonl

It compacts every raw hour the chosen service days read, three at a time,
then merges each day, through the deployed Lambdas. `--to` defaults to the
latest day whose merge window has closed. With `--log`, a rerun skips the
steps already finished. Merge within 3 days of compacting, before the
partials expire.

## Re-running a merge

The merger takes two optional event keys, both for re-runs. The scheduled
event carries neither and assembles every table for yesterday.

    aws lambda invoke --function-name sydney-bus-reliability-merger \
      --cli-read-timeout 900 --cli-binary-format raw-in-base64-out \
      --payload '{"service_date": "2026-09-18",
                  "tables": ["collector_run"]}' out.json

`--cli-read-timeout` is not optional. The CLI gives up after 60 seconds by
default, while the function has 600, so without it a successful merge looks
like a failed invocation.

`service_date` picks the Sydney service day. `tables` narrows the run to
`collector_run`, `trip`, `trip_stop` or `vehicle_position`.

Narrowing matters for old days. `trip`, `trip_stop` and `vehicle_position` are built
from the hourly partials, which expire after 3 days, so a re-run past that
window has nothing to read. The merger refuses rather than writing an empty
table over a good one. `collector_run` is folded from the collector's JSONL,
which is kept for 30 days, so it can be rebuilt long after the partials have
gone.

When `merger-anomalies` fires, the merger's log for that run has an
`Anomaly` line per failed check, naming it with its value and limit, and a
`Day checked` line with every measure. Fix the cause, deploy, and re-merge
the day with `trip` and `trip_stop` both named (the check runs only when
both are merged) while its partials remain; past three days the fix needs
`scripts.replay` from raw.

Run it as a Lambda rather than locally. The function is in the same region
as the bucket, and a day of audit records is about 2,880 small objects -
seconds in region, twenty minutes from another continent.

## Scripts

Three tools under `scripts/`, all run from the repo root as modules so
they can import `src/`:

    uv run python -m scripts.check_pipeline --date 2026-09-15
    uv run python -m scripts.verify_feeds
    uv run python -m scripts.check_layer_parity

`check_layer_parity.py` asserts the pyarrow inside the pinned AWS layer
matches the version `uv.lock` resolves. CI runs it. Bumping
`PyarrowLayerArn` in `template.yaml` means bumping the pin in this script
too, or a Parquet file written in production can differ from one written in
a test with nothing to show for it.

`verify_feeds.py` fetches both live feeds once and prints their status,
size and entity count. It predates the stack and needs `TFNSW_API_KEY` in
the environment.

`check_pipeline.py` invokes the `checker` Lambda and prints what it
returns. The work happens in region: a day of audit records is about
2,880 small objects, which is seconds from the bucket and twenty
minutes from another continent. The script only formats the figures.

It resolves the function from `--function-name`, then the deployed
stack's `CheckerFunctionName` output (`--stack-name`, default
`sydney-bus-reliability`). Credentials come from `--profile` if given,
else the standard chain (e.g. `AWS_PROFILE`).

`--date` sets the last Sydney day reported, defaulting to yesterday.
`--collection-days` and `--curation-days` widen either half, defaulting
to 1 and 14. The windows differ because the questions do: a missed
minute is a fact about one day, while an unjoined-trip rate means
nothing without the days either side of it.

The collection half reads `fact_collector_run` for a day the merger has
finished and the raw JSONL for one it has not, cutting both to the same
Sydney bounds, and says which it used. The curation half reports, per
job per day, how many runs happened against how many were owed, which
partitions are missing, the row and duplicate counters, peak memory and
errors. Expected compactor runs come from the day's real length, so a
23- or 25-hour day across a daylight-saving transition does not read as
a missing hour.
