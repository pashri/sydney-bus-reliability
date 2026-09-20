# Sydney bus reliability

[![CI](https://github.com/pashri/sydney-bus-reliability/actions/workflows/ci.yml/badge.svg)](https://github.com/pashri/sydney-bus-reliability/actions/workflows/ci.yml)

Collects Transport for NSW GTFS-Realtime bus feeds and folds them into a
queryable history, working towards measuring how reliable Sydney's buses
are. Collection, compaction, the daily service-day merge and the static
timetable snapshots are all deployed; analysis and marts are not built yet.

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

AWS SAM: four Lambdas, two layers and one S3 bucket.

- `collector`, every minute. Polls vehicle positions every 10 seconds and
  trip updates every 60, and stores the raw protobuf gzipped under `raw/`,
  keyed by actual fetch time
- `compactor`, hourly at 10 past. Decodes one UTC hour of `raw/` into two
  Parquet partials under `curated/_partial/`
- `merger`, 04:00 Australia/Sydney. Folds a whole service day of partials
  into `fact_trip_stop`, `fact_vehicle_position` and `fact_collector_run`.
  It runs at 04:00 rather than midnight because a trip can belong to one
  service day while running as late as 06:00 the next morning
- `schedule_loader`, 12:00 Australia/Sydney. Fetches the static GTFS bundle
  and writes a new `valid_from` snapshot of the seven dimensions only when
  the bundle's content hash changes, since the bundle is forward-looking
  and a missed day's timetable cannot be recovered later
- Two layers: DuckDB for the merger, built locally by `make layer`, and
  the AWS-managed SDK-for-pandas layer supplying pyarrow, pinned by
  version
- Lifecycle: `raw/` expires after 30 days, `curated/_partial/` after 3.
  Everything else is kept
- Python 3.14, managed with uv

## Deploy

    make install
    aws ssm put-parameter \
      --name /sydney-bus-reliability/tfnsw-api-key \
      --value '<key>' --type SecureString --region ap-southeast-2
    AWS_PROFILE=<profile> make deploy ALERT_EMAIL=<address>

`ALERT_EMAIL` is only needed the first time, when the stack has no stored
value for it. After that, `make deploy`.

`make deploy` builds the DuckDB layer first, but only when it is missing or
when `uv.lock` has moved, since the layer is pinned to the DuckDB version
uv resolved. The layer is a build artefact and is not in the repository, so
a fresh clone always builds it once. `make` on its own lists every target.

## Test

    make test     # pytest
    make check    # pytest, every linter, and sam validate

## Re-running a merge

The merger takes two optional event keys, both for re-runs. The scheduled
event carries neither and assembles all three tables for yesterday.

    aws lambda invoke --function-name sydney-bus-reliability-merger \
      --cli-read-timeout 900 --cli-binary-format raw-in-base64-out \
      --payload '{"service_date": "2026-09-18",
                  "tables": ["collector_run"]}' out.json

`--cli-read-timeout` is not optional. The CLI gives up after 60 seconds by
default, while the function has 600, so without it a successful merge looks
like a failed invocation.

`service_date` picks the Sydney service day. `tables` narrows the run to
`collector_run`, `trip_stop` or `vehicle_position`.

Narrowing matters for old days. `trip_stop` and `vehicle_position` are built
from the hourly partials, which expire after 3 days, so a re-run past that
window has nothing to read. The merger refuses rather than writing an empty
table over a good one. `collector_run` is folded from the collector's JSONL,
which is kept for 30 days, so it can be rebuilt long after the partials have
gone.

Run it as a Lambda rather than locally. The function is in the same region
as the bucket, and a day of audit records is about 2,880 small objects -
seconds in region, twenty minutes from another continent.

## Scripts

Three tools under `scripts/`, all run from the repo root as modules so
they can import `src/`:

    uv run python -m scripts.check_collection --date 2026-09-15
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

`check_collection.py` reports one UTC day's collector health from
the live bucket, read-only. It resolves the bucket from `--bucket`, then
`BUCKET_NAME`, then a lookup of the deployed stack's `BucketName`
output (`--stack-name`, default `sydney-bus-reliability`). AWS
credentials come from `--profile` if given, else the standard
credential chain (e.g. `AWS_PROFILE`).

Pass `--memory` to also report the collector Lambda's memory and
duration envelope for that day. This section is sourced from
CloudWatch Logs Insights (`@maxMemoryUsed` and `@duration` on the
REPORT log lines), not from the S3 audit trail, so it needs
`logs:StartQuery`/`logs:GetQueryResults` on the function's log
group and `lambda:GetFunctionConfiguration` on the function. It
reports the observed maximum memory used, the headroom against
the configured memory ceiling (in MB and as a percentage), a
per-10-minute-bin breakdown, the number of invocations seen (so a
partial sample is distinguishable from a full day), and the
maximum observed duration against the configured timeout. It is
opt-in because it is a second, slower data source with its own
IAM requirements, on top of the always-on S3-backed report.
