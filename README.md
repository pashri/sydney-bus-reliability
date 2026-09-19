# Sydney bus reliability

[![CI](https://github.com/pashri/sydney-bus-reliability/actions/workflows/ci.yml/badge.svg)](https://github.com/pashri/sydney-bus-reliability/actions/workflows/ci.yml)

Collects Transport for NSW GTFS-Realtime bus feeds, working towards
measuring how reliable Sydney's buses are. Phase 2 in progress: the
`schedule_loader` Lambda captures the daily static GTFS bundle —
compaction, analysis and marts are not built yet.

Contains public sector information licensed under the Creative Commons
Attribution 4.0 licence. Data source: Transport for NSW.

Arrival times come from GTFS-Realtime Trip Updates, which carry
predictions, not observations. The last prediction before a bus passes a
stop is used as a proxy for actual arrival — a good proxy when a vehicle
keeps reporting up to that moment, unreliable when it drops out early. See
[`docs/methodology.md`](docs/methodology.md) for this and the other measured
limits.

## Stack

- AWS SAM: one collector Lambda on a 1-minute schedule, one schedule
  loader Lambda on a daily schedule, one S3 bucket
- Vehicle positions polled every 10 seconds, trip updates every 60
- Raw protobuf stored gzipped, keyed by actual fetch time, expired after
  30 days
- Static GTFS bundle fetched daily at 03:00 Australia/Sydney; a new
  `valid_from` snapshot is written under `curated/` only when its
  content hash changes, since the bundle is forward-looking and a
  missed day's timetable cannot be recovered later
- Python 3.14, managed with uv

## Deploy

    uv sync
    aws ssm put-parameter \
      --name /sydney-bus-reliability/tfnsw-api-key \
      --value '<key>' --type SecureString --region ap-southeast-2
    uv export --no-dev --no-emit-project --no-color \
      --format requirements-txt -o src/requirements.txt
    sam build
    sam deploy --guided --parameter-overrides AlertEmail=<address>

## Test

    uv run pytest

## Operational scripts

Local, read-only tools under `scripts/`, run from the repo root as
modules so they can import `src/`:

    uv run python -m scripts.check_collection --date 2026-09-15
    uv run python -m scripts.verify_feeds

`check_collection.py` reports one UTC day's collector health from
the live bucket. It resolves the bucket from `--bucket`, then
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
