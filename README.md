# Sydney bus reliability

[![CI](https://github.com/pashri/sydney-bus-reliability/actions/workflows/ci.yml/badge.svg)](https://github.com/pashri/sydney-bus-reliability/actions/workflows/ci.yml)

Collects Transport for NSW GTFS-Realtime bus feeds and measures how
reliable Sydney's buses are.

Contains public sector information licensed under the Creative Commons
Attribution 4.0 licence. Data source: Transport for NSW.

## Stack

- AWS SAM: one collector Lambda on a 1-minute schedule, one S3 bucket
- Vehicle positions polled every 10 seconds, trip updates every 60
- Raw protobuf stored gzipped, keyed by actual fetch time, expired after
  30 days
- Python 3.14, managed with uv

## Deploy

    uv sync
    aws ssm put-parameter \
      --name /sydney-bus-reliability/tfnsw-api-key \
      --value '<key>' --type SecureString --region ap-southeast-2
    uv export --no-dev --no-emit-project --format requirements-txt \
      > requirements.txt
    sam build
    sam deploy --guided --parameter-overrides AlertEmail=<address>

## Test

    uv run pytest
