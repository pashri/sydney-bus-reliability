"""Tests for the checker's entry point."""

import json
import os
from datetime import UTC, date, datetime
from typing import Any, cast

import boto3
import pytest
from aws_lambda_powertools.utilities.typing import LambdaContext

from checker.handler import (
    SCHEMA_VERSION,
    day_range,
    handler,
    jsonable,
    target_day,
)

DAY = date(2026, 9, 17)


class _Context:
    """Minimal Lambda context for Powertools."""

    function_name = 'sydney-bus-reliability-checker'
    memory_limit_in_mb = 1024
    invoked_function_arn = (
        'arn:aws:lambda:ap-southeast-2:000000000000:function:test'
    )
    aws_request_id = 'test-request-id'


def _put_row(*, bucket: str, fetched_at: str) -> None:
    """Write one collector audit line into its UTC partition."""
    boto3.client('s3').put_object(
        Bucket=bucket,
        Key=f'curated/collector_run/dt={fetched_at[:10]}/0.jsonl',
        Body=json.dumps({
            'feed': 'vehiclepos',
            'fetched_at_utc': fetched_at,
            'received_at_utc': fetched_at,
            'rtt_s': 0.5,
            'server_date_utc': None,
            'skew_s': None,
            'status_code': 200,
            'body_bytes': 1234,
            'error': None,
        }).encode() + b'\n',
    )


def test_target_day_defaults_to_yesterday_in_sydney() -> None:
    """Sydney is a day ahead of UTC, so the default is read there."""
    assert target_day(
        event={}, now=datetime(2026, 9, 17, 18, 0, tzinfo=UTC),
    ) == date(2026, 9, 17)


def test_target_day_honours_an_override() -> None:
    assert target_day(
        event={'date': '2026-09-15'},
        now=datetime(2026, 9, 17, 18, 0, tzinfo=UTC),
    ) == date(2026, 9, 15)


def test_day_range_is_oldest_first_and_inclusive() -> None:
    assert day_range(last=DAY, count=3) == [
        date(2026, 9, 15), date(2026, 9, 16), DAY,
    ]


def test_day_range_of_one_is_just_that_day() -> None:
    assert day_range(last=DAY, count=1) == [DAY]


def test_day_range_never_returns_nothing() -> None:
    """A zero or negative window would otherwise summarise no days."""
    assert day_range(last=DAY, count=0) == [DAY]


def test_jsonable_renders_dates_as_strings() -> None:
    value = jsonable({
        'day': DAY,
        'at': datetime(2026, 9, 17, 1, 2, 3, tzinfo=UTC),
        'nested': [{'day': DAY}],
    })
    assert value['day'] == '2026-09-17'
    assert value['at'].startswith('2026-09-17T01:02:03')
    assert value['nested'][0]['day'] == '2026-09-17'


def test_jsonable_leaves_plain_values_alone() -> None:
    assert jsonable({'count': 3, 'name': 'x', 'ok': None}) == {
        'count': 3, 'name': 'x', 'ok': None,
    }


@pytest.fixture(name='environment')
def _environment(_bucket: str) -> Any:
    """Point the handler at the local moto bucket."""
    os.environ['BUCKET_NAME'] = _bucket
    yield
    del os.environ['BUCKET_NAME']


def _invoke(*, endpoint: str, event: dict[str, Any]) -> Any:
    """Run the handler against the local moto server."""
    return handler(event, cast(LambdaContext, _Context()), endpoint)


def test_handler_returns_both_halves(
    _bucket: str, _s3_endpoint: str, environment: None,
) -> None:
    _put_row(bucket=_bucket, fetched_at='2026-09-17T03:30:00+00:00')
    response = _invoke(
        endpoint=_s3_endpoint,
        event={'date': '2026-09-17', 'curation_days': 2},
    )
    assert response['schema_version'] == SCHEMA_VERSION
    assert len(response['collection']['days']) == 1
    assert len(response['curation']['days']) == 2


def test_handler_response_is_json_serialisable(
    _bucket: str, _s3_endpoint: str, environment: None,
) -> None:
    """The response crosses a Lambda boundary, so it must encode."""
    _put_row(bucket=_bucket, fetched_at='2026-09-17T03:30:00+00:00')
    response = _invoke(
        endpoint=_s3_endpoint,
        event={'date': '2026-09-17', 'curation_days': 1},
    )
    assert json.loads(json.dumps(response))['requested']['date'] == (
        '2026-09-17'
    )


def test_handler_reports_which_source_it_read(
    _bucket: str, _s3_endpoint: str, environment: None,
) -> None:
    """An unmerged day is read live, and says so."""
    _put_row(bucket=_bucket, fetched_at='2026-09-17T03:30:00+00:00')
    response = _invoke(
        endpoint=_s3_endpoint,
        event={'date': '2026-09-17', 'curation_days': 1},
    )
    assert response['collection']['days'][0]['source'] == 'live'


def test_handler_defaults_the_window_widths(
    _bucket: str, _s3_endpoint: str, environment: None,
) -> None:
    response = _invoke(
        endpoint=_s3_endpoint, event={'date': '2026-09-17'},
    )
    assert response['requested']['collection_days'] == 1
    assert response['requested']['curation_days'] == 14
