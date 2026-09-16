"""Tests for feed fetching."""

from datetime import UTC, datetime
from http import HTTPStatus

import requests
import responses

from src.common.feeds import fetch_feed
from src.common.types_ import Feed

VEHICLE_URL = (
    'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses'
)
TRIP_URL = 'https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses'


@responses.activate
def test_fetch_feed_returns_body_and_server_date() -> None:
    responses.add(
        responses.GET,
        VEHICLE_URL,
        body=b'\x0a\x031.0',
        status=200,
        headers={'Date': 'Tue, 15 Sep 2026 10:36:00 GMT'},
    )
    result = fetch_feed(feed=Feed.VEHICLE_POSITIONS, api_key='k')
    assert result.status_code == HTTPStatus.OK
    assert result.body == b'\x0a\x031.0'
    assert result.server_date_utc == datetime(
        2026, 9, 15, 10, 36, tzinfo=UTC,
    )
    assert result.received_at_utc >= result.fetched_at_utc
    assert result.error is None


@responses.activate
def test_fetch_feed_sends_apikey_header() -> None:
    responses.add(responses.GET, TRIP_URL, body=b'x', status=200)
    fetch_feed(feed=Feed.TRIP_UPDATES, api_key='secret')
    assert (
        responses.calls[0].request.headers['Authorization']
        == 'apikey secret'
    )


@responses.activate
def test_fetch_feed_records_non_200_without_raising() -> None:
    responses.add(
        responses.GET, VEHICLE_URL, body=b'nope', status=403,
    )
    result = fetch_feed(feed=Feed.VEHICLE_POSITIONS, api_key='k')
    assert result.status_code == HTTPStatus.FORBIDDEN
    assert result.error is not None
    assert result.body == b''


@responses.activate
def test_fetch_feed_records_transport_error_without_raising() -> None:
    responses.add(
        responses.GET,
        VEHICLE_URL,
        body=requests.exceptions.ConnectionError('boom'),
    )
    result = fetch_feed(feed=Feed.VEHICLE_POSITIONS, api_key='k')
    assert result.status_code is None
    assert result.error is not None
    assert result.body == b''


@responses.activate
def test_fetch_feed_tolerates_missing_date_header() -> None:
    responses.add(responses.GET, VEHICLE_URL, body=b'x', status=200)
    result = fetch_feed(feed=Feed.VEHICLE_POSITIONS, api_key='k')
    assert result.server_date_utc is None
    assert result.body == b'x'
