"""Tests for the static GTFS schedule loader handler."""

import io
import json
import zipfile
from http import HTTPStatus

import boto3
import pytest
import requests
import responses

from src.common.gtfs_static import StaticBundle
from src.common.parquet import ParquetRepository
from src.schedule_loader.handler import (
    fetch_bundle,
    latest_sha256,
    read_filename,
    schedule_check_key,
    write_dimensions,
)

BUNDLE_URL: str = (
    'https://api.transport.nsw.gov.au/v1/gtfs/schedule/buses'
)


def build_bundle(*, extra: str = '') -> bytes:
    """Build a minimal but complete static GTFS bundle.

    Parameters
    ----------
    extra : str
        Optional extra route row, used to change the content hash.

    Returns
    -------
    bytes
        Zip archive bytes.
    """
    members = {
        'stops.txt': (
            'stop_id,stop_name,stop_lat,stop_lon\n'
            '"200013","Alpha St","-33.8","151.2"\n'
        ),
        'routes.txt': (
            'route_id,agency_id,route_short_name,route_long_name,'
            'route_type\n'
            '"2447_160","2447","160","Cessnock","700"\n' + extra
        ),
        'trips.txt': (
            'route_id,service_id,trip_id,shape_id,trip_headsign,'
            'direction_id\n'
            '"2447_160","3","1012281","80047","Nelson Bay","0"\n'
        ),
        'stop_times.txt': (
            'trip_id,arrival_time,departure_time,stop_id,stop_sequence,'
            'shape_dist_traveled\n'
            '"1012281","07:30:00","07:30:30","200013","1","0.0"\n'
        ),
        'shapes.txt': (
            'shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon,'
            'shape_dist_traveled\n'
            '"80047","1","-33.8","151.2","0.0"\n'
        ),
        'calendar.txt': (
            'service_id,monday,tuesday,wednesday,thursday,friday,'
            'saturday,sunday,start_date,end_date\n'
            '"3","1","1","1","1","1","0","0","20260917","20270101"\n'
        ),
        'calendar_dates.txt': (
            'service_id,date,exception_type\n'
            '"3","20261225","2"\n'
        ),
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return buffer.getvalue()


def test_schedule_check_key_partitions_by_date() -> None:
    """Checks are partitioned by the UTC date of the fetch."""
    key = schedule_check_key(
        checked_at_iso='2026-09-19T03:00:00+00:00',
        invocation_id='abc',
    )
    assert key == (
        'curated/schedule_check/dt=2026-09-19/030000-abc.jsonl'
    )


def test_latest_sha256_prefers_the_later_same_day_check(
    _bucket: str,
) -> None:
    """A same-day retry must win over the earlier attempt.

    Keys sorted lexically by a random invocation id would pick
    arbitrarily, so the time is embedded in the name to make max()
    correct by construction.
    """
    client = boto3.client('s3')
    for iso, invocation, digest in (
        ('2026-09-19T03:00:00+00:00', 'zzz-early', 'early-hash'),
        ('2026-09-19T09:00:00+00:00', 'aaa-later', 'later-hash'),
    ):
        client.put_object(
            Bucket=_bucket,
            Key=schedule_check_key(
                checked_at_iso=iso, invocation_id=invocation,
            ),
            Body=json.dumps({
                'checked_at_utc': iso,
                'zip_sha256': digest,
                'zip_filename': 'b.zip',
                'changed': True,
                'valid_from': '2026-09-19',
            }).encode(),
        )
    assert latest_sha256(
        client=client, bucket=_bucket,
    ) == 'later-hash'


@pytest.mark.parametrize(
    ('disposition', 'expected'),
    [
        (
            'attachment; filename=buses_GTFS_PROD_20260918103100.zip',
            'buses_GTFS_PROD_20260918103100.zip',
        ),
        ('attachment; filename="a.zip"; size=123', 'a.zip'),
        ("attachment; filename*=UTF-8''name.zip", ''),
        ('', ''),
    ],
)
def test_read_filename_parses_content_disposition(
    disposition: str,
    expected: str,
) -> None:
    """The filename segment is parsed, quotes and garbage stripped."""
    response = requests.Response()
    if disposition:
        response.headers['Content-Disposition'] = disposition
    assert read_filename(response=response) == expected


@responses.activate
def test_fetch_bundle_hashes_bytes_not_filename() -> None:
    """Identical bytes under different filenames hash identically."""
    payload = build_bundle()
    responses.add(
        responses.GET, BUNDLE_URL, body=payload,
        status=HTTPStatus.OK,
        headers={
            'Content-Disposition':
                'attachment; filename=buses_GTFS_PROD_20260918103100.zip',
        },
    )
    responses.add(
        responses.GET, BUNDLE_URL, body=payload,
        status=HTTPStatus.OK,
        headers={
            'Content-Disposition':
                'attachment; filename=buses_GTFS_PROD_20260919110000.zip',
        },
    )
    first = fetch_bundle(api_key='k')
    second = fetch_bundle(api_key='k')
    assert first.sha256 == second.sha256
    assert first.filename != second.filename


@responses.activate
def test_fetch_bundle_raises_on_non_ok() -> None:
    """A failed fetch raises, because the day's bundle is unrecoverable."""
    responses.add(
        responses.GET, BUNDLE_URL, body=b'',
        status=HTTPStatus.FORBIDDEN,
    )
    with pytest.raises(RuntimeError):
        fetch_bundle(api_key='k')


def test_latest_sha256_none_when_no_history(_bucket: str) -> None:
    """A first-ever run has no previous hash to compare against."""
    client = boto3.client('s3')
    assert latest_sha256(client=client, bucket=_bucket) is None


def test_latest_sha256_reads_most_recent_record(_bucket: str) -> None:
    """The newest check record's hash is returned."""
    client = boto3.client('s3')
    client.put_object(
        Bucket=_bucket,
        Key='curated/schedule_check/dt=2026-09-18/a.jsonl',
        Body=b'{"zip_sha256": "old"}',
    )
    client.put_object(
        Bucket=_bucket,
        Key='curated/schedule_check/dt=2026-09-19/b.jsonl',
        Body=b'{"zip_sha256": "new"}',
    )
    assert latest_sha256(client=client, bucket=_bucket) == 'new'


def test_write_dimensions_writes_every_dimension(_bucket: str) -> None:
    """Every dimension in the bundle is written to its own key."""
    bundle = StaticBundle(
        payload=build_bundle(), sha256='deadbeef', filename='x.zip',
    )
    repository = ParquetRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    written = write_dimensions(
        bundle=bundle, repository=repository, valid_from='2026-09-19',
    )
    assert written == {
        'dim_stop': 1,
        'dim_route': 1,
        'dim_trip': 1,
        'dim_scheduled_stop_time': 1,
        'dim_shape': 1,
        'dim_calendar': 1,
        'dim_calendar_dates': 1,
    }


def test_write_dimensions_raises_on_corrupt_zip(_bucket: str) -> None:
    """A corrupt or truncated download raises rather than being lost."""
    bundle = StaticBundle(
        payload=b'not a zip', sha256='deadbeef', filename='x.zip',
    )
    repository = ParquetRepository(
        bucket=_bucket, session=boto3.Session(),
    )
    with pytest.raises(zipfile.BadZipFile):
        write_dimensions(
            bundle=bundle, repository=repository,
            valid_from='2026-09-19',
        )
