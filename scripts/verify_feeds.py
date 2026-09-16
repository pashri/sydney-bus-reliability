"""Phase 0 measurement of the TfNSW GTFS feeds.

Run locally with the API key in the environment. Not deployed. Prints the
entity counts and payload sizes needed to replace the spec's estimated
row-per-day figures with measured ones.
"""

import os
import sys

import requests
from google.transit import gtfs_realtime_pb2

BASE_URL = 'https://api.transport.nsw.gov.au/v1/gtfs'
FEEDS = {
    'vehiclepos': f'{BASE_URL}/vehiclepos/buses',
    'tripupdates': f'{BASE_URL}/realtime/buses',
}
TIMEOUT = 30.0


def measure(*, name: str, url: str, api_key: str) -> None:
    """Fetch one feed and print its size and entity count.

    Parameters
    ----------
    name : str
        Short feed name, used only for display.
    url : str
        Fully qualified feed URL.
    api_key : str
        TfNSW Open Data Hub API key.
    """
    response = requests.get(
        url,
        headers={'Authorization': f'apikey {api_key}'},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()  # pylint: disable=no-member
    feed.ParseFromString(response.content)
    print(
        f'{name}: {response.status_code} '
        f'{len(response.content):,} bytes '
        f'{len(feed.entity):,} entities '
        f'date={response.headers.get("Date")}'
    )


def main() -> int:
    """Measure every feed and return a process exit code.

    Returns
    -------
    int
        Zero on success.
    """
    api_key = os.environ['TFNSW_API_KEY']
    for name, url in FEEDS.items():
        measure(name=name, url=url, api_key=api_key)
    return 0


if __name__ == '__main__':
    sys.exit(main())
