"""Rebuild service days from raw through the deployed Lambdas.

Run from the repo root::

    PYTHONPATH=src uv run python -m scripts.replay \\
        --from 2026-09-17 --to 2026-09-22 \\
        --profile pashri-admin --log replay.jsonl

Compacts every raw hour inside the chosen service days' merge windows,
then merges each day into every fact table. Both Lambdas overwrite
what they write, so a replay can be rerun. With ``--log``, finished
steps are recorded and a rerun skips them.

Every hour is compacted before any day is merged, because a day's
window reaches into the next day's hours. Partials expire three days
after they are written, so the merges must run within three days of
the compaction. A day whose window has not closed yet is refused.
"""

import argparse
import json
import logging
import sys
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import boto3
from botocore.config import Config

from common.service_day import merge_window, service_date_for

logger = logging.getLogger(__name__)

DEFAULT_BUCKET: Final[str] = 'sydney-bus-reliability-043262084828'
DEFAULT_COMPACTOR: Final[str] = 'sydney-bus-reliability-compactor'
DEFAULT_MERGER: Final[str] = 'sydney-bus-reliability-merger'
RAW_FEEDS: Final[tuple[str, ...]] = ('tripupdates', 'vehiclepos')
READ_TIMEOUT: Final[int] = 900  # seconds
"""Longer than either function's own timeout, so a slow success is
not reported as a failure."""
MAX_FAILURES: Final[int] = 3


def parse_args() -> argparse.Namespace:
    """Read the command line.

    Returns
    -------
    argparse.Namespace
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--from', dest='first', required=True,
                        type=date.fromisoformat,
                        help='first Sydney service day, YYYY-MM-DD')
    parser.add_argument('--to', dest='last', type=date.fromisoformat,
                        help='last service day, default the latest complete')
    parser.add_argument('--parallel', type=int, default=3)
    parser.add_argument('--profile')
    parser.add_argument('--bucket', default=DEFAULT_BUCKET)
    parser.add_argument('--compactor', default=DEFAULT_COMPACTOR)
    parser.add_argument('--merger', default=DEFAULT_MERGER)
    parser.add_argument('--log', type=Path,
                        help='JSON Lines record of finished steps')
    return parser.parse_args()


def parse_raw_hour(*, prefix: str) -> datetime:
    """Read the UTC hour a raw prefix is partitioned under.

    Parameters
    ----------
    prefix : str
        A prefix such as ``raw/tripupdates/dt=2026-09-16/hour=16/``.

    Returns
    -------
    datetime
        Start of the hour, UTC.
    """
    _, _, day, hour = prefix.rstrip('/').split('/')
    return datetime.strptime(
        f"{day.removeprefix('dt=')} {hour.removeprefix('hour=')}",
        '%Y-%m-%d %H',
    ).replace(tzinfo=UTC)


def child_prefixes(*, client: Any, bucket: str, prefix: str) -> list[str]:
    """List the prefixes one level below another.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket to list.
    prefix : str
        Parent prefix, ending in ``/``.

    Returns
    -------
    list[str]
        Child prefixes, each ending in ``/``.
    """
    pages = client.get_paginator('list_objects_v2').paginate(
        Bucket=bucket, Prefix=prefix, Delimiter='/',
    )
    return [
        common['Prefix']
        for page in pages
        for common in page.get('CommonPrefixes', ())
    ]


def list_raw_hours(*, client: Any, bucket: str) -> set[datetime]:
    """Find every UTC hour holding raw objects for either feed.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding ``raw/``.

    Returns
    -------
    set[datetime]
        Hours with at least one raw object.
    """
    return {
        parse_raw_hour(prefix=hour)
        for feed in RAW_FEEDS
        for day in child_prefixes(
            client=client, bucket=bucket, prefix=f'raw/{feed}/',
        )
        for hour in child_prefixes(client=client, bucket=bucket, prefix=day)
    }


def last_complete_day(*, now: datetime) -> date:
    """Find the latest service day whose merge window has closed.

    Parameters
    ----------
    now : datetime
        Current UTC time.

    Returns
    -------
    date
        The latest Sydney service day safe to merge.
    """
    day = service_date_for(instant=now)
    while merge_window(service_date=day)[1] > now:
        day -= timedelta(days=1)
    return day


def hours_to_compact(
    *,
    available: set[datetime],
    first: date,
    last: date,
    now: datetime,
) -> list[datetime]:
    """Choose the raw hours the replayed days' merges will read.

    Parameters
    ----------
    available : set[datetime]
        Hours with raw objects.
    first : date
        First service day to replay.
    last : date
        Last service day to replay.
    now : datetime
        Current UTC time.

    Returns
    -------
    list[datetime]
        Hours inside the union of the days' merge windows, in order.

    Raises
    ------
    ValueError
        If ``last`` is a day whose merge window has not closed.
    """
    if last > last_complete_day(now=now):
        raise ValueError(f'service day {last} is not complete yet')
    start = merge_window(service_date=first)[0]
    end = merge_window(service_date=last)[1]
    return sorted(hour for hour in available if start <= hour < end)


def failure_of(*, response: dict[str, Any]) -> str | None:
    """Read why a synchronous Lambda invocation failed, if it did.

    Parameters
    ----------
    response : dict[str, Any]
        The ``invoke`` response.

    Returns
    -------
    str | None
        ``errorType: errorMessage``, or None when it succeeded.
    """
    payload = json.loads(response['Payload'].read() or b'{}')
    if 'FunctionError' not in response:
        return None
    return f"{payload.get('errorType')}: {payload.get('errorMessage')}"


@dataclass(frozen=True, slots=True)
class ReplayLog:
    """A JSON Lines record of finished replay steps."""

    path: Path | None

    def done(self) -> set[str]:
        """Read the steps an earlier run finished.

        Returns
        -------
        set[str]
            Step names, empty without a log or before the first run.
        """
        if self.path is None or not self.path.exists():
            return set()
        lines = self.path.read_text().splitlines()
        return {json.loads(line)['step'] for line in lines if line}

    def record(self, *, step: str) -> None:
        """Append one finished step.

        Parameters
        ----------
        step : str
            Step name.
        """
        if self.path is None:
            return
        with self.path.open('a') as handle:
            handle.write(json.dumps({
                'step': step,
                'finished_at_utc': datetime.now(tz=UTC).isoformat(),
            }) + '\n')


def invoker(
    *, session: boto3.Session, function: str,
) -> Callable[[dict[str, Any]], str | None]:
    """Build a function invoking one Lambda synchronously.

    Parameters
    ----------
    session : boto3.Session
        Session to invoke with.
    function : str
        Function name.

    Returns
    -------
    Callable[[dict[str, Any]], str | None]
        Takes an event, returns the failure or None.
    """
    client = session.client('lambda', config=Config(
        read_timeout=READ_TIMEOUT, retries={'max_attempts': 0},
    ))

    def invoke(event: dict[str, Any]) -> str | None:
        """Invoke the function once and wait for it.

        Parameters
        ----------
        event : dict[str, Any]
            The invocation event.

        Returns
        -------
        str | None
            The failure, or None when it succeeded.
        """
        return failure_of(response=client.invoke(
            FunctionName=function, Payload=json.dumps(event).encode(),
        ))

    return invoke


def run_steps(
    *,
    steps: Iterable[tuple[str, dict[str, Any]]],
    invoke: Callable[[dict[str, Any]], str | None],
    log: ReplayLog,
    parallel: int,
) -> int:
    """Run named invocations, skipping ones already logged as done.

    Parameters
    ----------
    steps : Iterable[tuple[str, dict[str, Any]]]
        Step name and event, in order.
    invoke : Callable[[dict[str, Any]], str | None]
        Invokes one event, returning its failure or None.
    log : ReplayLog
        Record of finished steps.
    parallel : int
        Invocations in flight at once.

    Returns
    -------
    int
        Steps that failed. Stops submitting once ``MAX_FAILURES`` is
        reached, since repeated failures usually share a cause.
    """
    done = log.done()
    pending = [(name, event) for name, event in steps if name not in done]
    failures = 0
    with ThreadPoolExecutor(max_workers=parallel) as pool:
        futures = {
            name: pool.submit(invoke, event) for name, event in pending
        }
        for name, future in futures.items():
            failure = future.result()
            if failure is None:
                log.record(step=name)
                logger.info('%s ok', name)
                continue
            failures += 1
            logger.error('%s failed: %s', name, failure)
            if failures >= MAX_FAILURES:
                pool.shutdown(cancel_futures=True)
                break
    return failures


def main() -> int:
    """Compact every hour the chosen days need, then merge each day.

    Returns
    -------
    int
        0 when every step succeeded, 1 otherwise.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    args = parse_args()
    now = datetime.now(tz=UTC)
    last = args.last or last_complete_day(now=now)
    session = boto3.Session(profile_name=args.profile)
    hours = hours_to_compact(
        available=list_raw_hours(
            client=session.client('s3'), bucket=args.bucket,
        ),
        first=args.first, last=last, now=now,
    )
    log = ReplayLog(path=args.log)
    logger.info('compacting %d hours for %s to %s', len(hours),
                args.first, last)
    if run_steps(
        steps=(
            (f'compact {hour:%Y-%m-%dT%H}', {'hour': hour.isoformat()})
            for hour in hours
        ),
        invoke=invoker(session=session, function=args.compactor),
        log=log, parallel=args.parallel,
    ):
        return 1
    days = [
        args.first + timedelta(days=offset)
        for offset in range((last - args.first).days + 1)
    ]
    return int(bool(run_steps(
        steps=(
            (f'merge {day}', {'service_date': f'{day}'}) for day in days
        ),
        invoke=invoker(session=session, function=args.merger),
        log=log, parallel=1,
    )))


if __name__ == '__main__':
    sys.exit(main())
