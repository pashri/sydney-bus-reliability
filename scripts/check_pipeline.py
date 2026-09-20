"""Print the pipeline checker's findings.

Run from the repo root::

    uv run python -m scripts.check_pipeline

Invokes the deployed checker Lambda, which reads the curated layer in
region, and renders what it returns. The figures are the Lambda's; this
script only formats them.

The function is resolved from ``--function-name``, then a lookup of the
deployed stack's ``CheckerFunctionName`` output (``--stack-name``).
Credentials come from ``--profile`` if given, else the standard chain.
"""

import argparse
import json
import sys
from typing import Any, Final

import boto3
from botocore.exceptions import ClientError

DEFAULT_STACK_NAME: Final[str] = 'sydney-bus-reliability'
DEFAULT_FUNCTION_NAME: Final[str] = 'sydney-bus-reliability-checker'
SUPPORTED_SCHEMA: Final[int] = 1
INDENT: Final[str] = ' ' * 14


def parse_args() -> argparse.Namespace:
    """Read the command line.

    Returns
    -------
    argparse.Namespace
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', help='last Sydney day to report')
    parser.add_argument('--collection-days', type=int)
    parser.add_argument('--curation-days', type=int)
    parser.add_argument('--function-name')
    parser.add_argument('--stack-name', default=DEFAULT_STACK_NAME)
    parser.add_argument('--profile')
    return parser.parse_args()


def build_event(*, args: argparse.Namespace) -> dict[str, Any]:
    """Build the invocation event from the command line.

    Parameters
    ----------
    args : argparse.Namespace
        The parsed arguments.

    Returns
    -------
    dict[str, Any]
        Only the keys the caller actually set.
    """
    wanted = {
        'date': args.date,
        'collection_days': args.collection_days,
        'curation_days': args.curation_days,
    }
    return {key: one for key, one in wanted.items() if one is not None}


def resolve_function(*, session: boto3.Session, args: Any) -> str:
    """Find which function to invoke.

    Parameters
    ----------
    session : boto3.Session
        Session to look the stack up with.
    args : Any
        The parsed arguments.

    Returns
    -------
    str
        The function name to invoke.
    """
    if args.function_name:
        return str(args.function_name)
    try:
        stacks = session.client('cloudformation').describe_stacks(
            StackName=args.stack_name,
        )
    except ClientError:
        return DEFAULT_FUNCTION_NAME
    outputs = stacks['Stacks'][0].get('Outputs', [])
    return next(
        (
            one['OutputValue'] for one in outputs
            if one['OutputKey'] == 'CheckerFunctionName'
        ),
        DEFAULT_FUNCTION_NAME,
    )


def invoke(
    *, session: boto3.Session, name: str, event: dict[str, Any],
) -> dict[str, Any]:
    """Invoke the checker and read its response.

    Parameters
    ----------
    session : boto3.Session
        Session to invoke with.
    name : str
        Function to invoke.
    event : dict[str, Any]
        The invocation event.

    Returns
    -------
    dict[str, Any]
        The decoded response.

    Raises
    ------
    RuntimeError
        If the function reported an error.
    """
    response = session.client('lambda').invoke(
        FunctionName=name, Payload=json.dumps(event).encode(),
    )
    payload = json.loads(response['Payload'].read())
    if response.get('FunctionError'):
        raise RuntimeError(f'checker failed: {payload}')
    return dict(payload)


def fmt_bytes(value: float) -> str:
    """Format a byte count with a sensible unit.

    Parameters
    ----------
    value : float
        A size in bytes.

    Returns
    -------
    str
        `value` formatted in B, KB, MB or GB, whichever reads best.
    """
    for limit, unit in ((1e9, 'GB'), (1e6, 'MB'), (1e3, 'KB')):
        if value >= limit:
            return f'{value / limit:.1f} {unit}'
    return f'{value:,.0f} B'


def fmt_duration(*, minutes: int) -> str:
    """Format a minute count as ``HhMMm``.

    Parameters
    ----------
    minutes : int
        Duration in whole minutes.

    Returns
    -------
    str
        Duration formatted as e.g. ``4h 08m``.
    """
    hours, remainder = divmod(minutes, 60)
    return f'{hours}h {remainder:02d}m'


def fmt_window(window: dict[str, Any] | None) -> str:
    """Format the observed collection window.

    Parameters
    ----------
    window : dict[str, Any] | None
        The observed span, or None if no rows were seen.

    Returns
    -------
    str
        A human-readable window description.
    """
    if not window:
        return 'no rows observed'
    span = fmt_duration(minutes=window['minutes'])
    return (
        f"{window['start'][11:16]}-{window['end'][11:16]} UTC ({span})"
    )


def fmt_timing(stats: dict[str, Any] | None) -> str:
    """Format min/median/max timing statistics, in seconds.

    Parameters
    ----------
    stats : dict[str, Any] | None
        The statistics, or None if none were measurable.

    Returns
    -------
    str
        A human-readable summary.
    """
    if not stats:
        return 'not measured'
    return (
        f"min {stats['minimum']:.2f}s, "
        f"median {stats['median']:.2f}s, "
        f"max {stats['maximum']:.2f}s"
    )


def print_collection(*, day: dict[str, Any]) -> None:
    """Print one Sydney day of collection.

    Parameters
    ----------
    day : dict[str, Any]
        One day's collection summary.
    """
    print(f"\ncollection  {day['date']}  ({day['source']})")
    print(f"  window      {fmt_window(day['window'])}")
    for feed, counts in sorted(day['feed_counts'].items()):
        print(
            f"  {feed:<11} {counts['actual']:,} polls "
            f"of {counts['expected']:,} expected",
        )
    print_failures(failures=day['failures'])
    print(f"  rtt         {fmt_timing(day['rtt'])}")
    print(f"  skew        {fmt_timing(day['skew'])}")
    for feed, size in sorted(day['payload_by_feed'].items()):
        print(
            f'  payload     {feed}: min {fmt_bytes(size["minimum"])}, '
            f'median {fmt_bytes(size["median"])}, '
            f'max {fmt_bytes(size["maximum"])}',
        )
    print(f"  fetched     {fmt_bytes(day['total_bytes'])} uncompressed")
    print_coverage(coverage=day['coverage'])


def print_failures(*, failures: dict[str, Any]) -> None:
    """Print the day's lost polls, by kind.

    Parameters
    ----------
    failures : dict[str, Any]
        The day's failure summary.
    """
    kinds = {
        'crashed': failures['crashed_count'],
        'transport': failures['transport_error_count'],
        'non-200': failures['non_200_count'],
        'no server date': failures['null_server_date_count'],
    }
    shown = ', '.join(
        f'{count} {kind}' for kind, count in kinds.items() if count
    )
    print(f"  failures    {shown or 'none'}")


def print_coverage(*, coverage: dict[str, Any]) -> None:
    """Print the day's per-minute coverage gaps.

    Parameters
    ----------
    coverage : dict[str, Any]
        The day's coverage summary.
    """
    short = coverage['minutes_short']
    print(f'  coverage    {short} minutes short')
    if coverage['worst']:
        gaps = '  '.join(
            f"{gap['minute']} ({gap['count']})"
            for gap in coverage['worst']
        )
        print(f'{INDENT}{gaps}')


def print_curation(*, day: dict[str, Any]) -> None:
    """Print one Sydney day of curation.

    Parameters
    ----------
    day : dict[str, Any]
        One day's curation summary.
    """
    print(f"\ncuration    {day['day']}  ({day['hours_in_day']} hours)")
    for job, figures in sorted(day['jobs'].items()):
        print_job(job=job, figures=figures)
    schedule = day['schedule']
    changed = 'changed' if schedule['changed'] else 'unchanged'
    print(
        f"  schedule    {schedule['checks_seen']}/"
        f"{schedule['checks_expected']} checks, timetable {changed}",
    )


def print_job(*, job: str, figures: dict[str, Any]) -> None:
    """Print one curation job's day.

    Parameters
    ----------
    job : str
        The job's name.
    figures : dict[str, Any]
        The job's summary for the day.
    """
    totals = figures['totals']
    print(
        f"  {job:<11} {figures['runs_seen']}/"
        f"{figures['runs_expected']} runs, "
        f"{figures['errors']} errors, "
        f"peak {figures['peak_rss_mb']} MB",
    )
    if figures['missing_partitions']:
        missing = ' '.join(figures['missing_partitions'])
        print(f'{INDENT}MISSING {missing}')
    print(
        f"{INDENT}objects {totals['objects_read']:,}"
        f"/{totals['objects_expected']:,}, "
        f"rows {totals['rows_in']:,} -> {totals['rows_out']:,}",
    )
    print(
        f"{INDENT}dupes {totals['dupes_collapsed']:,} "
        f"({totals['dupes_differing_position']:,} disagreeing), "
        f"unjoined trips {totals['unjoined_trip_ids']:,}",
    )


def print_report(*, response: dict[str, Any]) -> None:
    """Print a whole response.

    Parameters
    ----------
    response : dict[str, Any]
        The checker's response.
    """
    if response.get('schema_version') != SUPPORTED_SCHEMA:
        print(
            f"warning: checker returned schema "
            f"{response.get('schema_version')}, this script reads "
            f'{SUPPORTED_SCHEMA}',
            file=sys.stderr,
        )
    for day in response['collection']['days']:
        print_collection(day=day)
    for day in response['curation']['days']:
        print_curation(day=day)


def main() -> int:
    """Invoke the checker and print its findings.

    Returns
    -------
    int
        Process exit status.
    """
    args = parse_args()
    session = boto3.Session(profile_name=args.profile)
    name = resolve_function(session=session, args=args)
    try:
        response = invoke(
            session=session, name=name, event=build_event(args=args),
        )
    except (ClientError, RuntimeError) as error:
        print(f'could not read the checker: {error}', file=sys.stderr)
        return 1
    print_report(response=response)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
