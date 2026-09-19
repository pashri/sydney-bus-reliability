"""Process-level measurement shared by every curation Lambda.

Kept separate from any one Lambda's handler module, because
``peak_rss_mb`` is ``resource.getrusage`` plus a platform check and has
nothing to do with any particular Lambda's business logic. Importing it
from another handler's module couples two Lambdas across a packaging
boundary and breaks silently if that module is ever refactored.
"""

import resource
import sys


def peak_rss_mb() -> int:
    """Report this process's peak resident set size in megabytes.

    Recorded on every curation run because the memory envelope cost
    Phase 1 the most time, and because a streaming requirement has no
    guard other than measurement.

    Returns
    -------
    int
        Peak RSS in MB.

    Notes
    -----
    Linux reports ``ru_maxrss`` in kilobytes; macOS reports it in
    bytes. Dividing unconditionally by 1024 is roughly 1000x wrong on
    one of the two platforms, so the divisor is chosen by
    ``sys.platform``.
    """
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == 'darwin':
        return usage // 1024**2
    return usage // 1024
