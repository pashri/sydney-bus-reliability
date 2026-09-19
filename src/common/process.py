"""How much memory the running process has used."""

import resource
import sys


def peak_rss_mb() -> int:
    """Report the most memory this process has used at once, in MB.

    Returns
    -------
    int
        Peak memory use in MB.

    Notes
    -----
    Linux gives this figure in kilobytes and macOS gives it in bytes,
    so the same divisor would be about 1000x wrong on one of them. The
    divisor is picked from ``sys.platform``.
    """
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == 'darwin':
        return usage // 1024**2
    return usage // 1024
