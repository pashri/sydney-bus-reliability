"""Guard against pyarrow drift between Lambda and the lockfile.

The compactor and schedule_loader get ``pyarrow`` from the AWS-managed
SDK-for-pandas layer, while tests import whatever ``uv.lock`` pins. A
mismatch is invisible until a Parquet file written in production differs
from one written in a test, so CI asserts the two agree.
"""

import argparse
import logging
import sys
from typing import Final

import pyarrow

logger = logging.getLogger(__name__)

LAYER_ARN: Final[str] = (
    'arn:aws:lambda:ap-southeast-2:336392948345:'
    'layer:AWSSDKPandas-Python314-Arm64:11'
)
LAYER_PYARROW_VERSION: Final[str] = '24.0.0'
"""The ``pyarrow`` version inside :data:`LAYER_ARN`.

Bumping the layer in ``template.yaml`` must bump this and the
``pyproject.toml`` pin together, or CI fails.
"""


def check_parity(*, installed: str, expected: str) -> bool:
    """Compare two version strings for exact equality.

    Parameters
    ----------
    installed : str
        Version importable in this environment.
    expected : str
        Version shipped inside the Lambda layer.

    Returns
    -------
    bool
        True when the two agree exactly.
    """
    return installed == expected


def main() -> int:
    """Fail the build when the layer and lockfile disagree.

    Returns
    -------
    int
        0 when versions match, 1 otherwise.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    installed = pyarrow.__version__
    if check_parity(
        installed=installed, expected=LAYER_PYARROW_VERSION,
    ):
        return 0
    logger.error(
        'pyarrow %s installed but layer ships %s (%s)',
        installed,
        LAYER_PYARROW_VERSION,
        LAYER_ARN,
    )
    return 1


if __name__ == '__main__':
    sys.exit(main())
