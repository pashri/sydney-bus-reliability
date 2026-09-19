"""Tests for process-level measurement shared by every curation Lambda."""

import resource

import pytest

from src.common import process


def test_peak_rss_mb_divides_by_1024_on_linux(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Linux reports ru_maxrss in kilobytes."""
    monkeypatch.setattr(process.sys, 'platform', 'linux')
    monkeypatch.setattr(
        resource,
        'getrusage',
        lambda who: type('_Usage', (), {'ru_maxrss': 2048 * 1024})(),
    )
    assert process.peak_rss_mb() == 2048


def test_peak_rss_mb_divides_by_1024_squared_on_darwin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """macOS reports ru_maxrss in bytes."""
    monkeypatch.setattr(process.sys, 'platform', 'darwin')
    monkeypatch.setattr(
        resource,
        'getrusage',
        lambda who: type('_Usage', (), {'ru_maxrss': 2048 * 1024**2})(),
    )
    assert process.peak_rss_mb() == 2048
