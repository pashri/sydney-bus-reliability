"""Tests for server clock reconciliation."""

from datetime import UTC, datetime

import pytest

from src.common.clock import (
    parse_server_date,
    round_trip_seconds,
    skew_seconds,
)


def test_parse_server_date_returns_utc() -> None:
    result = parse_server_date(
        header_value='Tue, 15 Sep 2026 10:36:00 GMT',
    )
    assert result == datetime(2026, 9, 15, 10, 36, tzinfo=UTC)


def test_parse_server_date_converts_non_utc_zone() -> None:
    result = parse_server_date(
        header_value='Tue, 15 Sep 2026 20:36:00 +1000',
    )
    assert result == datetime(2026, 9, 15, 10, 36, tzinfo=UTC)


def test_parse_server_date_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        parse_server_date(header_value='not a date')


def test_round_trip_seconds_measures_elapsed_time() -> None:
    sent = datetime(2026, 9, 15, 10, 36, 0, tzinfo=UTC)
    received = datetime(2026, 9, 15, 10, 36, 0, 200000, tzinfo=UTC)
    assert round_trip_seconds(
        sent_at=sent, received_at=received,
    ) == 0.2


def test_skew_seconds_is_zero_for_synchronised_clocks(
) -> None:
    sent = datetime(2026, 9, 15, 10, 36, 0, tzinfo=UTC)
    received = datetime(2026, 9, 15, 10, 36, 0, 200000, tzinfo=UTC)
    server = datetime(2026, 9, 15, 10, 36, 0, 100000, tzinfo=UTC)
    assert skew_seconds(
        server_time=server, sent_at=sent, received_at=received,
    ) == 0.0


def test_skew_seconds_positive_when_local_ahead() -> None:
    sent = datetime(2026, 9, 15, 10, 36, 3, tzinfo=UTC)
    received = datetime(2026, 9, 15, 10, 36, 3, 200000, tzinfo=UTC)
    server = datetime(2026, 9, 15, 10, 36, 0, 100000, tzinfo=UTC)
    assert skew_seconds(
        server_time=server, sent_at=sent, received_at=received,
    ) == 3.0


def test_skew_seconds_negative_when_local_behind() -> None:
    sent = datetime(2026, 9, 15, 10, 36, 0, tzinfo=UTC)
    received = datetime(2026, 9, 15, 10, 36, 0, 200000, tzinfo=UTC)
    server = datetime(2026, 9, 15, 10, 36, 5, 100000, tzinfo=UTC)
    assert skew_seconds(
        server_time=server, sent_at=sent, received_at=received,
    ) == -5.0
