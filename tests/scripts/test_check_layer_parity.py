"""Tests for the pyarrow layer parity guard."""

from scripts.check_layer_parity import check_parity


def test_check_parity_matching_versions() -> None:
    """Identical versions pass."""
    assert check_parity(installed='21.0.0', expected='21.0.0')


def test_check_parity_mismatched_versions() -> None:
    """Any difference fails, so a layer bump breaks the build."""
    assert not check_parity(installed='21.0.0', expected='25.0.1')
