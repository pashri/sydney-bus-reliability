"""Tests for reference table naming and location."""

from datetime import date

import pytest

from analysis.reference.vintage import (
    ReferenceTable,
    SourceRelease,
    manifest_key,
    partition_prefix,
    table_prefix,
    vintage_prefix,
)

VINTAGE = date(2026, 9, 22)


def test_table_prefix() -> None:
    assert table_prefix(table=ReferenceTable.STOP_GEOGRAPHY) == (
        'reference/stop_geography'
    )


def test_table_prefix_pending_is_separate() -> None:
    assert table_prefix(
        table=ReferenceTable.STOP_GEOGRAPHY,
        pending=True,
    ) == 'reference/_pending/stop_geography'


def test_vintage_prefix() -> None:
    assert vintage_prefix(
        table=ReferenceTable.STOP_MESHBLOCK,
        vintage=VINTAGE,
    ) == 'reference/stop_meshblock/vintage=2026-09-22'


def test_partition_prefix() -> None:
    assert partition_prefix(
        table=ReferenceTable.CENSUS_SA1,
        vintage=VINTAGE,
        partition='G01',
    ) == 'reference/census_sa1/vintage=2026-09-22/table=G01'


@pytest.mark.parametrize('bad', ['', 'a/b', 'table=G01'])
def test_partition_prefix_rejects_unusable_values(bad: str) -> None:
    with pytest.raises(ValueError, match='unusable partition value'):
        partition_prefix(
            table=ReferenceTable.CENSUS_SA1,
            vintage=VINTAGE,
            partition=bad,
        )


def test_manifest_key() -> None:
    assert manifest_key(vintage=VINTAGE) == (
        'reference/_manifest/vintage=2026-09-22/manifest.json'
    )


def test_manifest_key_pending() -> None:
    assert manifest_key(vintage=VINTAGE, pending=True).startswith(
        'reference/_pending/_manifest/',
    )


def test_pending_and_published_never_collide() -> None:
    published = vintage_prefix(
        table=ReferenceTable.STATION,
        vintage=VINTAGE,
    )
    pending = vintage_prefix(
        table=ReferenceTable.STATION,
        vintage=VINTAGE,
        pending=True,
    )
    assert published != pending
    assert not pending.startswith(f'{published}/')


def test_source_release_records_editions_independently() -> None:
    release = SourceRelease(
        asgs_edition='ASGS2021',
        seifa_release='SEIFA2021',
        census_year=2021,
    )
    assert release.asgs_edition != release.seifa_release
