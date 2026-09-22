"""Tests for the published reference table schemas."""

import pyarrow as pa
import pytest

from analysis.reference.schemas import (
    STOP_GEOGRAPHY_SCHEMA,
    STOP_MESHBLOCK_SCHEMA,
    VINTAGE_FIELDS,
    GeographyMatch,
    RoutingStatus,
)

SCHEMAS = (STOP_GEOGRAPHY_SCHEMA, STOP_MESHBLOCK_SCHEMA)

CODE_COLUMNS = (
    'mesh_block_code',
    'sa1_code',
    'sa2_code',
    'sa3_code',
    'sa4_code',
    'gccsa_code',
    'lga_code',
)


@pytest.mark.parametrize('schema', SCHEMAS)
def test_every_schema_carries_provenance(schema: pa.Schema) -> None:
    for field in VINTAGE_FIELDS:
        assert schema.field(field.name).type == field.type


@pytest.mark.parametrize('schema', SCHEMAS)
def test_every_column_is_nullable(schema: pa.Schema) -> None:
    assert all(field.nullable for field in schema)


@pytest.mark.parametrize('column', CODE_COLUMNS)
def test_abs_codes_are_text(column: str) -> None:
    assert STOP_GEOGRAPHY_SCHEMA.field(column).type == pa.string()


def test_meshblock_codes_are_text() -> None:
    assert STOP_MESHBLOCK_SCHEMA.field('mesh_block_code').type == pa.string()
    assert STOP_MESHBLOCK_SCHEMA.field('sa1_code').type == pa.string()


def test_stop_geography_keeps_build_time_coordinates() -> None:
    assert STOP_GEOGRAPHY_SCHEMA.field('stop_lat').type == pa.float64()
    assert STOP_GEOGRAPHY_SCHEMA.field('stop_lon').type == pa.float64()


def test_stop_geography_records_how_it_matched() -> None:
    assert STOP_GEOGRAPHY_SCHEMA.field('geography_match').type == pa.string()
    assert STOP_GEOGRAPHY_SCHEMA.field('boundary_tie').type == pa.bool_()


def test_stop_meshblock_keeps_both_distances() -> None:
    for column in ('straight_line_distance_m', 'network_distance_m'):
        assert STOP_MESHBLOCK_SCHEMA.field(column).type == pa.float64()


@pytest.mark.parametrize('schema', SCHEMAS)
def test_column_names_are_unique(schema: pa.Schema) -> None:
    assert len(schema.names) == len(set(schema.names))


def test_geography_match_values() -> None:
    assert {member.value for member in GeographyMatch} == {
        'inside',
        'nearest',
        'unmatched',
    }


def test_routing_status_values() -> None:
    assert RoutingStatus.NOT_ATTEMPTED in set(RoutingStatus)
    assert RoutingStatus.SNAP_FAILED != RoutingStatus.UNROUTABLE


def test_an_empty_table_matches_the_schema() -> None:
    table = STOP_GEOGRAPHY_SCHEMA.empty_table()
    assert table.schema == STOP_GEOGRAPHY_SCHEMA
    assert table.num_rows == 0
