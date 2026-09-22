"""Arrow schemas for the published reference tables.

Codes are text everywhere. ABS geographic codes carry leading zeros,
and read as numbers they lose them: a code that has lost a leading
zero joins to nothing, or to a different area entirely.

Derived values are nullable and stay null when they are unknown. A
missing SEIFA decile coalesced to a number invents disadvantage data,
and an unmatched stop coalesced to a code puts a bus somewhere it has
never been.
"""

from enum import StrEnum
from typing import Any, Final

import pyarrow as pa


class GeographyMatch(StrEnum):
    """How a stop was assigned to a statistical area."""

    INSIDE = 'inside'
    NEAREST = 'nearest'
    UNMATCHED = 'unmatched'


VINTAGE_FIELDS: Final[list[pa.Field[Any]]] = [
    pa.field('vintage', pa.date32()),
    pa.field('asgs_edition', pa.string()),
    pa.field('seifa_release', pa.string()),
    pa.field('census_year', pa.int16()),
]
"""Provenance carried by every row.

``vintage`` repeats the partition value as data. Aggregating a value
that exists only as a partition key has been a source of trouble, and
a table that states its own vintage can be checked against the path it
was written to.
"""

STOP_GEOGRAPHY_FIELDS: Final[list[pa.Field[Any]]] = [
    pa.field('stop_id', pa.string()),
    pa.field('stop_lat', pa.float64()),
    pa.field('stop_lon', pa.float64()),
    pa.field('mesh_block_code', pa.string()),
    pa.field('sa1_code', pa.string()),
    pa.field('sa2_code', pa.string()),
    pa.field('sa2_name', pa.string()),
    pa.field('sa3_code', pa.string()),
    pa.field('sa3_name', pa.string()),
    pa.field('sa4_code', pa.string()),
    pa.field('sa4_name', pa.string()),
    pa.field('gccsa_code', pa.string()),
    pa.field('gccsa_name', pa.string()),
    pa.field('lga_code', pa.string()),
    pa.field('lga_name', pa.string()),
    pa.field('geography_match', pa.string()),
    pa.field('geography_match_distance_m', pa.float64()),
    pa.field('boundary_tie', pa.bool_()),
    pa.field('irsd_decile', pa.int16()),
    pa.field('irsad_decile', pa.int16()),
    pa.field('ier_decile', pa.int16()),
    pa.field('ieo_decile', pa.int16()),
    pa.field('seifa_geography_level', pa.string()),
    pa.field('distance_to_cbd_m', pa.float64()),
    pa.field('nearest_centre_id', pa.string()),
    pa.field('nearest_centre_name', pa.string()),
    pa.field('distance_to_nearest_centre_m', pa.float64()),
]
STOP_GEOGRAPHY_SCHEMA: Final[pa.Schema] = pa.schema(
    STOP_GEOGRAPHY_FIELDS + VINTAGE_FIELDS,
)
"""One row per stop, holding where it is and what surrounds it.

``stop_lat`` and ``stop_lon`` are the coordinates the enrichment was
computed from, not a copy for convenience. A stop that moves in a
later bundle keeps this geography until someone rebuilds, and
comparing these against the current ones is how that is noticed.
"""

STOP_MESHBLOCK_FIELDS: Final[list[pa.Field[Any]]] = [
    pa.field('stop_id', pa.string()),
    pa.field('mesh_block_code', pa.string()),
    pa.field('straight_line_distance_m', pa.float64()),
    pa.field('network_distance_m', pa.float64()),
    pa.field('network_duration_s', pa.float64()),
    pa.field('routing_status', pa.string()),
    pa.field('snap_distance_m', pa.float64()),
    pa.field('detour_ratio', pa.float64()),
    pa.field('person_count', pa.int32()),
    pa.field('dwelling_count', pa.int32()),
    pa.field('mesh_block_category', pa.string()),
    pa.field('sa1_code', pa.string()),
    pa.field('osrm_profile', pa.string()),
    pa.field('osm_extract_id', pa.string()),
]
STOP_MESHBLOCK_SCHEMA: Final[pa.Schema] = pa.schema(
    STOP_MESHBLOCK_FIELDS + VINTAGE_FIELDS,
)
"""One row per stop and nearby mesh block.

Both distances are kept. Straight-line distance understates a walk
wherever water or a motorway intervenes, which in this city is often
and unevenly, so the pair is more informative than either alone.
``person_count`` is perturbed by the ABS and a zero does not prove a
mesh block is empty.
"""
