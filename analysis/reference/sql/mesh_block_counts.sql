-- Census person and dwelling counts per mesh block.
--
-- Create this input first, with read_xlsx over the Mesh Block Counts
-- workbook, reading all_varchar with header=false and range A8 down:
--
--   mesh_block_count_raw   NSW sheets, unioned
--
-- New South Wales is split across two sheets, "Table 1" and
-- "Table 1.1", because the state has more mesh blocks than a
-- worksheet holds rows. Reading only the first loses the second half
-- of the state without any error.
--
-- Counts are perturbed by the ABS to protect confidentiality: small
-- values are adjusted slightly, so a mesh block reporting no
-- residents is not proof that nobody lives there, and a count should
-- not be trusted to the last person. Aggregated over a catchment the
-- perturbation largely cancels.

create or replace view mesh_block_count as
select
    raw.A as mesh_block_code,
    raw.B as mesh_block_category,
    try_cast(raw.C as double) as area_albers_sqkm,
    try_cast(raw.D as integer) as dwelling_count,
    try_cast(raw.E as integer) as person_count
from mesh_block_count_raw as raw
where raw.A is not null and length(raw.A) = 11;
