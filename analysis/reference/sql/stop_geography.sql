-- Assign each stop the statistical areas its coordinates fall inside.
--
-- Create these inputs first:
--
--   dim_stop      curated/dim_stop/ (stop_id, stop_name, stop_lat, stop_lon)
--   mesh_block    ASGS mesh block polygons, read with ST_Read
--   lga           ASGS LGA polygons, read with ST_Read
--
-- ABS codes carry leading zeros. Read every source with all_varchar or
-- an explicit VARCHAR cast: read as numbers they lose the zero, and a
-- code that has lost its leading zero joins to nothing, or silently to
-- the wrong area.

-- Stops as points. Longitude first: ST_Point takes x then y, and
-- swapping them puts Sydney in the Indian Ocean without erroring.
--
-- ST_Distance_Sphere does not follow that convention. It reads the
-- first ordinate as latitude, so a geometry built for ST_Intersects
-- gives a wrong distance if handed to it directly - wrong by enough
-- to matter and little enough to look plausible. Every spherical
-- distance below therefore builds its own point in latitude-first
-- order, and never reuses ``geom``.
create or replace view stop_point as
select
    stop.stop_id,
    stop.stop_lat,
    stop.stop_lon,
    st_point(stop.stop_lon, stop.stop_lat) as geom
from dim_stop as stop;

-- Every mesh block a stop falls inside.
--
-- A stop on a shared boundary falls inside both neighbours, so this
-- can return more than one row per stop. That is resolved below
-- rather than hidden by a distinct.
create or replace view stop_mesh_block_match as
select
    stop.stop_id,
    mb.mesh_block_code,
    mb.sa1_code,
    mb.sa2_code,
    mb.sa2_name,
    mb.sa3_code,
    mb.sa3_name,
    mb.sa4_code,
    mb.sa4_name,
    mb.gccsa_code,
    mb.gccsa_name
from stop_point as stop
join mesh_block as mb
    on st_intersects(mb.geom, stop.geom);

-- One mesh block per stop, with ties made visible.
create or replace view stop_mesh_block as
select
    match.stop_id,
    any_value(match.mesh_block_code) as mesh_block_code,
    any_value(match.sa1_code) as sa1_code,
    any_value(match.sa2_code) as sa2_code,
    any_value(match.sa2_name) as sa2_name,
    any_value(match.sa3_code) as sa3_code,
    any_value(match.sa3_name) as sa3_name,
    any_value(match.sa4_code) as sa4_code,
    any_value(match.sa4_name) as sa4_name,
    any_value(match.gccsa_code) as gccsa_code,
    any_value(match.gccsa_name) as gccsa_name,
    count(*) > 1 as boundary_tie
from stop_mesh_block_match as match
group by match.stop_id;

-- One LGA per stop. LGA is a separate boundary set, not a mesh block
-- attribute, so it needs its own containment test.
create or replace view stop_lga as
select
    stop.stop_id,
    any_value(lga.lga_code) as lga_code,
    any_value(lga.lga_name) as lga_name
from stop_point as stop
join lga
    on st_intersects(lga.geom, stop.geom)
group by stop.stop_id;

-- Stops that fell inside nothing, with the nearest mesh block and how
-- far away it is.
--
-- These are real: a stop on a wharf, a coordinate typo, a stop just
-- outside the clipped extent. Left as a silent null they become an
-- unexamined hole in every geographic comparison, so they are kept,
-- measured, and labelled.
-- Ranking uses planar distance in degrees, which is anisotropic but
-- picks the same nearest polygon at these latitudes. The distance
-- reported is spherical, in metres, measured to the closest point on
-- that polygon rather than to its centroid.
create or replace view stop_nearest_mesh_block as
select
    stop.stop_id,
    nearest.mesh_block_code,
    nearest.sa1_code,
    nearest.sa2_code,
    nearest.sa2_name,
    nearest.sa3_code,
    nearest.sa3_name,
    nearest.sa4_code,
    nearest.sa4_name,
    nearest.gccsa_code,
    nearest.gccsa_name,
    st_distance_sphere(
        st_point(st_y(nearest.closest), st_x(nearest.closest)),
        st_point(stop.stop_lat, stop.stop_lon)
    ) as distance_m
from stop_point as stop
cross join lateral (
    select
        mb.mesh_block_code,
        mb.sa1_code,
        mb.sa2_code,
        mb.sa2_name,
        mb.sa3_code,
        mb.sa3_name,
        mb.sa4_code,
        mb.sa4_name,
        mb.gccsa_code,
        mb.gccsa_name,
        st_closestpoint(mb.geom, stop.geom) as closest
    from mesh_block as mb
    order by st_distance(mb.geom, stop.geom)
    limit 1
) as nearest
where stop.stop_id not in (select stop_id from stop_mesh_block);
