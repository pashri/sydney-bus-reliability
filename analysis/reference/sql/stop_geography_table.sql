-- The published stop_geography table.
--
-- Depends on stop_geography.sql, seifa.sql, mesh_block_counts.sql and
-- abs_sources.sql having been run, plus a ``centre`` table holding
-- the committed centre list and a ``build`` table holding one row of
-- vintage and edition values.

-- Every stop with its mesh block, however it was assigned.
--
-- A stop that fell inside a polygon and a stop matched to its nearest
-- polygon are both kept, distinguished by geography_match rather than
-- by one of them being absent. Absence would leave a hole nobody
-- notices; a label is something a query can filter on.
create or replace view stop_area as
select
    inside.stop_id,
    inside.mesh_block_code,
    inside.sa1_code,
    inside.sa2_code,
    inside.sa2_name,
    inside.sa3_code,
    inside.sa3_name,
    inside.sa4_code,
    inside.sa4_name,
    inside.gccsa_code,
    inside.gccsa_name,
    'inside' as geography_match,
    0.0 as geography_match_distance_m,
    inside.boundary_tie
from stop_mesh_block as inside
union all
select
    near.stop_id,
    near.mesh_block_code,
    near.sa1_code,
    near.sa2_code,
    near.sa2_name,
    near.sa3_code,
    near.sa3_name,
    near.sa4_code,
    near.sa4_name,
    near.gccsa_code,
    near.gccsa_name,
    'nearest' as geography_match,
    near.distance_m as geography_match_distance_m,
    false as boundary_tie
from stop_nearest_mesh_block as near;

-- The nearest centre to each stop, and how far away it is.
--
-- Distance to the Sydney CBD is kept separately because it is the
-- conventional measure, but it describes a monocentric city. A stop
-- in Parramatta is not peripheral simply because Martin Place is far
-- away, and for a Newcastle stop the distance to Sydney says nothing
-- at all.
create or replace view stop_centre as
select
    stop.stop_id,
    nearest.centre_id as nearest_centre_id,
    nearest.centre_name as nearest_centre_name,
    nearest.distance_m as distance_to_nearest_centre_m
from stop_point as stop
cross join lateral (
    select
        centre.centre_id,
        centre.centre_name,
        st_distance_sphere(
            st_point(centre.latitude, centre.longitude),
            st_point(stop.stop_lat, stop.stop_lon)
        ) as distance_m
    from centre
    order by distance_m
    limit 1
) as nearest;

-- One row per stop.
create or replace view stop_geography as
select
    stop.stop_id,
    stop.stop_lat,
    stop.stop_lon,
    area.mesh_block_code,
    counts.mesh_block_category,
    area.sa1_code,
    area.sa2_code,
    area.sa2_name,
    area.sa3_code,
    area.sa3_name,
    area.sa4_code,
    area.sa4_name,
    area.gccsa_code,
    area.gccsa_name,
    lga.lga_code,
    lga.lga_name,
    area.geography_match,
    area.geography_match_distance_m,
    area.boundary_tie,
    seifa.irsd_score,
    seifa.irsd_national_decile,
    seifa.irsd_state_decile,
    seifa.irsad_score,
    seifa.irsad_national_decile,
    seifa.irsad_state_decile,
    seifa.ier_score,
    seifa.ier_national_decile,
    seifa.ier_state_decile,
    seifa.ieo_score,
    seifa.ieo_national_decile,
    seifa.ieo_state_decile,
    seifa.usual_resident_population as sa1_usual_resident_population,
    coalesce(seifa.irsd_excluded, false) as seifa_irsd_excluded,
    st_distance_sphere(
        st_point(cbd.latitude, cbd.longitude),
        st_point(stop.stop_lat, stop.stop_lon)
    ) as distance_to_cbd_m,
    centre.nearest_centre_id,
    centre.nearest_centre_name,
    centre.distance_to_nearest_centre_m,
    build.vintage,
    build.asgs_edition,
    build.seifa_release,
    build.census_year
from stop_point as stop
left join stop_area as area on area.stop_id = stop.stop_id
left join stop_lga as lga on lga.stop_id = stop.stop_id
left join stop_centre as centre on centre.stop_id = stop.stop_id
left join mesh_block_count as counts
    on counts.mesh_block_code = area.mesh_block_code
left join seifa_sa1 as seifa on seifa.sa1_code = area.sa1_code
cross join (select * from centre where centre_id = 'sydney') as cbd
cross join build;
