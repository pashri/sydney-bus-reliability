-- The published stop_meshblock bridge: who lives near each stop.
--
-- Depends on abs_sources.sql, mesh_block_counts.sql and
-- stop_geography.sql having been run, plus a ``build`` table holding
-- one row of vintage and edition values.
--
-- One row per stop and nearby mesh block, keeping the distance rather
-- than a population within a fixed radius. Any catchment - 400 m,
-- 800 m, a distance-decay weighting - is then a query rather than a
-- rebuild, and a question nobody has asked yet does not require
-- recomputing anything.

-- A point guaranteed to lie inside its mesh block.
--
-- The centroid of a concave block can fall outside the block
-- entirely, which happens for a small but real share of them here:
-- blocks wrapping a bay, a park or a bend in a river. Routing from a
-- point in the wrong block returns a plausible number that is wrong,
-- so an interior point is used instead. Area is carried alongside so
-- a consumer can see when a single point represents something far too
-- large to stand for.
create or replace view mesh_block_anchor as
select
    mb.mesh_block_code,
    mb.mesh_block_category,
    mb.sa1_code,
    mb.area_sqkm,
    st_x(st_pointonsurface(mb.geom)) as anchor_lon,
    st_y(st_pointonsurface(mb.geom)) as anchor_lat
from mesh_block as mb;

-- Candidate pairs, cut down by a bounding box before any distance is
-- measured.
--
-- The box is deliberately a little wider than the radius, since a
-- degree of longitude is shorter than a degree of latitude at this
-- latitude and the box has to contain the circle.
create or replace view stop_meshblock_candidate as
select
    stop.stop_id,
    stop.stop_lat,
    stop.stop_lon,
    anchor.mesh_block_code,
    anchor.mesh_block_category,
    anchor.sa1_code,
    anchor.area_sqkm,
    anchor.anchor_lat,
    anchor.anchor_lon
from stop_point as stop
join mesh_block_anchor as anchor
    on anchor.anchor_lat
        between stop.stop_lat - 0.0145 and stop.stop_lat + 0.0145
   and anchor.anchor_lon
        between stop.stop_lon - 0.0175 and stop.stop_lon + 0.0175;

-- One row per stop and mesh block within the catchment radius.
--
-- network_distance_m is left for the routing pass to fill, so every
-- row starts as not_attempted. It stays null rather than being
-- approximated, because a straight line is not a walk: in a city
-- divided by water and motorways the two diverge most exactly where
-- the difference matters. The routing pass will skip mesh blocks with
-- no residents, which contribute nothing to any population sum.
create or replace view stop_meshblock as
select
    candidate.stop_id,
    candidate.mesh_block_code,
    st_distance_sphere(
        st_point(candidate.anchor_lat, candidate.anchor_lon),
        st_point(candidate.stop_lat, candidate.stop_lon)
    ) as straight_line_distance_m,
    cast(null as double) as network_distance_m,
    cast(null as double) as network_duration_s,
    'not_attempted' as routing_status,
    counts.person_count,
    counts.dwelling_count,
    candidate.mesh_block_category,
    candidate.sa1_code,
    candidate.area_sqkm,
    build.vintage,
    build.asgs_edition,
    build.seifa_release,
    build.census_year
from stop_meshblock_candidate as candidate
left join mesh_block_count as counts
    on counts.mesh_block_code = candidate.mesh_block_code
cross join build
where st_distance_sphere(
    st_point(candidate.anchor_lat, candidate.anchor_lon),
    st_point(candidate.stop_lat, candidate.stop_lon)
) <= 1600;
