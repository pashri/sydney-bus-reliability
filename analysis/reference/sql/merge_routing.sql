-- Fold routed walking distances into the catchment bridge.
--
-- Create these inputs first:
--
--   pair        the built stop_meshblock Parquet
--   routed_leg  the routing store's results
--
-- A pair that was never sent to the router keeps its
-- ``not_attempted`` status: mesh blocks with no residents are skipped
-- deliberately, and beyond the routed radius nothing was asked.

-- The bridge with walking distances filled where they were measured.
--
-- snap_distance_m says how far the two ends moved onto the walking
-- network before measuring. It matters most at short range: both ends
-- move independently, so where the true distance is comparable to the
-- snap the measured walk can come out shorter than the straight line,
-- which is geometrically impossible and entirely expected. The
-- discrepancy is bounded by the snap distance, so it is small in
-- absolute terms and concentrated where it changes no catchment.
create or replace view stop_meshblock_routed as
select
    pair.stop_id,
    pair.mesh_block_code,
    pair.straight_line_distance_m,
    routed.network_distance_m,
    routed.network_duration_s,
    coalesce(routed.routing_status, pair.routing_status) as routing_status,
    routed.snap_distance_m,
    routed.network_distance_m
        / nullif(pair.straight_line_distance_m, 0) as detour_ratio,
    pair.person_count,
    pair.dwelling_count,
    pair.mesh_block_category,
    pair.sa1_code,
    pair.area_sqkm,
    pair.vintage,
    pair.asgs_edition,
    pair.seifa_release,
    pair.census_year
from pair
left join routed_leg as routed
    on routed.stop_id = pair.stop_id
   and routed.mesh_block_code = pair.mesh_block_code;
