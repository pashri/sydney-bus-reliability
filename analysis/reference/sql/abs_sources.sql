-- Rename ABS boundary columns to the names the geography SQL uses.
--
-- Create these inputs first, with ST_Read over the extracted
-- shapefiles:
--
--   mesh_block_raw  MB_2021_AUST_GDA2020.shp
--   lga_raw         LGA_2021_AUST_GDA2020.shp
--
-- The ABS ships one file for the whole country and names its columns
-- after the edition, so both the naming and the extent are adapted
-- here rather than in the queries that follow. Nothing downstream
-- mentions an ABS column name, which is what lets the edition change
-- without rewriting the analysis.
--
-- ST_Read already returns the codes as text. They must stay that way:
-- a mesh block code is eleven digits and some begin with a zero, so
-- reading one as a number silently produces a code that matches
-- nothing, or matches somewhere else.

-- Mesh blocks, clipped to the collected extent.
--
-- The state filter alone would still carry the whole of NSW, most of
-- which holds no stops. The envelope is the area the feed covers.
create or replace view mesh_block as
select
    raw.MB_CODE21 as mesh_block_code,
    raw.MB_CAT21 as mesh_block_category,
    raw.SA1_CODE21 as sa1_code,
    raw.SA2_CODE21 as sa2_code,
    raw.SA2_NAME21 as sa2_name,
    raw.SA3_CODE21 as sa3_code,
    raw.SA3_NAME21 as sa3_name,
    raw.SA4_CODE21 as sa4_code,
    raw.SA4_NAME21 as sa4_name,
    raw.GCC_CODE21 as gccsa_code,
    raw.GCC_NAME21 as gccsa_name,
    raw.STE_CODE21 as state_code,
    raw.AREASQKM21 as area_sqkm,
    raw.geom as geom
from mesh_block_raw as raw
where raw.STE_CODE21 = '1'
  and st_intersects(
      raw.geom,
      st_makeenvelope(150.1, -34.7, 152.2, -32.0)
  );

-- Local government areas, over the same extent.
create or replace view lga as
select
    raw.LGA_CODE21 as lga_code,
    raw.LGA_NAME21 as lga_name,
    raw.geom as geom
from lga_raw as raw
where raw.STE_CODE21 = '1'
  and st_intersects(
      raw.geom,
      st_makeenvelope(150.1, -34.7, 152.2, -32.0)
  );
