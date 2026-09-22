-- SEIFA socio-economic indexes at SA1, tidied from the ABS workbook.
--
-- Create these inputs first, one per sheet, with read_xlsx over the
-- SA1 indexes workbook:
--
--   seifa_irsd_raw      Table 2, range A7:L70000
--   seifa_irsad_raw     Table 3, range A7:L70000
--   seifa_ier_raw       Table 4, range A7:L70000
--   seifa_ieo_raw       Table 5, range A7:L70000
--   seifa_excluded_raw  Table 6, range A7:F40000
--
-- Read every sheet with all_varchar so codes keep leading zeros, and
-- with header=false so the columns arrive as A, B, C and so on: the
-- workbook spreads its headings over two rows and neither is usable
-- as a header.
--
-- Each index is published with two rankings. The national decile
-- compares an area with all of Australia; the state decile compares
-- it only with the rest of NSW. They answer different questions and
-- both are kept, because a within-NSW comparison wants the state
-- ranking while any claim about the country needs the national one.

-- One sheet, tidied. Rows below the data are dropped by the length
-- test: the sheet ends with a copyright line in the first column.
create or replace view seifa_irsd as
select
    raw.A as sa1_code,
    try_cast(raw.B as integer) as usual_resident_population,
    try_cast(raw.C as double) as score,
    try_cast(raw.F as smallint) as national_decile,
    try_cast(raw.G as smallint) as national_percentile,
    try_cast(raw.K as smallint) as state_decile,
    try_cast(raw.L as smallint) as state_percentile
from seifa_irsd_raw as raw
where raw.A is not null and length(raw.A) = 11;

create or replace view seifa_irsad as
select
    raw.A as sa1_code,
    try_cast(raw.C as double) as score,
    try_cast(raw.F as smallint) as national_decile,
    try_cast(raw.K as smallint) as state_decile
from seifa_irsad_raw as raw
where raw.A is not null and length(raw.A) = 11;

create or replace view seifa_ier as
select
    raw.A as sa1_code,
    try_cast(raw.C as double) as score,
    try_cast(raw.F as smallint) as national_decile,
    try_cast(raw.K as smallint) as state_decile
from seifa_ier_raw as raw
where raw.A is not null and length(raw.A) = 11;

create or replace view seifa_ieo as
select
    raw.A as sa1_code,
    try_cast(raw.C as double) as score,
    try_cast(raw.F as smallint) as national_decile,
    try_cast(raw.K as smallint) as state_decile
from seifa_ieo_raw as raw
where raw.A is not null and length(raw.A) = 11;

-- The areas the ABS left out, and which index each was left out of.
--
-- Exclusion is per index, not per area: an area can be excluded from
-- disadvantage and still score on education and occupation. Most
-- excluded areas have no residents, but some have a few and were
-- dropped because a score built on them would not be reliable.
create or replace view seifa_excluded as
select
    raw.A as sa1_code,
    try_cast(raw.B as integer) as usual_resident_population,
    raw.C = 'Y' as irsd_excluded,
    raw.D = 'Y' as irsad_excluded,
    raw.E = 'Y' as ier_excluded,
    raw.F = 'Y' as ieo_excluded
from seifa_excluded_raw as raw
where raw.A is not null and length(raw.A) = 11;

-- One row per SA1 that received at least one index score, plus the
-- exclusion flags for any index it did not.
create or replace view seifa_sa1 as
select
    coalesce(
        irsd.sa1_code, irsad.sa1_code, ier.sa1_code, ieo.sa1_code
    ) as sa1_code,
    irsd.usual_resident_population,
    irsd.score as irsd_score,
    irsd.national_decile as irsd_national_decile,
    irsd.state_decile as irsd_state_decile,
    irsad.score as irsad_score,
    irsad.national_decile as irsad_national_decile,
    irsad.state_decile as irsad_state_decile,
    ier.score as ier_score,
    ier.national_decile as ier_national_decile,
    ier.state_decile as ier_state_decile,
    ieo.score as ieo_score,
    ieo.national_decile as ieo_national_decile,
    ieo.state_decile as ieo_state_decile,
    coalesce(excluded.irsd_excluded, false) as irsd_excluded,
    coalesce(excluded.irsad_excluded, false) as irsad_excluded,
    coalesce(excluded.ier_excluded, false) as ier_excluded,
    coalesce(excluded.ieo_excluded, false) as ieo_excluded
from seifa_irsd as irsd
full join seifa_irsad as irsad using (sa1_code)
full join seifa_ier as ier using (sa1_code)
full join seifa_ieo as ieo using (sa1_code)
left join seifa_excluded as excluded using (sa1_code);
