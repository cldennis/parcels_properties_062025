import duckdb

# 1) connect and load spatial extension
conn = duckdb.connect()
conn.execute("INSTALL spatial;")
conn.execute("LOAD spatial;")
region = 'midwest'

# 2) build & run the SQL
conn.execute(f"""
CREATE TABLE county_aw_zip_match AS
WITH parcel_areas AS (
  SELECT
    county,
    state2,
    zip_match,
    ST_AREA(ST_GeomFromWKB(geom)) / 4046.86 AS area
  FROM parquet_scan('/home/christina/Desktop/property-matching/regrid_2025/parquet/{region}/{region}_propsholds_final/*urban.parquet')
  ),

county_weighted AS (
  SELECT
    county,
    state2,
    ROUND(SUM(zip_match * area) / SUM(area),3 ) AS aw_zip_match
  FROM parcel_areas
  GROUP BY county, state2
)

-- 3) attach the geoid
SELECT
  cw.county,
  cw.state2,
  cw.aw_zip_match,
  cg.geoid
FROM county_weighted AS cw
LEFT JOIN read_csv_auto('/media/christina/ssd_cd/cdennis_2025/data_info/county_area/county_geoid_{region}.csv') AS cg
  ON cw.state2  = cg.state2
 AND cw.county = cg.county;
""")



conn.execute(f"""
COPY county_aw_zip_match 
TO '/home/christina/Desktop/property-matching/regrid_2025/methods_figures/parquets_local_urban/summaries/{region}_county_aw_zip.csv'
 (HEADER, DELIMITER ',');""")

##### merge all states
duckdb.sql(f"""
COPY(
SELECT * from '/home/christina/Desktop/property-matching/regrid_2025/methods_figures/parquets_local_urban/summaries/midwest_county_aw_zip.csv' 
UNION
SELECT * from '/home/christina/Desktop/property-matching/regrid_2025/methods_figures/parquets_local_urban/summaries/west_county_aw_zip.csv' 
UNION
SELECT * from '/home/christina/Desktop/property-matching/regrid_2025/methods_figures/parquets_local_urban/summaries/south_county_aw_zip.csv' 
UNION
SELECT * from '/home/christina/Desktop/property-matching/regrid_2025/methods_figures/parquets_local_urban/summaries/northeast_county_aw_zip.csv' 
)
TO '/home/christina/Desktop/property-matching/regrid_2025/methods_figures/parquets_local_urban/summaries/county_area_local_fulljoin.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',' );
""")
