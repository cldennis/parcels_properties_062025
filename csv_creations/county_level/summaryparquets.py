import duckdb

region = "northeast"
which = "holdid"

### make parquets
duckdb.sql(f""" INSTALL spatial;
LOAD spatial;
""")

duckdb.sql(f"""
COPY (
  WITH props AS (
    SELECT
      {which},
      zip_match,
      in_urban,
      -- compute parcel area in acres once
      ST_AREA(ST_GeomFromWKB(geom)) / 4046.86 AS area_acres
    FROM parquet_scan(
      '/home/christina/Desktop/property-matching/regrid_2025/parquet/{region}/{region}_propsholds_final/*.parquet'
    )
  )
  SELECT
    {which},

    -- plain averages, for comparison
    ROUND(AVG(zip_match),        3) AS avg_zip_match,
    ROUND(AVG(in_urban),         3) AS avg_in_urban,

    -- total area in acres
    ROUND(SUM(area_acres),        3) AS {which}_area_acres,

    -- area-weighted means
    ROUND(
      SUM(zip_match  * area_acres)
      / SUM(area_acres),
      3
    ) AS aw_avg_zip_match,

    ROUND(
      SUM(in_urban   * area_acres)
      / SUM(area_acres),
      3
    ) AS aw_avg_in_urban

  FROM props
  GROUP BY {which}
)
TO '/home/christina/Desktop/property-matching/regrid_2025/methods_figures/parquets_local_urban/{region}_local_urban_{which}_weighted.parquet'
(FORMAT parquet);
""")


duckdb.sql(f"""
SELECT * 
FROM  '/home/christina/Desktop/property-matching/regrid_2025/methods_figures/parquets_local_urban/{region}_local_urban_{which}_weighted.parquet' 
where aw_avg_zip_match != 1.0 order by aw_avg_zip_match desc limit 10""")
