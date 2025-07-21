import duckdb
import os
import glob
import pandas as pd

# Get environment variables
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

duckdb.sql(f"""
INSTALL spatial; LOAD spatial;

CREATE TABLE prop_shapes_aw AS
SELECT
  s.propid,
  s.holdid,
  s.area_acres,
  s.num_parcels,
  pl.mean_zip_match AS mean_zip_match_aw,
  pl.mean_in_urban  AS mean_in_urban_aw
FROM
  read_parquet('{data_dir}/parquet/{region}/{region}_prop_shapes/prop_shapes_{region}_with_urban.parquet') AS s
  LEFT JOIN (
    SELECT
      propid,
      SUM(zip_match * ST_AREA(ST_GeomFromWKB(geom)))   / SUM(ST_AREA(ST_GeomFromWKB(geom)))   AS mean_zip_match,
      SUM(in_urban  * ST_AREA(ST_GeomFromWKB(geom)))   / SUM(ST_AREA(ST_GeomFromWKB(geom)))   AS mean_in_urban
    FROM read_parquet('{data_dir}/parquet/{region}/{region}_propsholds_final/*.parquet')
    GROUP BY propid
  ) AS pl USING (propid);
COPY (SELECT * FROM prop_shapes_aw) TO '{data_dir}/parquet/{region}/{region}_prop_shapes/prop_shapes_{region}_aw.parquet' (FORMAT PARQUET);
""")
