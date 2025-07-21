#!/usr/bin/env python3
import duckdb, os, glob

BASE_DIR      = '/home/christina/Desktop/property-matching/regrid_2025/parquet'
URBAN_PARQUET = '/home/christina/Desktop/data/census/urban/urban_5070.parquet'
REGIONS       = ['midwest','west','south','northeast']

con = duckdb.connect(':memory:')
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

# === FIXED urban load ===
con.execute(f"""
CREATE TABLE urban AS
SELECT
  ST_Multi(ST_GeomFromWKB(geom)) AS geom
FROM parquet_scan('{URBAN_PARQUET}');
""")


for region in REGIONS:
    print(f"Processing region: {region}")
    out_dir = os.path.join(BASE_DIR, region, f"{region}_census")
    os.makedirs(out_dir, exist_ok=True)

    cent_pattern = os.path.join(
        BASE_DIR, region,
        f"{region}_centroids",           # e.g. midwest_centroids
        f"{region}_*_centroids.parquet"  # e.g. midwest_IA_centroids.parquet
    )
    files = glob.glob(cent_pattern)
    print(f"  → Found {len(files)} centroid files")

    for cent_path in files:
        state = os.path.basename(cent_path).split('_')[1]
        print(f"    • Flagging {region}/{state}")

        out_path = os.path.join(out_dir, f"{region}_{state}_urban_flag.parquet")

        con.execute(f"""
        CREATE OR REPLACE TABLE tmp_flags AS
        SELECT
          c.fips_id,
          MAX(CASE WHEN ST_Intersects(c.geom, u.geom) THEN 1 ELSE 0 END) AS in_urban
        FROM (
          SELECT
            fips_id,
            centroid AS geom           -- use the GEOMETRY column directly
          FROM parquet_scan('{cent_path}')
        ) AS c
        LEFT JOIN urban AS u
          ON ST_Intersects(c.geom, u.geom)
        GROUP BY c.fips_id;
        """)

        con.execute(f"""
        COPY tmp_flags
        TO '{out_path}'
        (FORMAT parquet);
        """)
        print(f"      ✔ Wrote {out_path}")

con.close()
