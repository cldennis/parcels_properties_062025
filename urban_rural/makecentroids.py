#!/usr/bin/env python3
import duckdb
from pathlib import Path

# Base folder for your state Parquets
BASE = Path("/home/christina/Desktop/property-matching/regrid_2025/parquet")

# Region‐state pairs
JOBS = [
    # midwest
    ("midwest", "WI"), ("midwest", "MN"), ("midwest", "IA"), ("midwest", "IL"),
    ("midwest", "IN"), ("midwest", "OH"), ("midwest", "SD"), ("midwest", "ND"),
    ("midwest", "KS"), ("midwest", "MO"), ("midwest", "NE"), ("midwest", "MI"),
    # northeast
    ("northeast", "DE"), ("northeast", "VT"), ("northeast", "MA"), ("northeast", "MD"),
    ("northeast", "NH"), ("northeast", "PA"), ("northeast", "NJ"), ("northeast", "RI"),
    ("northeast", "NY"), ("northeast", "CT"), ("northeast", "ME"),
    # south
    ("south", "AL"), ("south", "AR"), ("south", "DC"), ("south", "FL"),
    ("south", "GA"), ("south", "KY"), ("south", "LA"), ("south", "MS"),
    ("south", "NC"), ("south", "OK"), ("south", "SC"), ("south", "TN"),
    ("south", "TX"), ("south", "VA"), ("south", "WV"),
    # west
    ("west", "AK"), ("west", "AZ"), ("west", "CA"), ("west", "CO"),
    ("west", "HI"), ("west", "ID"), ("west", "MT"), ("west", "NM"),
    ("west", "NV"), ("west", "OR"), ("west", "UT"), ("west", "WA"),
    ("west", "WY"),
]

# Connect to DuckDB and load spatial extension
con = duckdb.connect()
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
# Adjust thread count as desired
con.execute("PRAGMA threads = 6;")

# Loop over each state, compute and save centroids
for region, st in JOBS:
    # Input Parquet
    in_pq = BASE / region / f"{region}_propsholds_final" / f"propsholds_final_{st}.parquet"
    # Output directory for centroids
    out_dir = BASE / region / f"{region}_centroids"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_pq = out_dir / f"{region}_{st}_centroids.parquet"

    con.execute(f"""
        COPY (
          SELECT
            fips_id,
            propid,
            holdid,
            ST_AsWKB(
              ST_Centroid(
                ST_GeomFromWKB(geom)
              )
            ) AS centroid
          FROM read_parquet('{in_pq}')
        ) TO '{out_pq}' (FORMAT parquet);
    """
    )
    print(f"Wrote centroids: {out_pq}")
