#!/usr/bin/env python3
import duckdb

# ────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────────────────────────────
regions = ["midwest", "northeast", "south", "west"]

base_dir       = "/home/christina/Desktop/property-matching/regrid_2025/parquet"
validation_dir = "/home/christina/Desktop/property-matching/regrid_2025/validation"

N_SAMPLES   = 25
BUFFER_DIST = 100  # meters
# ────────────────────────────────────────────────────────────────────────

con = duckdb.connect()
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

for region in regions:
    print(f"\n>> Processing {region}")

    glob_path  = f"{base_dir}/{region}/{region}_propsholds_final/*.parquet"
    out_csv    = f"{validation_dir}/{region}_neighbors.csv"

    # drop old views
    con.execute("DROP VIEW IF EXISTS all_parcels;")
    con.execute("DROP VIEW IF EXISTS sampled;")
    con.execute("DROP VIEW IF EXISTS buffers;")
    con.execute("DROP VIEW IF EXISTS neighbors;")

    # 1) read only the columns we need via parquet_scan
    con.execute(f"""
    CREATE VIEW all_parcels AS
    SELECT
      fips_id,
      owner,
      pstlclean,
      propid,
      ST_GeomFromWKB(geom) AS geom
    FROM parquet_scan('{glob_path}');
    """)

    # 2) sample N random parcels
    con.execute(f"""
    CREATE VIEW sampled AS
    SELECT
      fips_id,
      owner,
      pstlclean,
      propid,
      geom
    FROM all_parcels
    ORDER BY RANDOM()
    LIMIT {N_SAMPLES};
    """)

    # 3) buffer by 100 m
    con.execute(f"""
    CREATE VIEW buffers AS
    SELECT
      fips_id    AS focal_id,
      ST_Buffer(geom, {BUFFER_DIST}) AS buffer_geom
    FROM sampled;
    """)

    # 4) find neighbors with owner & pstlclean
    con.execute("""
    CREATE VIEW neighbors AS
    SELECT
      p.fips_id,
      p.propid,
      p.owner,
      p.pstlclean,
      b.focal_id
    FROM all_parcels AS p
    JOIN buffers AS b
      ON ST_Intersects(p.geom, b.buffer_geom);
    """)

    # 5) export to CSV
    con.execute(f"""
    COPY neighbors
    TO '{out_csv}'
    (HEADER, DELIMITER ',');
    """)
    print(f"✅ {region}: wrote {out_csv}")

print("\nAll done!")
