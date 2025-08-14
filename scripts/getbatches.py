import duckdb
import os
import glob
import multiprocessing

# Get environment variables (with defaults for testing)
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

# Define paths for input and output
propsholds_dir = f"{data_dir}/parquet/{region}/{region}_propsholds"
prop_shapes_output_dir = f"{data_dir}/parquet/{region}/{region}_prop_shapes"

# Ensure output directory exists
os.makedirs(prop_shapes_output_dir, exist_ok=True)

# Get list of all state-level `propsholds` Parquet files
propsholds_files = glob.glob(os.path.join(propsholds_dir, "propsholds_*.parquet"))
if not propsholds_files:
    raise ValueError(f" No Parquet files found in {propsholds_dir}")
print(f" Found {len(propsholds_files)} state Parquet files. Processing entire region...")

con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute(f"PRAGMA threads = {multiprocessing.cpu_count()};")
con.execute("PRAGMA memory_limit='100GB';")
con.execute(f"PRAGMA temp_directory='{data_dir}/duckdb_temp';")
con.execute("PRAGMA max_temp_directory_size='500GB';")


#  Step 1: Load All State Properties into DuckDB
print(" Loading all state propsholds files into a single table...")
con.execute(f"""
    CREATE OR REPLACE TABLE propsholds AS 
    SELECT 
        fips_id, 
        propid, 
        holdid,
        ST_GeomFromWKB(geom) AS geom         -- Convert geometry properly
    FROM read_parquet([{', '.join(f"'{f}'" for f in propsholds_files)}]);
""")
propsholds_count = con.execute("SELECT COUNT(*) FROM propsholds;").fetchone()[0]
print(f" Loaded {propsholds_count} property records.")

#  Step 2: Compute Decile-Based Batch Ranges **in DuckDB**
print(" Computing decile-based batch ranges across the entire region...")

decile_query = """
WITH propid_numeric AS (
    SELECT 
        propid, 
        TRY_CAST(REPLACE(propid, ' ', '') AS BIGINT) AS clean_propid
    FROM propsholds
),
percentiles AS (
    SELECT 
        percentile_cont(0.0) WITHIN GROUP (ORDER BY clean_propid) AS p0,
        percentile_cont(0.1) WITHIN GROUP (ORDER BY clean_propid) AS p10,
        percentile_cont(0.2) WITHIN GROUP (ORDER BY clean_propid) AS p20,
        percentile_cont(0.3) WITHIN GROUP (ORDER BY clean_propid) AS p30,
        percentile_cont(0.4) WITHIN GROUP (ORDER BY clean_propid) AS p40,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY clean_propid) AS p50,
        percentile_cont(0.6) WITHIN GROUP (ORDER BY clean_propid) AS p60,
        percentile_cont(0.7) WITHIN GROUP (ORDER BY clean_propid) AS p70,
        percentile_cont(0.8) WITHIN GROUP (ORDER BY clean_propid) AS p80,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY clean_propid) AS p90,
        percentile_cont(1.0) WITHIN GROUP (ORDER BY clean_propid) AS p100
    FROM propid_numeric
)
SELECT * FROM percentiles;
"""

deciles = con.execute(decile_query).fetchone()

# Generate batch ranges while ensuring that lower bound < upper bound
batch_ranges = [
    (max(0, int(deciles[i])), int(deciles[i + 1]))
    for i in range(len(deciles) - 1)
    if int(deciles[i]) < int(deciles[i + 1])
]
print(f" Batch Ranges for the entire region: {batch_ranges}")

#  Step 3: **Ensure `prop_shapes` table exists before inserting**
con.execute("DROP TABLE IF EXISTS prop_shapes;")
con.execute("""
CREATE TABLE prop_shapes (
    propid VARCHAR,
    holdid VARCHAR,
    geom GEOMETRY,
    area_acres NUMERIC,
    num_parcels INTEGER
);
""")
print(" Created `prop_shapes` table.")

#  Step 4: Process Each Batch and Insert into `prop_shapes`
for lo, hi in batch_ranges:
    count = con.execute(f"""
        SELECT COUNT(*) FROM propsholds 
        WHERE TRY_CAST(REPLACE(propid, ' ', '') AS BIGINT) BETWEEN {lo} AND {hi};
    """).fetchone()[0]

    print(f" Checking batch {lo} to {hi}: {count} matching records")

    if count == 0:
        continue  # Skip batches with no matching records

    con.execute(f"""
    INSERT INTO prop_shapes
    WITH collected AS (
        SELECT 
            propid,
            MIN(holdid) AS holdid,
            ST_Collect(LIST(geom)) AS collected_geom,
            COUNT(geom) AS num_parcels
        FROM propsholds
        WHERE TRY_CAST(REPLACE(propid, ' ', '') AS BIGINT) BETWEEN {lo} AND {hi}
        GROUP BY propid
    )
    SELECT 
        propid,
        holdid,
        collected_geom AS geom,
        ST_AREA(collected_geom) / 4046 AS area_acres,
        num_parcels
    FROM collected;
    """)

    print(f" Batch {lo} to {hi} inserted into `prop_shapes`.")

#  Step 5: Save `prop_shapes` to Parquet
prop_shapes_parquet_path = os.path.join(prop_shapes_output_dir, f"prop_shapes_{region}.parquet")
con.execute(f"COPY prop_shapes TO '{prop_shapes_parquet_path}' (FORMAT 'parquet');")
print(f" `prop_shapes` saved to {prop_shapes_parquet_path}")

# Close the connection
con.close()
print(" Processing complete! `prop_shapes` data saved as a single regional Parquet file.")
