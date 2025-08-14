import duckdb
import os
import multiprocessing

# Set environment variables (adjust as needed)
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

# Input/Output file paths
prop_shapes_path = f"{data_dir}/parquet/{region}/{region}_prop_shapes/prop_shapes_{region}.parquet"
holds_dispersion_output_path = f"{data_dir}/parquet/{region}/{region}_holds_dispersion/holds_dispersion_bbox.parquet"

# Ensure the output directory exists
os.makedirs(os.path.dirname(holds_dispersion_output_path), exist_ok=True)

print(f"🔹 Processing bounding-box dispersion analysis for holdings in region: {region}")

# Ensure temporary directory exists for DuckDB
temp_duckdb_dir = f"{data_dir}/duckdb_temp"
os.makedirs(temp_duckdb_dir, exist_ok=True)

# Connect to DuckDB in memory and configure spatial extension and performance settings
con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute(f"PRAGMA threads = {multiprocessing.cpu_count()};")
con.execute("PRAGMA memory_limit='100GB';")
con.execute(f"PRAGMA temp_directory='{temp_duckdb_dir}';")
con.execute("PRAGMA max_temp_directory_size='500GB';")

# Step 1: Compute the envelope of parcel geometries for each holding.
# Instead of ST_Union, we use ARRAY_AGG to aggregate all parcel geometries and then build a collection.
# ST_Envelope returns the bounding box of that collection.
print(f" Loading property shapes from: {prop_shapes_path} and computing envelope geometry for each holding...")
con.execute(f"""
    CREATE OR REPLACE TABLE holds_union AS
    SELECT holdid,
           ST_Envelope(ST_Collect(ARRAY_AGG(geom))) AS union_geom
    FROM read_parquet('{prop_shapes_path}')
    GROUP BY holdid;
""")
union_count = con.execute("SELECT COUNT(*) FROM holds_union;").fetchone()[0]
print(f" Computed envelope geometries for {union_count} holdings.")

# Step 2: Calculate the bounding box diagonal (dispersion metric).
# This calculates the distance between the lower-left and upper-right corners of the envelope.
print(" Calculating bounding box diagonal (dispersion) for each holding...")
con.execute("DROP TABLE IF EXISTS holds_dispersion_bbox;")
con.execute("""
    CREATE TABLE holds_dispersion_bbox AS
    SELECT 
         holdid,
         ST_Distance(
             ST_Point(ST_XMin(union_geom), ST_YMin(union_geom)),
             ST_Point(ST_XMax(union_geom), ST_YMax(union_geom))
         ) / 1000.0 AS bbox_diagonal_km
    FROM holds_union;
""")
dispersion_count = con.execute("SELECT COUNT(*) FROM holds_dispersion_bbox;").fetchone()[0]
print(f" Computed dispersion for {dispersion_count} holdings.")

# Step 3: Save the dispersion results to a Parquet file.
print(f" Saving dispersion results to: {holds_dispersion_output_path}")
con.execute(f"COPY holds_dispersion_bbox TO '{holds_dispersion_output_path}' (FORMAT 'parquet');")
print(f" Dispersion data saved successfully to {holds_dispersion_output_path}")

# Close the DuckDB connection
con.close()
print(" Processing complete! Dispersion data (based on bounding box diagonal) is now stored in a single Parquet file.")
