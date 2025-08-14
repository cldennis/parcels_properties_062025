import duckdb
import os
import glob
import multiprocessing

# Get environment variables
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

# Define paths for input/output
prop_shapes_path = f"{data_dir}/parquet/{region}/{region}_prop_shapes/prop_shapes_{region}.parquet"
holdings_info_path = f"{data_dir}/parquet/{region}/{region}_holdings/holdings_info.parquet"

# Ensure output directory exists
os.makedirs(os.path.dirname(holdings_info_path), exist_ok=True)

# Connect to DuckDB using on-disk storage for stability
duckdb_file = f"{data_dir}/duckdb_temp/region_{region}.duckdb"
os.makedirs(f"{data_dir}/duckdb_temp", exist_ok=True)

con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute(f"PRAGMA threads = {multiprocessing.cpu_count()};")
con.execute("PRAGMA memory_limit='100GB';")
con.execute(f"PRAGMA temp_directory='{data_dir}/duckdb_temp';")
con.execute("PRAGMA max_temp_directory_size='500GB';")

#  Step 1: Load `prop_shapes.parquet`
print(f" Loading property shapes from: {prop_shapes_path}")
con.execute(f"""
    CREATE OR REPLACE TABLE prop_shapes AS 
    SELECT * FROM read_parquet('{prop_shapes_path}');
""")

prop_shapes_count = con.execute("SELECT COUNT(*) FROM prop_shapes;").fetchone()[0]
print(f" Loaded {prop_shapes_count} property shape records.")

#  Step 2: Compute Holdings Information
print(" Aggregating holdings information...")

con.execute("""
    CREATE OR REPLACE TABLE holdings_info AS
    SELECT
        holdid,
        SUM(area_acres) AS hold_area_acres,  -- Total area in acres
        COUNT(holdid) AS numprops,          -- Number of properties per holding
        SUM(num_parcels) AS holds_numparcels  -- Total count of parcels per holding
    FROM prop_shapes
    GROUP BY holdid;
""")

#  Step 3: Verify Holdings Data
holdings_count = con.execute("SELECT COUNT(*) FROM holdings_info;").fetchone()[0]
print(f" Total records in holdings_info: {holdings_count}")

#  Step 4: Save Holdings Info to Parquet
print(f" Saving holdings info to: {holdings_info_path}...")
con.execute(f"COPY holdings_info TO '{holdings_info_path}' (FORMAT 'parquet');")

print(f" `holdings_info` saved successfully to {holdings_info_path}")

# Close connection
con.close()
print(" Processing complete! `holdings_info` is now stored in a single Parquet file.")
