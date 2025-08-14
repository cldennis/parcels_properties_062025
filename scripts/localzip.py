import duckdb
import os
import glob
import multiprocessing

# Get environment variables
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

# Define paths
propsholds_updated_dir = f"{data_dir}/parquet/{region}/{region}_propsholds_updated"  #  From previous step
propsholds_final_dir = f"{data_dir}/parquet/{region}/{region}_propsholds_final"  #  Final output

# Ensure output directory exists
os.makedirs(propsholds_final_dir, exist_ok=True)

# Get list of `propsholds_updated_{state}.parquet` files
propsholds_files = glob.glob(os.path.join(propsholds_updated_dir, "propsholds_*.parquet"))

# Debug: List all available files
print(f" Checking for state-based property holdings in: {propsholds_updated_dir}")
if propsholds_files:
    print(f" Found {len(propsholds_files)} files.")
    for file in propsholds_files:
        print(f"   - {file}")  # Print each file found
else:
    raise ValueError(f" No `propsholds_updated_*.parquet` files found in {propsholds_updated_dir}")

#  Connect to DuckDB using on-disk storage for stability
duckdb_file = f"{data_dir}/duckdb_temp/region_{region}.duckdb"
os.makedirs(f"{data_dir}/duckdb_temp", exist_ok=True)
# Ensure temporary directory exists for DuckDB
temp_duckdb_dir = f"{data_dir}/duckdb_temp"
os.makedirs(temp_duckdb_dir, exist_ok=True)

con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute(f"PRAGMA threads = {multiprocessing.cpu_count()};")
con.execute("PRAGMA memory_limit='100GB';")
con.execute(f"PRAGMA temp_directory='{temp_duckdb_dir}';")
con.execute("PRAGMA max_temp_directory_size='500GB';")

#  Step 1: Process Each State-Based `propsholds_updated_{state}.parquet`
for propsholds_path in propsholds_files:
    state = os.path.basename(propsholds_path).replace("propsholds_", "").replace(".parquet", "")
    propsholds_final_path = os.path.join(propsholds_final_dir, f"propsholds_final_{state}.parquet")

    print(f" Processing state: {state}")
    print(f" Loading properties from: {propsholds_path}")

    # Check if the file exists before processing (redundant, but extra safety)
    if not os.path.exists(propsholds_path):
        print(f" Skipping missing file: {propsholds_path}")
        continue

    # Load `propsholds_updated_{state}.parquet`
    con.execute(f"""
        CREATE OR REPLACE TABLE propsholds AS 
        SELECT * FROM read_parquet('{propsholds_path}');
    """)

    #  Step 2: Compute `zip_match` Field
    print(f" Identifying local zips for state: {state}")
    con.execute(f"""
        CREATE OR REPLACE TABLE propsholds_final AS
        SELECT 
            *,
            CASE 
                WHEN pstlzip IS NULL OR census_zcta IS NULL THEN NULL
                WHEN pstlzip = census_zcta THEN 1
                ELSE 0
            END AS zip_match
        FROM propsholds;
    """)

    #  Step 3: Save the updated data to Parquet
    print(f" Saving final `propsholds_final_{state}.parquet` to: {propsholds_final_path}")
    con.execute(f"COPY propsholds_final TO '{propsholds_final_path}' (FORMAT 'parquet');")
    print(f" `propsholds_final_{state}.parquet` saved successfully.")

    #  Step 4: Verify ZIP Match Count
    local_zips_count = con.execute("SELECT COUNT(*) FROM propsholds_final WHERE zip_match = 1;").fetchone()[0]
    print(f" Number of local rows in `propsholds_final_{state}.parquet`: {local_zips_count}")

# Close connection
con.close()
print("Processing complete! Updated `propsholds` files are now stored as state-based Parquet files.")
