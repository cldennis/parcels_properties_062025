import duckdb
import os
import glob
import multiprocessing

# Get environment variables
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

# Define paths for input/output
props_with_groupids_dir = f"{data_dir}/parquet/{region}/{region}_props_with_groupids"

# Ensure the directory exists
os.makedirs(props_with_groupids_dir, exist_ok=True)

# Get list of Parquet files
parquet_files = glob.glob(os.path.join(props_with_groupids_dir, "props_with_groupids_*.parquet"))
states = [os.path.basename(f).replace("props_with_groupids_", "").replace(".parquet", "") for f in parquet_files]

if not states:
    raise ValueError(f" No Parquet files found in {props_with_groupids_dir}")

print(f" Found state Parquet files: {states}")

con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute(f"PRAGMA threads = {multiprocessing.cpu_count()};")
con.execute("PRAGMA memory_limit='100GB';")
con.execute(f"PRAGMA temp_directory='{data_dir}/duckdb_temp';")
con.execute("PRAGMA max_temp_directory_size='500GB';")

# Process each state's Parquet file
for state in states:
    parquet_path = os.path.join(props_with_groupids_dir, f"props_with_groupids_{state}.parquet")

    print(f" Processing {state} from {parquet_path}")

    # Load Parquet into DuckDB
    con.execute(f"""
        CREATE OR REPLACE TABLE props_with_groupids AS 
        SELECT * FROM read_parquet('{parquet_path}');
    """)

    #  Directly update `propid`, do NOT create `propid_fixed`
    con.execute("""
        UPDATE props_with_groupids
        SET propid = fips_id
        WHERE propid IS NULL OR TRIM(propid) = '';
    """)

    # Verify updates
    updated_count = con.execute("SELECT COUNT(*) FROM props_with_groupids WHERE propid = fips_id;").fetchone()[0]
    print(f" Updated {updated_count} rows where `propid` was NULL for {state}.")

    #  Save directly to Parquet (overwrite)
    con.execute(f"COPY props_with_groupids TO '{parquet_path}' (FORMAT 'parquet');")

    print(f" Overwritten updated Parquet file: {parquet_path}")

# Close connection
con.close()
print(" Processing complete! Updated Parquet files now include corrected `propid` values.")
