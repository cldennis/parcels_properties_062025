import duckdb
import os
import glob
import multiprocessing

# Get environment variables
region = os.getenv("REGION", "northeast")
data_dir = os.getenv("DATA_DIR", "/Volumes/LaCie/cdennis/national_regrid_test_2025_01_16")


# Define paths for input and output
props_with_groupids_dir = f"{data_dir}/parquet/{region}/{region}_props_with_groupids"
regional_holdings_path = f"{data_dir}/parquet/{region}/{region}_holdings/holdings.parquet"  # Single regional holdings file
propsholds_output_dir = f"{data_dir}/parquet/{region}/{region}_propsholds"

# Ensure output directory exists
os.makedirs(propsholds_output_dir, exist_ok=True)

# Get list of Parquet files to process
props_files = glob.glob(os.path.join(props_with_groupids_dir, "props_with_groupids_*.parquet"))
states = [os.path.basename(f).replace("props_with_groupids_", "").replace(".parquet", "") for f in props_files]

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

#  Step 1: Load Regional Holdings File
print(f" Loading regional holdings from: {regional_holdings_path}")
con.execute(f"""
    CREATE OR REPLACE TABLE holdings AS 
    SELECT * FROM read_parquet('{regional_holdings_path}');
""")

holdings_count = con.execute("SELECT COUNT(*) FROM holdings;").fetchone()[0]
print(f" Loaded {holdings_count} regional holdings records.")

#  Step 2: Process Each State's Property Data
for state in states:
    props_parquet_path = os.path.join(props_with_groupids_dir, f"props_with_groupids_{state}.parquet")
    propsholds_parquet_path = os.path.join(propsholds_output_dir, f"propsholds_{state}.parquet")

    print(f" Processing {state} from: {props_parquet_path}")

    # Perform Join Against Regional Holdings
    con.execute(f"""
        CREATE OR REPLACE TABLE propsholds AS
        SELECT 
            t1.*, 
            COALESCE(t2.holdid, t1.propid) AS holdid  -- Assign `propid` if `holdid` is NULL
        FROM read_parquet('{props_parquet_path}') t1
        LEFT JOIN holdings t2
        ON t1.fips_id = t2.fips_id;
    """)

    #  Step 3: Verify the Join
    joined_count = con.execute("SELECT COUNT(*) FROM propsholds;").fetchone()[0]
    print(f" Total records in `propsholds` for {state}: {joined_count}")

    #  Step 4: Save `propsholds` to Parquet
    print(f" Saving joined `propsholds` data for {state} to Parquet...")
    con.execute(f"COPY propsholds TO '{propsholds_parquet_path}' (FORMAT 'parquet');")
    print(f" `propsholds` saved to {propsholds_parquet_path}")

# Close connection
con.close()
print(" Processing complete! `propsholds` data saved in Parquet files.")
