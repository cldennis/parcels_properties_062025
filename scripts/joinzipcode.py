import duckdb
import os
import glob
import multiprocessing

# Get environment variables
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

# Define paths for input and output
propsholds_dir = f"{data_dir}/parquet/{region}/{region}_propsholds"
parquets_dir = f"{data_dir}/parquet/{region}/parquets_partitioned"
updated_propsholds_output_dir = f"{data_dir}/parquet/{region}/{region}_propsholds_updated"

# Ensure output directory exists
os.makedirs(updated_propsholds_output_dir, exist_ok=True)

# Get list of Parquet files to process
propsholds_files = glob.glob(os.path.join(propsholds_dir, "propsholds_*.parquet"))
states = [os.path.basename(f).replace("propsholds_", "").replace(".parquet", "") for f in propsholds_files]

if not states:
    raise ValueError(f"❌ No Parquet files found in {propsholds_dir}")

print(f"🔹 Found state Parquet files: {states}")
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

# Process each state's Parquet file
for state in states:
    propsholds_parquet_path = os.path.join(propsholds_dir, f"propsholds_{state}.parquet")
    parquets_parquet_path = os.path.join(parquets_dir, f"parquets_{state}.parquet")
    updated_propsholds_parquet_path = os.path.join(updated_propsholds_output_dir, f"propsholds_{state}.parquet")

    # Ensure partitioned parquet file exists
    if not os.path.exists(parquets_parquet_path):
        print(f"⚠️ Missing Parquet file for {state}: {parquets_parquet_path}. Skipping...")
        continue

    print(f"🔹 Loading data for {state} from:")
    print(f"   📂 propsholds: {propsholds_parquet_path}")
    print(f"   📂 parquets: {parquets_parquet_path}")

    # Load data from Parquet files into DuckDB
    df_propsholds = con.execute(f"SELECT * FROM read_parquet('{propsholds_parquet_path}');").fetchdf()
    df_parquets = con.execute(f"SELECT fips_id, census_zcta FROM read_parquet('{parquets_parquet_path}');").fetchdf()

    if df_propsholds.empty or df_parquets.empty:
        print(f"⚠️ Skipping {state} due to missing data.")
        continue

    # 🚀 Step 1: Perform the Join and Compute `pstlzip`
    print(f"🔗 Joining `propsholds` with `parquets` to add `census_zcta` and extract `pstlzip` for {state}...")
    df_updated = con.execute("""
        SELECT 
            a.*, 
            b.census_zcta,
            RIGHT(a.pstlclean, 5) AS pstlzip
        FROM df_propsholds a
        LEFT JOIN df_parquets b
        ON a.fips_id = b.fips_id;
    """).fetchdf()

    print(f"✅ Joined {len(df_updated)} records for {state}")

    # 🚀 Step 2: Save `propsholds` to Parquet
    df_updated.to_parquet(updated_propsholds_parquet_path, index=False)
    print(f"📁 `propsholds` saved to {updated_propsholds_parquet_path}")

# Close connection
con.close()
print("🎉 Processing complete! Updated `propsholds` data saved as Parquet files.")
