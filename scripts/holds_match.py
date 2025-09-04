import duckdb
import os
import glob
import multiprocessing

# Get environment variables
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

# Define paths for input/output
props_with_groupids_dir = f"{data_dir}/parquet/{region}/{region}_props_with_groupids"
holdings_output_file = f"{data_dir}/parquet/{region}/{region}_holdings/holdings.parquet"

# Ensure output directory exists
os.makedirs(os.path.dirname(holdings_output_file), exist_ok=True)

# Get list of all Parquet files in the region
parquet_files = glob.glob(os.path.join(props_with_groupids_dir, "props_with_groupids_*.parquet"))

if not parquet_files:
    raise ValueError(f"❌ No Parquet files found in {props_with_groupids_dir}")

print(f"🔹 Found {len(parquet_files)} state Parquet files. Processing entire region...")

con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute(f"PRAGMA threads = {multiprocessing.cpu_count()};")
con.execute("PRAGMA memory_limit='100GB';")
con.execute(f"PRAGMA temp_directory='{data_dir}/duckdb_temp';")
con.execute("PRAGMA max_temp_directory_size='500GB';")


# 🚀 Step 1: Load all state Parquet files into a single DuckDB table
print("📥 Loading all Parquet files into a single table...")

con.execute(f"""
    CREATE OR REPLACE TABLE props_with_groupids AS 
    SELECT * FROM read_parquet({parquet_files}, union_by_name=True);
""")

# 🚀 Step 5: Compute `holdid` at the **regional level** across all states
print("🏡 Computing holdings at the regional level...")
con.execute("""
CREATE OR REPLACE TABLE _props_norm AS
SELECT
  *,
  NULLIF(REGEXP_REPLACE(UPPER(pstlclean),'[^A-Z0-9]','','g'),'') AS addr_key
FROM props_with_groupids;

""")
con.execute("""
CREATE OR REPLACE TABLE holdings_temp AS
SELECT
  fips_id,
  propid,
  COALESCE(
    MIN(propid) OVER (PARTITION BY addr_key),
    propid
  ) AS holdid
FROM _props_norm;
""")

# 🚀 Step 3: Verify holdings
grouped_count = con.execute("SELECT COUNT(*) FROM holdings_temp;").fetchone()[0]
print(f"✅ Total records in `holdings_temp`: {grouped_count}")

# 🚀 Step 4: Save Holdings Data to a single Parquet file
print(f"📁 Saving regional holdings data to {holdings_output_file}...")
con.execute(f"COPY holdings_temp TO '{holdings_output_file}' (FORMAT 'parquet');")
print(f"📁 Regional holdings saved to {holdings_output_file}")

# Close connection
con.close()
print("🎉 Processing complete! Regional holdings data is now stored in a single Parquet file.")
