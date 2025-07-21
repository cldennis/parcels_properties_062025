import duckdb
import os
import glob
import pandas as pd

# Get environment variables
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

# Define Parquet directories
parquets_dir = f"{data_dir}/parquet/{region}/parquets_partitioned/"  # ✅ Partitioned input data
propsholds_final_dir = f"{data_dir}/parquet/{region}/{region}_propsholds_final/"  # ✅ Final property holdings
prop_shapes_dir = f"{data_dir}/parquet/{region}/{region}_prop_shapes/"  # ✅ Processed property geometries
holdings_info_path = f"{data_dir}/parquet/{region}/{region}_holdings/holdings_info.parquet"  # ✅ Final holdings info

# ✅ Connect to DuckDB using on-disk storage
duckdb_file = f"{data_dir}/duckdb_temp/region_{region}.duckdb"
os.makedirs(f"{data_dir}/duckdb_temp", exist_ok=True)
con = duckdb.connect(database=duckdb_file)

print(f"🔹 Connected to DuckDB at: {duckdb_file}")

# 🚀 Step 1: Validate `parquets_partitioned`
print("📊 Validating partitioned input Parquet data...")
parquet_files = glob.glob(os.path.join(parquets_dir, "parquets_*.parquet"))
if not parquet_files:
    raise ValueError(f"❌ No `parquets_{{state}}.parquet` files found in {parquets_dir}")

parquets_count = con.execute(f"""
    SELECT COUNT(*) FROM read_parquet([{', '.join(f"'{f}'" for f in parquet_files)}]);
""").fetchone()[0]
print(f"✅ Total records in `parquets_partitioned`: {parquets_count}")

# 🚀 Step 2: Validate `propsholds_final`
print("📊 Validating processed property holdings...")
propsholds_files = glob.glob(os.path.join(propsholds_final_dir, "propsholds_final_*.parquet"))
if not propsholds_files:
    raise ValueError(f"❌ No `propsholds_final_{{state}}.parquet` files found in {propsholds_final_dir}")

propsholds_count = con.execute(f"""
    SELECT COUNT(*) FROM read_parquet([{', '.join(f"'{f}'" for f in propsholds_files)}]);
""").fetchone()[0]
print(f"✅ Total records in `propsholds_final`: {propsholds_count}")

distinct_fips_count = con.execute(f"""
    SELECT COUNT(DISTINCT fips_id) FROM read_parquet([{', '.join(f"'{f}'" for f in propsholds_files)}]);
""").fetchone()[0]
print(f"✅ Unique `fips_id` count in `propsholds_final`: {distinct_fips_count}")

distinct_holdid_count = con.execute(f"""
    SELECT COUNT(DISTINCT holdid) FROM read_parquet([{', '.join(f"'{f}'" for f in propsholds_files)}]);
""").fetchone()[0]
print(f"✅ Unique `holdid` count in `propsholds_final`: {distinct_holdid_count}")

distinct_propid_count = con.execute(f"""
    SELECT COUNT(DISTINCT propid) FROM read_parquet([{', '.join(f"'{f}'" for f in propsholds_files)}]);
""").fetchone()[0]
print(f"✅ Unique `propid` count in `propsholds_final`: {distinct_propid_count}")  # ✅ FIX: `propid` is in `propsholds_final`

# 🚀 Step 3: Validate `prop_shapes`
print("📊 Validating processed property shapes...")
prop_shapes_files = glob.glob(os.path.join(prop_shapes_dir, "prop_shapes_*.parquet"))
if not prop_shapes_files:
    raise ValueError(f"❌ No `prop_shapes_{{state}}.parquet` files found in {prop_shapes_dir}")

prop_shapes_count = con.execute(f"""
    SELECT COUNT(*) FROM read_parquet([{', '.join(f"'{f}'" for f in prop_shapes_files)}]);
""").fetchone()[0]
print(f"✅ Total records in `prop_shapes`: {prop_shapes_count}")

# 🚀 Step 4: Validate `holdings_info`
print("📊 Validating processed holdings information...")
holdings_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{holdings_info_path}');").fetchone()[0]
print(f"✅ Total records in `holdings_info`: {holdings_count}")

# 🚀 Step 5: Sample Data Previews
print("\n📊 Sample records from `propsholds_final`:")
sample_propsholds = con.execute(f"SELECT * FROM read_parquet('{propsholds_files[0]}') LIMIT 5;").fetchdf()
print(sample_propsholds)

print("\n📊 Sample records from `prop_shapes`:")
sample_prop_shapes = con.execute(f"SELECT * FROM read_parquet('{prop_shapes_files[0]}') LIMIT 5;").fetchdf()
print(sample_prop_shapes)

print("\n📊 Sample records from `holdings_info`:")
sample_holdings_info = con.execute(f"SELECT * FROM read_parquet('{holdings_info_path}') LIMIT 5;").fetchdf()
print(sample_holdings_info)

# ✅ Close connection
con.close()
print("\n🎉 Data validation complete!")