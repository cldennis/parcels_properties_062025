import duckdb
import os
import glob
import multiprocessing

# Get environment variables
region = os.getenv("REGION")
data_dir = os.getenv("DATA_DIR")

# Define input/output paths
input_folder = f"{data_dir}/parquet/{region}/parquets_partitioned/"
output_folder = f"{data_dir}/parquet/{region}/parquets_concat/"

# Ensure output directory exists
os.makedirs(output_folder, exist_ok=True)

# Get partitioned Parquet files
parquet_files = glob.glob(os.path.join(input_folder, "parquets_*.parquet"))
states = [os.path.basename(f).replace("parquets_", "").replace(".parquet", "") for f in parquet_files]

if not states:
    raise ValueError(f"No Parquet state files found in {input_folder}")

print(f"🔹 Found partitioned Parquet files for states: {states}")

con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute(f"PRAGMA threads = {multiprocessing.cpu_count()};")
con.execute("PRAGMA memory_limit='100GB';")
con.execute(f"PRAGMA temp_directory='{data_dir}/duckdb_temp';")
con.execute("PRAGMA max_temp_directory_size='500GB';")


# Process each state separately
for state in states:
    input_parquet = os.path.join(input_folder, f"parquets_{state}.parquet")
    output_parquet = os.path.join(output_folder, f"concatpstl_{state}.parquet")

    print(f"🔹 Processing state: {state}")

    # Read partitioned Parquet file
    con.execute(f"""
        CREATE OR REPLACE TABLE parquets AS 
        SELECT * FROM read_parquet('{input_parquet}');
    """)

    # Generate `pstladress` (concatenated address)
    con.execute("DROP TABLE IF EXISTS state_pstl;")
    con.execute("""
        CREATE TABLE state_pstl AS
        SELECT 
            fips_id,
            mailadd, 
            COALESCE(mailadd, '') || ' ' || COALESCE(mail_city, '') || ' ' || COALESCE(mail_state2, '') || ' ' || COALESCE(mail_zip, '') AS pstladress
        FROM parquets;
    """)

    # Generate `tmp_orig_sn` (owner & location info)
    con.execute("DROP TABLE IF EXISTS tmp_orig_sn;")
    con.execute("""
        CREATE TABLE tmp_orig_sn AS
        SELECT
            fips_id,
            UPPER(owner) AS owner,
            mailadd,
            city,
            county,
            state2,
            geom
        FROM parquets;
    """)

    # Join `state_pstl` with `tmp_orig_sn`
    con.execute("DROP TABLE IF EXISTS test_concat;")
    con.execute("""
        CREATE TABLE test_concat AS
        SELECT a.*, b.pstladress
        FROM tmp_orig_sn a
        LEFT JOIN state_pstl b
        ON a.fips_id = b.fips_id;
    """)

    # Verify row count
    row_count = con.execute("SELECT COUNT(*) FROM test_concat;").fetchone()[0]
    print(f"Processed {row_count} rows for {state}")

    # Save processed data as Parquet
    con.execute(f"COPY test_concat TO '{output_parquet}' (FORMAT 'parquet');")
    print(f"Saved processed data to {output_parquet}")

# Close connection
con.close()
print("🎉 Processing complete! Address-concatenated Parquet files are ready.")
