import duckdb
import os
import glob
import multiprocessing

# ── Environment setup ──────────────────────────────────────────────────────────
region        = os.getenv("REGION")
data_dir      = os.getenv("DATA_DIR")
input_folder  = f"{data_dir}/parquet/{region}/parquets_projected"
output_folder = f"{data_dir}/parquet/{region}/parquets_partitioned"
os.makedirs(output_folder, exist_ok=True)

# ── Find Parquet files ─────────────────────────────────────────────────────────
parquet_files = glob.glob(os.path.join(input_folder, "*.parquet"))
if not parquet_files:
    raise ValueError(f" No Parquet files found in {input_folder!r}")
print(f" Found {len(parquet_files)} Parquet files.")

# ── Connect to DuckDB ──────────────────────────────────────────────────────────
con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute(f"PRAGMA threads = {multiprocessing.cpu_count()};")
con.execute("PRAGMA memory_limit='100GB';")
con.execute(f"PRAGMA temp_directory='{data_dir}/duckdb_temp';")
con.execute("PRAGMA max_temp_directory_size='500GB';")

# ── Load into staging, assign numeric fileid & build new fips_id ───────────────
print(" Loading Parquet into DuckDB, tagging files with numeric IDs…")
con.execute(f"""
CREATE OR REPLACE TABLE parquets AS
WITH base AS (
  SELECT
    *,
    filename
  FROM read_parquet(
    '{input_folder}/*.parquet',
    union_by_name := TRUE,
    filename      := 'filename'        -- <-- use 'filename' here
  )
),
tagged AS (
  SELECT
    *,
    -- assign a small integer ID to each distinct filename
    DENSE_RANK() OVER (ORDER BY filename) AS fileid
  FROM base
)
SELECT
  ogc_fid,
  geoid,
  fileid,                                         -- new numeric fileid
  -- build fips_id from your numeric fileid + ogc_fid
  CAST(fileid AS VARCHAR) || '_' || CAST(ogc_fid AS VARCHAR) AS fips_id,
  owntype,
  owner,
  mailadd,
  mail_unit,
  mail_city,
  mail_state2,
  mail_zip,
  mail_country,
  city,
  county,
  UPPER(TRIM(COALESCE(state2, '<MISSING>'))) AS state2,  -- cleaned state
  census_zcta,
  CAST(wkb_geometry AS GEOMETRY) AS geom
FROM tagged;
""")

# ── Diagnostics ────────────────────────────────────────────────────────────────
total    = con.execute("SELECT COUNT(*)            FROM parquets").fetchone()[0]
distinct = con.execute("SELECT COUNT(DISTINCT fips_id) FROM parquets").fetchone()[0]
print(f"Total rows:              {total}")
print(f"Distinct fips_id values: {distinct}")
assert total == distinct, " fips_id still not unique!"

print(" Distinct state codes and record counts:")
df_states = con.execute("""
  SELECT state2, COUNT(*) AS count
  FROM parquets
  GROUP BY state2
  ORDER BY state2
""").fetchdf()
print(df_states.to_string(index=False))

# ── Partitioning ───────────────────────────────────────────────────────────────
states = df_states["state2"].tolist()
for state in states:
    safe_state = state.replace('<','').replace('>','').replace(' ','_')
    out_path = os.path.join(output_folder, f"parquets_{safe_state}.parquet")
    print(f"📁 Writing partition for state '{state}' → {out_path}")
    con.execute(f"""
      COPY (
        SELECT *
        FROM parquets
        WHERE state2 = '{state}'
      ) TO '{out_path}' (FORMAT 'parquet');
    """)

print(f" Finished writing {len(states)} partitions under {output_folder}")
con.close()
print(" All done!")
