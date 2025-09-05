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

# 1) Load all state parquet into one table
print("📥 Loading all Parquet files into a single table...")
con.execute(f"""
    CREATE OR REPLACE TABLE props_with_groupids AS 
    SELECT * FROM read_parquet({parquet_files}, union_by_name:=TRUE);
""")

# 2) Build propid ↔ reliable address map (only rows with a mail address)
print("🏡 Computing holdings at the regional level...")
con.execute("""
CREATE OR REPLACE TABLE _prop_addr AS
SELECT DISTINCT
  propid,
  NULLIF(
    REGEXP_REPLACE(UPPER(pstlclean), '[^A-Z0-9]', '', 'g'),
    ''
  ) AS addr_key
FROM props_with_groupids
WHERE
  -- pstlclean not null/empty
  NULLIF(TRIM(pstlclean), '') IS NOT NULL
  -- mailadd not null/empty
  AND NULLIF(TRIM(mailadd), '') IS NOT NULL
  -- must contain at least one digit (street number heuristic)
  AND REGEXP_MATCHES(UPPER(mailadd), '.*[0-9].*');
""")

# Optional peek at top addresses
df = con.execute("""
SELECT addr_key, COUNT(*) AS k
FROM _prop_addr
WHERE addr_key IS NOT NULL
GROUP BY addr_key
ORDER BY k DESC
LIMIT 20;
""").df()
print(df)

# 3) Address canonical hub
con.execute("""
CREATE OR REPLACE TABLE _addr_canon AS
SELECT addr_key, MIN(propid) AS canon
FROM _prop_addr
GROUP BY addr_key;
""")

# 4) Sparse undirected edges (star)
con.execute("""
CREATE OR REPLACE TABLE _edges AS
SELECT p.propid AS u, a.canon AS v
FROM _prop_addr p
JOIN _addr_canon a USING (addr_key)
WHERE p.propid <> a.canon;

CREATE OR REPLACE TABLE _edges_ud AS
SELECT u, v FROM _edges
UNION ALL
SELECT v AS u, u AS v FROM _edges;
""")

# 5) Initialize labels: one per propid
con.execute("""
CREATE OR REPLACE TABLE _labels AS
SELECT DISTINCT propid AS node, propid AS label
FROM props_with_groupids;
""")

# 6) Label propagation until convergence
changed = 1
iters = 0
while changed:
    iters += 1

    # Best label from neighbors (and self)
    con.execute("""
        CREATE OR REPLACE TABLE _best AS
        SELECT
            l.node,
            MIN(COALESCE(ln.label, l.label)) AS best_label
        FROM _labels l
        LEFT JOIN _edges_ud e ON e.u = l.node
        LEFT JOIN _labels ln ON ln.node = e.v
        GROUP BY l.node;
    """)

    # Only the rows that would change
    con.execute("""
        CREATE OR REPLACE TABLE _updates AS
        SELECT
            l.node,
            LEAST(b.best_label, l.label) AS new_label
        FROM _labels l
        JOIN _best b USING (node)
        WHERE LEAST(b.best_label, l.label) <> l.label;
    """)

    changed = con.execute("SELECT COUNT(*) FROM _updates;").fetchone()[0]

    if changed:
        con.execute("""
            UPDATE _labels AS L
            SET label = U.new_label
            FROM _updates AS U
            WHERE L.node = U.node;
        """)
        con.execute("DROP TABLE _updates;")

    print(f"iteration {iters}, changed={changed}")

# 7) === IMPORTANT: match original output schema ===
# holdings_temp must be (fips_id, propid, holdid) with one row per parcel.
con.execute("""
CREATE OR REPLACE TABLE holdings_temp AS
SELECT DISTINCT
  p.fips_id,
  p.propid,
  l.label AS holdid
FROM props_with_groupids p
JOIN _labels l
  ON l.node = p.propid;
""")

# (Optional) Row-level props with holdid, if you need it elsewhere
con.execute("""
CREATE OR REPLACE TABLE props_with_regional_hold AS
SELECT p.*, h.holdid
FROM props_with_groupids p
JOIN holdings_temp h USING (fips_id, propid);
""")

# 8) Verify & save
grouped_count = con.execute("SELECT COUNT(*) FROM holdings_temp;").fetchone()[0]
print(f"✅ Total records in `holdings_temp`: {grouped_count}")

print(f"📁 Saving regional holdings data to {holdings_output_file}...")
con.execute(f"COPY holdings_temp TO '{holdings_output_file}' (FORMAT 'parquet');")
print(f"📁 Regional holdings saved to {holdings_output_file}")

con.close()
print("🎉 Processing complete! Regional holdings data is now stored in a single Parquet file.")
