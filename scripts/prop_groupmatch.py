import polars as pl
import networkx as nx
import duckdb
import os
import glob
import multiprocessing

# Get environment variables
region = os.getenv("REGION",)
data_dir = os.getenv("DATA_DIR")

# Define input/output paths
match_pairs_dir = f"{data_dir}/parquet/{region}/{region}_match_pairs/"
cleaned_pstl_dir = f"{data_dir}/parquet/{region}/parquets_cleaned/"
output_dir = f"{data_dir}/parquet/{region}/{region}_props_with_groupids"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Get list of Parquet files
match_pairs_files = glob.glob(os.path.join(match_pairs_dir, "match_pairs_*.parquet"))
states = [os.path.basename(f).replace("match_pairs_", "").replace(".parquet", "") for f in match_pairs_files]

if not states:
    raise ValueError(f" No match pair files found in {match_pairs_dir}")

print(f" Found match pairs for states: {states}")

con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute(f"PRAGMA threads = {multiprocessing.cpu_count()};")
con.execute("PRAGMA memory_limit='100GB';")
con.execute(f"PRAGMA temp_directory='{data_dir}/duckdb_temp';")
con.execute("PRAGMA max_temp_directory_size='500GB';")


# Process each state separately
for state in states:
    match_pairs_parquet = os.path.join(match_pairs_dir, f"match_pairs_{state}.parquet")
    cleaned_pstl_parquet = os.path.join(cleaned_pstl_dir, f"cleanedpstl_{state}.parquet")
    output_parquet = os.path.join(output_dir, f"props_with_groupids_{state}.parquet")

    print(f" Processing state: {state}")

    # Load match pairs using Polars
    match_pairs = pl.read_parquet(match_pairs_parquet).select(["id1", "id2"])

    if match_pairs.is_empty():
        print(f"⚠ No match pairs found for {state}. Skipping...")
        continue

    # Convert Polars DataFrame to NetworkX graph
    G = nx.Graph()
    G.add_edges_from(match_pairs.to_numpy().tolist())

    # Create property groups using connected components
    groups = {node: min(nodes) for nodes in nx.connected_components(G) for node in nodes}

    # Convert groups dictionary to a Polars DataFrame
    groups_df = pl.DataFrame({"id": list(groups.keys()), "groupid": list(groups.values())})

    print(f" Generated {len(groups_df)} property groups for {state}.")

    # Register Polars DataFrame into DuckDB
    con.register("groups_df", groups_df.to_pandas())  # Convert Polars DataFrame to Pandas for DuckDB

    # Load cleaned_pstl data into DuckDB
    con.execute(f"""
        CREATE OR REPLACE TABLE cleaned_pstl AS 
        SELECT * FROM read_parquet('{cleaned_pstl_parquet}');
    """)

    # **Join and Save Results**
    con.execute("DROP TABLE IF EXISTS props_with_groupids;")
    con.execute("""
        CREATE TABLE props_with_groupids AS
        SELECT a.*, CAST(b.groupid AS TEXT) AS propid
        FROM cleaned_pstl a
        LEFT JOIN groups_df b
        ON a.fips_id = b.id
        WHERE a.state2 = ?;
    """, [state])

    # Verify DuckDB table
    grouped_count = con.execute("SELECT COUNT(*) FROM props_with_groupids;").fetchone()[0]

    # **Save results to Parquet using DuckDB**
    con.execute(f"COPY props_with_groupids TO '{output_parquet}' (FORMAT 'parquet');")

    print(f"📁 Saved grouped data to {output_parquet}")

# Close connection
con.close()
print(" Processing complete! Grouped data is now stored in Parquet files.")
